using Amazon.S3;
using Amazon.S3.Model;

namespace PocWorker.Services;

/// <summary>
/// Metadados do objeto necessarios para a leitura parcial.
/// </summary>
/// <param name="Length">Tamanho em bytes (para achar o footer pelo fim do arquivo).</param>
/// <param name="ETag">Identidade do conteudo usado no pinning (If-Match).</param>
/// <param name="VersionId">Versao, quando o bucket tem versionamento habilitado.</param>
public readonly record struct S3ObjectInfo(long Length, string? ETag, string? VersionId);

/// <summary>
/// Stream <b>seekable</b> sobre o S3 usando <c>Range GET</c>.
///
/// Para que serve: o <c>ParquetReader.CreateAsync(Stream)</c> so exige um stream legivel e
/// seekable. Ele le os ultimos 8 bytes (tamanho do footer), busca o footer (schema + offset e
/// tamanho de cada column chunk de cada row group) e a partir dai faz Seek + Read por coluna.
/// Este stream traduz exatamente esses Seek/Read em requisicoes HTTP com header Range, de forma
/// que o arquivo <b>nunca precisa ser baixado inteiro</b> — nem para memoria, nem para disco.
///
/// Estrategia de busca (importa mais do que parece):
///  - <c>Seek</c> so move o ponteiro; nada e transferido.
///  - A busca NAO e alinhada a um bloco fixo. Ela pede exatamente o que o reader pediu, com um
///    piso (<c>MinFetchBytes</c> = 256 KB) e um teto (<c>RangeBlockMb</c>).
///    Alinhar em blocos grandes parece elegante e e um tiro no pe: um column chunk deste layout
///    tem ~450 KB comprimidos, entao um bloco de 8 MB alinhado transferiria ~18x mais bytes que
///    o necessario — potencialmente MAIS trafego que baixar o arquivo inteiro.
///  - Leitura sequencial dentro do intervalo ja buscado nao gera trafego novo; ao passar do fim,
///    a proxima busca comeca onde paramos (sem sobreposicao).
///
/// Memoria: limitada ao maior intervalo buscado (<c>RangeBlockMb</c>) + o que o reader
/// materializa do row group. Nao depende do tamanho do objeto.
///
/// Consistencia (importante em producao): cada Range GET e uma requisicao independente. Se o
/// objeto for sobrescrito por outro produtor no meio da leitura, sem cuidado o reader monta um
/// arquivo Frankenstein — footer de uma versao, column chunks de outra — e o resultado ou
/// explode em parse error ou, pior, insere dado inconsistente silenciosamente. Para evitar isso,
/// o stream captura o <c>ETag</c> do objeto em <see cref="GetObjectInfoAsync"/> e envia
/// <c>If-Match</c> em toda requisicao: se o conteudo mudar, o S3 responde 412 em vez de devolver
/// bytes de outra versao. A mensagem falha de forma limpa e volta para a fila.
///
/// Premissa: acesso sequencial (um row group por vez, coluna por coluna), que e como o
/// <see cref="ParquetProcessor"/> consome. O cache nao e thread-safe por design.
/// </summary>
public sealed class S3RangeStream : Stream
{
    private const int MinFetchBytes = 256 * 1024;

    private readonly IAmazonS3 _s3;
    private readonly string _bucket;
    private readonly string _key;
    private readonly long _length;
    private readonly int _maxFetchBytes;
    private readonly string? _etag;
    private readonly bool _pinVersion;
    private readonly ILogger? _logger;
    private readonly bool _trace;

    private byte[] _buffer = [];
    private long _bufferStart;
    private int _bufferLen;
    private long _position;

    /// <summary>Bytes efetivamente transferidos do S3 (a metrica que mostra o ganho da projecao).</summary>
    public long TotalBytesFetched { get; private set; }

    /// <summary>Quantidade de requisicoes Range feitas.</summary>
    public long Requests { get; private set; }

    public S3RangeStream(IAmazonS3 s3, string bucket, string key, long length, int maxFetchBytes,
        string? etag = null, bool pinVersion = true, ILogger? logger = null, bool trace = false)
    {
        _s3 = s3;
        _bucket = bucket;
        _key = key;
        _length = length;
        _maxFetchBytes = Math.Max(MinFetchBytes, maxFetchBytes);
        _etag = etag;
        _pinVersion = pinVersion;
        _logger = logger;
        _trace = trace;
    }

    /// <summary>Tamanho + ETag do objeto (necessarios para ler o footer pelo fim e pinar a versao).</summary>
    public static async Task<S3ObjectInfo> GetObjectInfoAsync(
        IAmazonS3 s3, string bucket, string key, CancellationToken ct)
    {
        var meta = await s3.GetObjectMetadataAsync(new GetObjectMetadataRequest
        {
            BucketName = bucket,
            Key = key
        }, ct);

        return new S3ObjectInfo(meta.ContentLength, meta.ETag, meta.VersionId);
    }

    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;
    public override long Length => _length;

    public override long Position
    {
        get => _position;
        set => _position = value;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        _position = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => _position + offset,
            SeekOrigin.End => _length + offset,
            _ => throw new ArgumentOutOfRangeException(nameof(origin))
        };
        return _position;
    }

    /// <summary>
    /// Preenche o buffer o maximo possivel, encadeando buscas quando a leitura e maior que o
    /// teto. Retorna o total lido (0 somente em EOF).
    /// </summary>
    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var total = 0;

        while (total < buffer.Length && _position < _length)
        {
            if (_position < _bufferStart || _position >= _bufferStart + _bufferLen)
            {
                await FetchAsync(_position, buffer.Length - total, cancellationToken);
                if (_bufferLen == 0) break;
            }

            var available = _bufferLen - (int)(_position - _bufferStart);
            if (available <= 0) break;

            var count = Math.Min(buffer.Length - total, available);
            _buffer.AsMemory((int)(_position - _bufferStart), count).CopyTo(buffer[total..]);
            _position += count;
            total += count;
        }

        return total;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    /// <summary>
    /// O Parquet.Net usa a API assincrona; o caminho sincrono existe para compatibilidade com
    /// quem ler o stream direto. Nao ha contexto de UI aqui, entao bloquear e seguro.
    /// </summary>
    public override int Read(byte[] buffer, int offset, int count)
        => ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();

    /// <summary>Busca a partir de <paramref name="offset"/> exatamente o necessario, sem alinhamento.</summary>
    private async Task FetchAsync(long offset, int requested, CancellationToken ct)
    {
        var length = (long)Math.Clamp(requested, MinFetchBytes, _maxFetchBytes);
        length = Math.Min(length, _length - offset);

        if (length <= 0)
        {
            _bufferLen = 0;
            return;
        }

        _bufferStart = offset;
        var end = offset + length - 1;

        var request = new GetObjectRequest
        {
            BucketName = _bucket,
            Key = _key,
            ByteRange = new ByteRange(offset, end)
        };

        // Pinning: se o objeto for sobrescrito no meio da leitura, isto vira 412 em vez de
        // silenciosamente misturar bytes de duas versoes diferentes.
        if (_pinVersion && !string.IsNullOrEmpty(_etag))
        {
            request.EtagToMatch = _etag;
        }

        using var response = await _s3.GetObjectAsync(request, ct);

        var wanted = (int)length;
        if (_buffer.Length < wanted) _buffer = new byte[wanted];

        var read = 0;
        while (read < wanted)
        {
            var n = await response.ResponseStream.ReadAsync(_buffer.AsMemory(read, wanted - read), ct);
            if (n == 0) break;
            read += n;
        }

        _bufferLen = read;
        TotalBytesFetched += read;
        Requests++;

        if (_trace && _logger is not null)
        {
            _logger.LogInformation(
                "[range #{N}] offset={Offset:N0} len={Len:N0} (fim={End:N0}) do objeto de {Total:N0} bytes",
                Requests, offset, read, end, _length);
        }
    }

    public override void Flush() { }

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
