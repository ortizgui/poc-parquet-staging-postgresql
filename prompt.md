Crie uma POC local em Python para simular um processamento batch de arquivos Parquet vindos de um S3, usando Docker Compose.

Objetivo:

* Simular um fluxo onde um arquivo Parquet é disponibilizado em um bucket S3 local.
* Usar MiniStack como emulador local gratuito de AWS/S3.
* Um script Python lê esse arquivo em chunks.
* Cada chunk é inserido em uma tabela staging no PostgreSQL.
* Depois, um comando separado executa um merge/upsert da staging para uma tabela final.
* A tabela final deve começar com alguns registros já existentes para validar atualização.
* A staging deve receber registros novos e registros que já existem na final, para validar insert e update.

Stack:

* Docker Compose
* Python 3.12
* PostgreSQL 16
* MiniStack para simular S3 local
* boto3 para acessar o S3 local
* pandas/pyarrow para gerar e ler Parquet
* psycopg ou SQLAlchemy para inserir no PostgreSQL

Estrutura esperada:

.
├── docker-compose.yml
├── README.md
├── requirements.txt
├── data
│   └── input
├── scripts
│   ├── create_sample_file.py
│   ├── upload_to_s3.py
│   ├── process_file.py
│   └── merge_staging.py
└── sql
├── 001_init.sql
└── 002_seed_final.sql

Docker Compose:

* Deve subir:
    * postgres
    * ministack
* PostgreSQL:
    * image: postgres:16
    * container_name: poc-postgres
    * database: pocdb
    * user: pocuser
    * password: pocpass
    * port: 5432
* MiniStack:
    * image: ministackorg/ministack
    * container_name: poc-ministack
    * port: 4566
    * usar endpoint http://localhost:4566
    * região padrão us-east-1
    * bucket: poc-bucket

Observação:

* Não usar AWS real.
* Não usar LocalStack.
* Não depender de conta AWS, API key ou licença.
* Usar MiniStack com endpoint local:
    http://localhost:4566

Variáveis de ambiente esperadas:

* AWS_ACCESS_KEY_ID=test
* AWS_SECRET_ACCESS_KEY=test
* AWS_DEFAULT_REGION=us-east-1
* AWS_ENDPOINT_URL=http://localhost:4566
* S3_BUCKET=poc-bucket
* POSTGRES_HOST=localhost
* POSTGRES_PORT=5432
* POSTGRES_DB=pocdb
* POSTGRES_USER=pocuser
* POSTGRES_PASSWORD=pocpass

Tabelas PostgreSQL:

1. Tabela final: custody_position

Campos:

* id serial primary key
* account_id varchar not null
* asset_id varchar not null
* reference_date date not null
* quantity numeric(18, 4) not null
* amount numeric(18, 2) not null
* updated_at timestamp not null default now()

Constraint única:

* account_id, asset_id, reference_date

2. Tabela staging: custody_position_staging

Campos:

* id serial primary key
* batch_id uuid not null
* source_file varchar not null
* row_number integer not null
* record_hash varchar not null
* account_id varchar not null
* asset_id varchar not null
* reference_date date not null
* quantity numeric(18, 4) not null
* amount numeric(18, 2) not null
* status varchar not null default ‘PENDING’
* error_reason text null
* created_at timestamp not null default now()
* merged_at timestamp null

Constraints únicas:

* source_file, row_number
* source_file, record_hash

3. Tabela de erro: custody_position_error

Campos:

* id serial primary key
* batch_id uuid not null
* source_file varchar not null
* row_number integer not null
* payload jsonb not null
* error_reason text not null
* created_at timestamp not null default now()

Dados iniciais:

* A tabela final deve começar com pelo menos 3 registros.
* O arquivo Parquet gerado deve conter aproximadamente 25 registros.
* O arquivo deve conter:
    * registros novos;
    * registros com a mesma chave de negócio da tabela final, mas com quantity/amount diferentes, para testar update;
    * pelo menos 1 registro inválido para testar envio para tabela de erro, por exemplo quantity negativo;
    * pelo menos 1 registro inválido com account_id vazio.

Scripts:

1. create_sample_file.py

* Gera um arquivo Parquet local em:
    ./data/input/custody_position.parquet
* Deve criar aproximadamente 25 registros.
* Deve incluir registros novos, registros que atualizam a tabela final e registros inválidos.
* Deve imprimir o caminho do arquivo gerado e a quantidade de linhas.

2. upload_to_s3.py

* Cria o bucket poc-bucket no MiniStack, se não existir.
* Faz upload do arquivo:
    ./data/input/custody_position.parquet
* Destino:
    s3://poc-bucket/input/custody_position.parquet
* Deve usar boto3 apontando para:
    endpoint_url=http://localhost:4566
* Deve imprimir o bucket e a key enviada.

3. process_file.py

* Recebe como parâmetro:
    * bucket
    * key
    * chunk-size
* Exemplo:
    python scripts/process_file.py –bucket poc-bucket –key input/custody_position.parquet –chunk-size 5
* Baixa/lê o arquivo Parquet do S3 local usando MiniStack.
* Processa em chunks pequenos, por exemplo 5 linhas.
* Para cada linha:
    * valida campos obrigatórios;
    * valida quantity >= 0;
    * valida amount >= 0;
    * calcula record_hash com base nos dados da linha;
    * se inválida, grava em custody_position_error;
    * se válida, insere em custody_position_staging.
* O insert na staging deve usar ON CONFLICT DO NOTHING para garantir idempotência.
* O script deve imprimir no terminal:
    * batch_id;
    * total lido;
    * total válido;
    * total inválido;
    * total ignorado por duplicidade, se possível;
    * quantidade de chunks processados.

4. merge_staging.py

* Executa o merge/upsert da staging para a tabela final.
* Pode usar INSERT … ON CONFLICT DO UPDATE, porque é compatível e simples para a POC.
* Deve considerar apenas registros válidos da staging com status = ‘PENDING’.
* Depois do merge, atualizar status da staging para ‘MERGED’ e preencher merged_at.
* Deve imprimir:
    * total de registros pendentes antes;
    * total enviado para merge;
    * total final na tabela custody_position.

Requisitos importantes:

* O processamento precisa ser idempotente:
    * se eu rodar process_file.py duas vezes para o mesmo arquivo, não pode duplicar staging;
    * se eu rodar merge_staging.py duas vezes, não pode duplicar final.
* Use variáveis de ambiente para conexão com PostgreSQL e MiniStack.
* Inclua logs simples no terminal.
* Código simples e fácil de entender.
* Nomes de variáveis, funções e arquivos em inglês.
* Não criar framework ou abstrações desnecessárias.

README:

* Gerar um README completo com comandos para executar a POC localmente.
* Como estou em macOS, considerar comandos compatíveis com macOS.
* Para editar arquivos manualmente, prefira exemplos usando vim.
* O README deve conter comandos para:

Subir ambiente:

docker compose up -d

Criar ambiente Python:

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

Gerar arquivo:

python scripts/create_sample_file.py

Enviar arquivo para o S3 local no MiniStack:

python scripts/upload_to_s3.py

Processar arquivo:

python scripts/process_file.py –bucket poc-bucket –key input/custody_position.parquet –chunk-size 5

Executar merge:

python scripts/merge_staging.py

Conectar no PostgreSQL:

docker exec -it poc-postgres psql -U pocuser -d pocdb

Consultas úteis:

select * from custody_position order by account_id, asset_id, reference_date;
select * from custody_position_staging order by id;
select * from custody_position_error order by id;

Critérios de aceite:

* O Docker Compose sobe PostgreSQL e MiniStack.
* O MiniStack fica disponível em http://localhost:4566.
* O arquivo Parquet é gerado localmente.
* O arquivo é enviado para o S3 local no MiniStack.
* O script processa o arquivo em chunks.
* Linhas inválidas vão para tabela de erro.
* Linhas válidas vão para staging.
* Reprocessar o mesmo arquivo não duplica staging.
* O merge atualiza registros existentes na tabela final.
* O merge insere registros novos na tabela final.
* Reexecutar o merge não duplica registros finais.
* Tudo roda localmente sem AWS real e sem LocalStack.