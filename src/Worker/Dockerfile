# Build
# Tags pinadas na versao stable exata (.NET 10 = LTS ativo). Antes: sdk:10.0 / runtime:10.0 (flutuante).
FROM mcr.microsoft.com/dotnet/sdk:10.0.401 AS build
WORKDIR /src
COPY src/Worker/Worker.csproj .
RUN dotnet restore
COPY src/Worker/ .
RUN dotnet publish -c Release -o /app

# Runtime
FROM mcr.microsoft.com/dotnet/runtime:10.0.12 AS runtime
WORKDIR /app
COPY --from=build /app .

# Diretorio de paginacao (arquivo temporario do S3). Configuravel via Consumer__TempPath.
RUN mkdir -p /tmp/poc-ingest

ENTRYPOINT ["dotnet", "Worker.dll"]
