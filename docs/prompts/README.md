# Prompts para agentes de IA — paginação por row group

Dois prompts **auto-contidos e executáveis** por um agente de codificação. Eles traduzem a tese desta
POC (o pico de memória do consumidor é definido pelo **maior row group**, não pelo tamanho do
arquivo) em tarefas concretas nos dois lados do fluxo.

> Tese e evidência: [`../evidencias/README.md`](../evidencias/README.md) ·
> [`../memory-test-results.json`](../memory-test-results.json) ·
> [`../aws-producao-ecs-fargate.md`](../aws-producao-ecs-fargate.md) ·
> [`../replicar-testes-e-metricas.md`](../replicar-testes-e-metricas.md) ·
> [`../../scripts/test_rowgroup_ab.sh`](../../scripts/test_rowgroup_ab.sh) ·
> [`../../README.md`](../../README.md).

| Prompt | Use quando | Arquivo |
|---|---|---|
| **Produtor** | Você controla quem **escreve** o Parquet e precisa garantir que o maior row group caiba no pod consumidor (parametrizar + validar o orçamento no pipeline). | [`producer-row-group-size.md`](producer-row-group-size.md) |
| **Consumidor** | Você precisa **ler** Parquet do S3 em .NET paginando por row group, com memória chapada mesmo para arquivo maior que a RAM, projeção de colunas e falha limpa em RG gigante. | [`consumer-leitura-row-groups-dotnet.md`](consumer-leitura-row-groups-dotnet.md) |

## Como usar

1. Escolha o prompt pelo lado em que você atua (não misture responsabilidades).
2. Entregue o arquivo inteiro ao agente como contexto. Ele é estruturado em: problema, contexto,
   requisitos, restrições, implementação de referência, critérios de aceite, anti-padrões e receita
   de teste.
3. A implementação de referência real está em `src/Worker/Services/` (consumidor) e
   `scripts/generate_large_parquet.py` (gerador com `--row-group-rows`).
4. O aceite de ambos converge no A/B de [`../../scripts/test_rowgroup_ab.sh`](../../scripts/test_rowgroup_ab.sh):
   mesmo limite, muitos RGs → `ok` com pico plano; 1 RG gigante → falha por memória.

## Resultado medido de referência (A/B a 192 MB)

| Cenário | Row groups | Maior RG | Pico | Resultado |
|---|---|---|---|---|
| MUITOS_RG | 58 | 32,5 MiB | **109,4 MiB** | `ok` — +1.073.909 ins / ~85.920 upd / −0 err |
| UM_RG | 1 | 1.803,6 MiB | **176,9 MiB** | `out_of_memory` gerenciada (0 linhas) |

Detalhe bruto em [`../evidencias/`](../evidencias/README.md).
