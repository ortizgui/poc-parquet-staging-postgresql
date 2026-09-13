#!/usr/bin/env python3
"""Coleta as metricas da ingestao no Prometheus e resume os 4 golden signals.

Por que existe: o CSV do `run_memory_test.sh` mede a memoria do container via
`docker stats`. Isso responde "o pod estourou?", mas nao responde "quanto trafegou",
"quantas requisicoes Range", "quantos row groups", "qual o pico pelo cgroup". Essas
series vivem no Prometheus (alimentado pelo `/metrics` do worker e pelo cadvisor) e
sao **a mesma fonte que o Grafana usa** — entao o numero que este script imprime e o
numero que o painel mostra.

Sem dependencias externas (so a stdlib), para rodar em qualquer maquina com python3.

Uso:
    ./scripts/collect_metrics.py
    ./scripts/collect_metrics.py --minutes 3
    ./scripts/collect_metrics.py --prometheus http://localhost:9090 --out reports/metrics.json

Saida: resumo legivel no stdout + JSON (para anexar a evidencia do teste).
"""

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request

# (rotulo, expressao PromQL, golden signal, unidade)
#
# Saturacao: o pico e medido de DUAS formas de proposito.
#   - container_memory_working_set_bytes (cadvisor): e o numero que decide o OOMKilled,
#     porque compara com o limite do cgroup. É a saturacao do POD.
#   - process_working_set_bytes (prometheus-net): o que o processo enxerga. Serve de
#     contraste — se o pod morre e o processo nao chegou perto, o pico estava fora do
#     heap gerenciado (buffer nativo, arena do GC, etc.).
QUERIES = [
    ("linhas_lidas",        "poc_parquet_rows_processed_total",                   "traffic",    "linhas"),
    ("bytes_s3",            "poc_parquet_bytes_downloaded_total",                 "traffic",    "bytes"),
    ("requisicoes_range",   "poc_parquet_range_requests_total",                   "traffic",    "reqs"),
    ("row_groups",          "poc_parquet_row_groups_total",                       "traffic",    "grupos"),
    ("arquivos",            "poc_ingest_files_total",                             "traffic",    "arquivos"),
    ("ultimo_arquivo_mb",   "poc_ingest_last_file_bytes / 1024 / 1024",           "traffic",    "MB"),
    ("linhas_inseridas",    "poc_db_records_inserted_total",                      "traffic",    "linhas"),
    ("linhas_atualizadas",  "poc_db_records_updated_total",                       "traffic",    "linhas"),
    ("registros_invalidos", "poc_ingest_invalid_records_total",                   "errors",     "linhas"),
    ("falhas_mensagem",     "poc_sqs_message_failures_total",                     "errors",     "msgs"),
    ("enviados_dlq",        "poc_sqs_messages_sent_to_dlq_total",                 "errors",     "msgs"),
    ("dlq_profundidade",    "poc_sqs_dlq_depth",                                  "errors",     "msgs"),
    ("fila_profundidade",   "poc_sqs_queue_depth",                                "traffic",    "msgs"),
    ("upsert_p95_s",        "histogram_quantile(0.95, rate(poc_db_upsert_seconds_bucket[5m]))",   "latency", "s"),
    ("rowgroup_p95_s",      "histogram_quantile(0.95, rate(poc_parquet_rowgroup_read_seconds_bucket[5m]))", "latency", "s"),
]

PEAKS = [
    ("pico_memoria_pod_mb",
     'max_over_time(container_memory_working_set_bytes{{name=~".*{container}.*"}}[{w}m]) / 1024 / 1024',
     "saturation", "MB"),
    ("pico_memoria_processo_mb",
     'max_over_time(process_working_set_bytes[{w}m]) / 1024 / 1024',
     "saturation", "MB"),
    ("pico_heap_gerenciado_mb",
     'max_over_time(dotnet_total_managed_memory_bytes[{w}m]) / 1024 / 1024',
     "saturation", "MB"),
    ("cpu_throttled_ratio",
     'rate(container_cpu_cfs_throttled_periods_total{{name=~".*{container}.*"}}[{w}m])',
     "saturation", "ratio"),
]


def prom(prometheus_url, expr, timeout=10):
    """Consulta instantanea. Retorna float ou None (serie ausente nao e erro)."""
    url = f"{prometheus_url.rstrip('/')}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            payload = json.load(r)
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
        raise SystemExit(
            f"erro: nao consegui consultar o Prometheus em {prometheus_url} ({e}).\n"
            f"       a stack esta no ar? (docker compose up -d)  confira --prometheus"
        )
    if payload.get("status") != "success":
        return None
    result = payload.get("data", {}).get("result", [])
    if not result:
        return None
    try:
        return float(result[0]["value"][1])
    except (KeyError, IndexError, ValueError):
        return None


def fmt(value, unit):
    if value is None:
        return "n/d"
    if unit == "bytes":
        return f"{value:,.0f} B ({value / 1024 / 1024:.1f} MB)"
    if unit == "ratio":
        return f"{value:.4f}"
    if unit == "s":
        return f"{value:.4f} s"
    if abs(value) >= 1000:
        return f"{value:,.0f} {unit}"
    return f"{value:,.2f} {unit}"


def main():
    p = argparse.ArgumentParser(description="Resume as metricas da ingestao direto do Prometheus.")
    p.add_argument("--prometheus", default="http://localhost:9090", help="URL do Prometheus")
    p.add_argument("--minutes", type=int, default=10, help="janela para os picos (default: 10)")
    p.add_argument("--container", default="poc-consumer", help="nome do container para o filtro do cadvisor")
    p.add_argument("--out", default=None, help="caminho do JSON de saida")
    args = p.parse_args()

    out = {"janela_minutos": args.minutes, "prometheus": args.prometheus,
           "container": args.container, "metricas": {}, "picos": {}}

    print(f"== metricas da ingestao (janela de {args.minutes} min) ==")
    atual = None
    for label, expr, signal, unit in QUERIES:
        v = prom(args.prometheus, expr)
        out["metricas"][label] = {"valor": v, "unidade": unit, "golden_signal": signal, "promql": expr}
        if signal != atual:
            print(f"\n  [{signal}]")
            atual = signal
        print(f"    {label:<20} {fmt(v, unit)}")

    print("\n  [saturation — picos na janela]")
    for label, tmpl, signal, unit in PEAKS:
        expr = tmpl.format(w=args.minutes, container=args.container)
        v = prom(args.prometheus, expr)
        out["picos"][label] = {"valor": v, "unidade": unit, "promql": expr}
        print(f"    {label:<26} {fmt(v, unit)}")

    # Veredito de trafego: a projecao de colunas esta funcionando?
    baixado = out["metricas"].get("bytes_s3", {}).get("valor")
    arquivo = out["metricas"].get("ultimo_arquivo_mb", {}).get("valor")
    if baixado and arquivo:
        pct = baixado / (arquivo * 1024 * 1024) * 100
        reqs = out["metricas"].get("requisicoes_range", {}).get("valor") or 0
        out["veredito_trafego_pct"] = round(pct, 2)
        print(f"\n  [veredito] trafego = {pct:.2f}% do ultimo arquivo"
              f" ({baixado / 1024 / 1024:.1f} MB de {arquivo:.1f} MB) em {reqs:,.0f} requisicoes Range")
        if pct > 90:
            print("             ATENCAO: ~100% do objeto — a projecao nao esta ativa ou o ReadMode virou LocalFile.")
        elif pct < 20:
            print("             OK: leitura parcial funcionando (so a fracao das colunas projetadas).")

    pico_pod = out["picos"].get("pico_memoria_pod_mb", {}).get("valor")
    if pico_pod is None:
        print("\n  [aviso] pico do POD indisponivel: sem serie do cadvisor.")
        print("          Em Docker-in-Docker o cadvisor pode nao enumerar os cgroups aninhados —")
        print("          use o 'docker stats' do run_memory_test.sh como fonte do pico.")

    if args.out:
        with open(args.out, "w") as f:
            json.dump(out, f, indent=2, ensure_ascii=False)
        print(f"\n  json: {args.out}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
