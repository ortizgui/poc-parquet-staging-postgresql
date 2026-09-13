#!/usr/bin/env python3
"""Gera o grafico do teste de memoria/paginacao da ingestao.

Le dois insumos:
  --results  docs/memory-test-results.json   (o que foi medido, por cenario)
  --curve    reports/memory_test_DEPOIS.csv  (amostras de memoria do worker paginado)

Painel esquerdo: pico de memoria por cenario, contra o limite do pod.
Painel direito : a curva do worker paginado (o plato abaixo do limite).

Uso:
    pip install matplotlib
    python3 scripts/plot_memory_test.py \
        --results docs/memory-test-results.json \
        --curve reports/memory_test_DEPOIS.csv \
        --out docs/assets/memoria-antes-depois.png
"""

import argparse
import csv
import json
import os
import sys

CORES = {"ok": "#2ca02c", "falhou": "#d62728", "nao-deterministico": "#ff7f0e", "OOMKilled": "#8c564b", "falha_memoria": "#8c564b"}
ROTULOS = {"ok": "OK", "falhou": "OutOfMemory", "nao-deterministico": "oscilou", "OOMKilled": "OOMKilled", "falha_memoria": "Falha de memoria"}


def ler_curva(path):
    ts, mem = [], []
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            try:
                ts.append(int(row["t_s"]))
                mem.append(float(row["mem_mib"] or 0))
            except (KeyError, ValueError):
                continue
    return ts, mem


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--results", required=True)
    p.add_argument("--curve", default=None)
    p.add_argument("--out", default="docs/assets/memoria-antes-depois.png")
    p.add_argument("--titulo", default="POC ingestao Parquet -> PostgreSQL: memoria do pod no teste de paginacao")
    args = p.parse_args()

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        sys.exit("matplotlib nao instalado: pip install matplotlib")

    dados = json.load(open(args.results, encoding="utf-8"))
    limite = int(dados["ambiente"]["limite_do_pod"].split()[0].replace("MB", "").strip()) \
        if "limite_do_pod" in dados["ambiente"] else 512

    # deduplica: para cada (codigo, arquivo) fica o pico observado mais alto
    cenarios = {}
    for r in dados["resultados"]:
        chave = (r["codigo"].split("(")[0].strip(), r["arquivo"], r["limite_mb"])
        atual = cenarios.get(chave)
        if atual is None or r["pico_mb"] > atual["pico_mb"]:
            cenarios[chave] = r
    cenarios = sorted(cenarios.values(), key=lambda r: (r["limite_mb"], r["pico_mb"]))

    tem_curva = bool(args.curve) and os.path.exists(args.curve)
    ncols = 2 if tem_curva else 1
    fig, axes = plt.subplots(1, ncols, figsize=(13 if tem_curva else 8, 6), dpi=130)
    ax = axes[0] if tem_curva else axes

    labels = [f"{('ANTES' if 'antes' in c['codigo'] else 'DEPOIS')}\n{c['arquivo']}\n{c['limite_mb']}MB" for c in cenarios]
    valores = [c["pico_mb"] for c in cenarios]
    cores = [CORES.get(c["resultado"], "#7f7f7f") for c in cenarios]

    barras = ax.bar(range(len(cenarios)), valores, color=cores, width=0.62)
    ax.axhline(limite, color="#d62728", linestyle="--", linewidth=1.8, label=f"limite do pod ({limite} MB)")

    for i, (b, c) in enumerate(zip(barras, cenarios)):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height() + limite * 0.02,
                f"{c['pico_mb']:.0f} MB\n{ROTULOS.get(c['resultado'], c['resultado'])}\n{c['linhas_gravadas']:,} linhas",
                ha="center", va="bottom", fontsize=8, color="#333333")

    ax.set_xticks(range(len(cenarios)))
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("pico do working set do container (MB)")
    ax.set_title("Pico de memoria por cenario")
    ax.set_ylim(0, max(limite * 1.28, max(valores or [1]) * 1.5))
    ax.grid(axis="y", alpha=0.3, linestyle=":")
    ax.legend(fontsize=8, loc="upper left")

    if tem_curva:
        ts, mem = ler_curva(args.curve)
        ax2 = axes[1]
        ax2.axhline(limite, color="#d62728", linestyle="--", linewidth=1.6, label=f"limite ({limite} MB)")
        ax2.plot(ts, mem, color="#2ca02c", linewidth=2.2, marker="o", markersize=3.5,
                 label="worker paginado")
        ax2.fill_between(ts, 0, mem, color="#2ca02c", alpha=0.12)
        if mem:
            pico = max(mem)
            ax2.annotate(f"plato em ~{sum(mem)/len(mem):.0f} MB\npico {pico:.0f} MB ({pico*100/limite:.0f}% do limite)",
                         xy=(ts[mem.index(pico)], pico),
                         xytext=(ts[0] + (max(ts) - ts[0]) * 0.18, pico + limite * 0.28),
                         fontsize=9, color="#2ca02c",
                         arrowprops=dict(arrowstyle="->", color="#2ca02c", lw=1.2))
        ax2.set_xlabel("segundos de processamento")
        ax2.set_ylabel("working set do container (MB)")
        ax2.set_title("Curva do worker paginado (arquivo de 358.9 MB)")
        ax2.set_ylim(0, limite * 1.05)
        ax2.grid(alpha=0.3, linestyle=":")
        ax2.legend(fontsize=8, loc="lower right")

    fig.suptitle(args.titulo, fontsize=12)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    fig.savefig(args.out)
    print(f"grafico salvo em {args.out}")
    for c in cenarios:
        print(f"  {c['codigo'][:7]:7} {c['arquivo']:16} {c['limite_mb']:>4}MB -> "
              f"{c['pico_mb']:6.1f} MB | {c['resultado']:18} | {c['linhas_gravadas']:,} linhas")


if __name__ == "__main__":
    main()
