"""Generate a Grafana-style dark dashboard HTML report from simulation CSV metrics.

Usage:
    python3 scripts/generate_report.py reports/metrics.csv
    python3 scripts/generate_report.py reports/metrics.csv --output custom.html
"""

import argparse
import base64
import os
import pandas as pd
from datetime import datetime
from io import BytesIO

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
plt.style.use('dark_background')


METRICS = [
    {
        "key": "Registros Processados",
        "column": "total_processed",
        "max": True,
        "format": "int",
        "tooltip": "Total de registros processados da staging para a tabela principal"
    },
    {
        "key": "Tempo Total",
        "column": "timestamp",
        "max": True,
        "format": "time",
        "tooltip": "Tempo total de execução do merge"
    },
    {
        "key": "Throughput Médio",
        "column": "throughput",
        "calc": lambda df: df['total_processed'].max() / df['timestamp'].max() if df['timestamp'].max() > 0 else 0,
        "format": "float",
        "unit": "/s",
        "tooltip": "Média de registros processados por segundo"
    },
    {
        "key": "Tempo Médio/Batch",
        "column": "batch_time_ms",
        "mean": True,
        "format": "float",
        "unit": "ms",
        "tooltip": "Tempo médio de execução de cada batch em milissegundos"
    },
    {
        "key": "Total Inserted",
        "column": "inserted",
        "sum": True,
        "format": "int",
        "tooltip": "Novos registros inseridos na tabela principal"
    },
    {
        "key": "Total Updated",
        "column": "updated",
        "sum": True,
        "format": "int",
        "tooltip": "Registros existentes atualizados na tabela principal"
    },
    {
        "key": "Máximo Pending Locks",
        "column": "pending_locks",
        "max": True,
        "format": "int",
        "tooltip": "Número máximo de locks pendentes durante a execução"
    },
    {
        "key": "Total Batches",
        "column": "batch",
        "count": True,
        "format": "int",
        "tooltip": "Quantidade total de batches processados"
    },
    {
        "key": "Último Batch Time",
        "column": "batch_time_ms",
        "last": True,
        "format": "float",
        "unit": "ms",
        "tooltip": "Tempo do último batch processado"
    },
]


def calculate_metric(df, metric):
    if "calc" in metric:
        return metric["calc"](df)
    col = metric["column"]
    if "sum" in metric:
        return df[col].sum()
    if "mean" in metric:
        return df[col].mean()
    if "max" in metric:
        return df[col].max()
    if "last" in metric:
        return df[col].iloc[-1] if len(df) > 0 else 0
    if "count" in metric:
        return len(df)
    return df[col].iloc[-1] if len(df) > 0 else 0


def format_value(value, fmt, unit=""):
    if fmt == "int":
        return f"{int(value):,}{unit}"
    elif fmt == "float":
        return f"{value:.1f}{unit}"
    elif fmt == "time":
        return f"{value:.1f}s"
    return str(value)


def generate_chart(df, x_col, y_col, title, color="#00D9FF"):
    fig, ax = plt.subplots(figsize=(10, 4), facecolor='#16213e')
    ax.set_facecolor('#16213e')

    ax.plot(df[x_col], df[y_col], color=color, linewidth=2, marker='o', markersize=4)
    ax.fill_between(df[x_col], df[y_col], alpha=0.3, color=color)

    ax.set_xlabel(x_col.replace('_', ' ').title(), color='#888', fontsize=10)
    ax.set_ylabel(y_col.replace('_', ' ').title(), color='#888', fontsize=10)
    ax.tick_params(colors='#888', labelsize=9)
    ax.spines['bottom'].set_color('#333')
    ax.spines['left'].set_color('#333')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(True, alpha=0.2, color='#333')

    ax.set_title(title, color='#fff', fontsize=12, pad=10)

    plt.tight_layout()

    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight', facecolor='#16213e')
    plt.close()

    buffer.seek(0)
    return base64.b64encode(buffer.read()).decode()


def generate_throughput_chart(df):
    if 'batch' not in df.columns or 'batch_time_ms' not in df.columns:
        return None

    throughput_per_batch = []
    for i, row in df.iterrows():
        batch_time_s = row['batch_time_ms'] / 1000.0
        if batch_time_s > 0:
            processed = row.get('inserted', 0) + row.get('updated', 0)
            throughput_per_batch.append(processed / batch_time_s)
        else:
            throughput_per_batch.append(0)

    df_plot = df.copy()
    df_plot['throughput_calc'] = throughput_per_batch

    fig, ax = plt.subplots(figsize=(10, 4), facecolor='#16213e')
    ax.set_facecolor('#16213e')

    ax.plot(df_plot.index, df_plot['throughput_calc'], color='#00D9FF', linewidth=2, marker='o', markersize=4)
    ax.fill_between(df_plot.index, df_plot['throughput_calc'], alpha=0.3, color='#00D9FF')

    ax.set_xlabel('Batch', color='#888', fontsize=10)
    ax.set_ylabel('Throughput (regs/s)', color='#888', fontsize=10)
    ax.tick_params(colors='#888', labelsize=9)
    ax.spines['bottom'].set_color('#333')
    ax.spines['left'].set_color('#333')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(True, alpha=0.2, color='#333')

    ax.set_title('Throughput over Time', color='#fff', fontsize=12, pad=10)

    plt.tight_layout()

    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight', facecolor='#16213e')
    plt.close()

    buffer.seek(0)
    return base64.b64encode(buffer.read()).decode()


def generate_inserted_updated_chart(df):
    if 'batch' not in df.columns:
        return None

    fig, ax = plt.subplots(figsize=(10, 4), facecolor='#16213e')
    ax.set_facecolor('#16213e')

    x = range(len(df))
    width = 0.35

    inserted = df['inserted'].values if 'inserted' in df.columns else [0] * len(df)
    updated = df['updated'].values if 'updated' in df.columns else [0] * len(df)

    bars1 = ax.bar([i - width/2 for i in x], inserted, width, label='Inserted', color='#00D9FF')
    bars2 = ax.bar([i + width/2 for i in x], updated, width, label='Updated', color='#FF6B6B')

    ax.set_xlabel('Batch', color='#888', fontsize=10)
    ax.set_ylabel('Registros', color='#888', fontsize=10)
    ax.tick_params(colors='#888', labelsize=9)
    ax.spines['bottom'].set_color('#333')
    ax.spines['left'].set_color('#333')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(True, alpha=0.2, color='#333', axis='y')
    ax.legend(facecolor='#16213e', edgecolor='#333', labelcolor='#fff', fontsize=9)

    ax.set_title('Inserted vs Updated per Batch', color='#fff', fontsize=12, pad=10)

    plt.tight_layout()

    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight', facecolor='#16213e')
    plt.close()

    buffer.seek(0)
    return base64.b64encode(buffer.read()).decode()


def generate_report(csv_file, output_file=None):
    """Generate a Grafana-style dark dashboard HTML report."""

    df = pd.read_csv(csv_file)

    if output_file is None:
        csv_dir = os.path.dirname(csv_file)
        base_name = os.path.splitext(os.path.basename(csv_file))[0]
        output_file = os.path.join(csv_dir, f"{base_name}_report.html") if csv_dir else f"{base_name}_report.html"

    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    metrics_html = ""
    for metric in METRICS:
        value = calculate_metric(df, metric)
        formatted = format_value(value, metric.get("format", "int"), metric.get("unit", ""))
        tooltip_text = metric.get("tooltip", "")

        metrics_html += f"""
            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-label">{metric["key"]}</span>
                    <span class="info-icon" data-tooltip="{tooltip_text}">ℹ️</span>
                </div>
                <div class="metric-value">{formatted}</div>
            </div>
        """

    throughput_chart = generate_throughput_chart(df) if 'batch' in df.columns else None
    inserted_updated_chart = generate_inserted_updated_chart(df) if 'batch' in df.columns else None

    batch_time_chart = None
    if 'batch' in df.columns and 'batch_time_ms' in df.columns:
        batch_time_chart = generate_chart(df, 'batch', 'batch_time_ms', 'Batch Time (ms)', "#FFD93D")

    charts_html = ""
    if throughput_chart:
        charts_html += f'<div class="chart-card"><h3>📈 Throughput over Time</h3><img src="data:image/png;base64,{throughput_chart}" alt="Throughput Chart"></div>'
    if inserted_updated_chart:
        charts_html += f'<div class="chart-card"><h3>📊 Inserted vs Updated</h3><img src="data:image/png;base64,{inserted_updated_chart}" alt="Inserted vs Updated Chart"></div>'
    if batch_time_chart:
        charts_html += f'<div class="chart-card"><h3>⏱️ Batch Time</h3><img src="data:image/png;base64,{batch_time_chart}" alt="Batch Time Chart"></div>'

    html_content = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Load Simulation Report - Grafana Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}

        body {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #1a1a2e;
            color: #e0e0e0;
            line-height: 1.6;
            min-height: 100vh;
        }}

        .dashboard {{
            max-width: 1600px;
            margin: 0 auto;
            padding: 20px;
        }}

        .header {{
            background: linear-gradient(135deg, #16213e 0%, #0f3460 50%, #16213e 100%);
            border-radius: 12px;
            padding: 30px 40px;
            margin-bottom: 30px;
            border: 1px solid #0f3460;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
        }}

        .header h1 {{
            color: #fff;
            font-size: 28px;
            font-weight: 700;
            margin-bottom: 8px;
            background: linear-gradient(90deg, #00D9FF, #00FF88);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}

        .header .subtitle {{
            color: #888;
            font-size: 14px;
        }}

        .section {{
            margin-bottom: 30px;
        }}

        .section-title {{
            color: #fff;
            font-size: 18px;
            font-weight: 600;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid #0f3460;
            display: flex;
            align-items: center;
            gap: 10px;
        }}

        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 16px;
        }}

        .metric-card {{
            background: #16213e;
            border-radius: 10px;
            padding: 20px;
            border: 1px solid #0f3460;
            transition: all 0.3s ease;
            position: relative;
        }}

        .metric-card:hover {{
            transform: translateY(-4px);
            box-shadow: 0 8px 25px rgba(0, 217, 255, 0.15);
            border-color: #00D9FF;
        }}

        .metric-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }}

        .metric-label {{
            color: #888;
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 1px;
            font-weight: 500;
        }}

        .info-icon {{
            font-size: 14px;
            cursor: pointer;
            position: relative;
            opacity: 0.6;
            transition: opacity 0.3s ease;
        }}

        .info-icon:hover {{
            opacity: 1;
        }}

        .info-icon::before {{
            content: attr(data-tooltip);
            position: absolute;
            bottom: 100%;
            left: 50%;
            transform: translateX(-50%);
            background: #0f3460;
            color: #fff;
            padding: 8px 12px;
            border-radius: 6px;
            font-size: 11px;
            white-space: nowrap;
            max-width: 250px;
            white-space: normal;
            text-transform: none;
            letter-spacing: 0;
            opacity: 0;
            visibility: hidden;
            transition: all 0.3s ease;
            z-index: 100;
            border: 1px solid #00D9FF;
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3);
        }}

        .info-icon:hover::before {{
            opacity: 1;
            visibility: visible;
            bottom: calc(100% + 8px);
        }}

        .metric-value {{
            font-size: 26px;
            font-weight: 700;
            color: #00D9FF;
        }}

        .charts-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(450px, 1fr));
            gap: 20px;
        }}

        .chart-card {{
            background: #16213e;
            border-radius: 10px;
            padding: 20px;
            border: 1px solid #0f3460;
            transition: all 0.3s ease;
        }}

        .chart-card:hover {{
            box-shadow: 0 8px 25px rgba(0, 217, 255, 0.1);
            border-color: #0f3460;
        }}

        .chart-card h3 {{
            color: #fff;
            font-size: 14px;
            font-weight: 600;
            margin-bottom: 15px;
            display: flex;
            align-items: center;
            gap: 8px;
        }}

        .chart-card img {{
            width: 100%;
            height: auto;
            border-radius: 6px;
        }}

        .footer {{
            text-align: center;
            color: #555;
            padding: 30px;
            font-size: 12px;
        }}

        @media (max-width: 768px) {{
            .metrics-grid {{
                grid-template-columns: repeat(2, 1fr);
            }}
            .charts-grid {{
                grid-template-columns: 1fr;
            }}
            .header {{
                padding: 20px;
            }}
            .header h1 {{
                font-size: 22px;
            }}
        }}

        @media (max-width: 480px) {{
            .metrics-grid {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="dashboard">
        <div class="header">
            <h1>📊 Load Simulation Dashboard</h1>
            <div class="subtitle">Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</div>
        </div>

        <div class="section">
            <div class="section-title">📈 Métricas Gerais</div>
            <div class="metrics-grid">
                {metrics_html}
            </div>
        </div>

        <div class="section">
            <div class="section-title">📉 Gráficos</div>
            <div class="charts-grid">
                {charts_html}
            </div>
        </div>

        <div class="footer">
            Load Simulation Report | POC Parquet Staging PostgreSQL
        </div>
    </div>
</body>
</html>"""

    with open(output_file, 'w') as f:
        f.write(html_content)

    print(f"Report generated: {output_file}")
    return output_file


def main():
    parser = argparse.ArgumentParser(description="Generate Grafana-style HTML report from simulation CSV")
    parser.add_argument("csv_file", help="Path to CSV file")
    parser.add_argument("--output", "-o", help="Output HTML file")
    args = parser.parse_args()

    if not os.path.exists(args.csv_file):
        print(f"Error: File not found: {args.csv_file}")
        return

    generate_report(args.csv_file, args.output)


if __name__ == "__main__":
    main()
