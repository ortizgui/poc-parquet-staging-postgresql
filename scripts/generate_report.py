"""Generate a Grafana-style dark dashboard HTML report from simulation CSV metrics.

Usage:
    python3 scripts/generate_report.py reports/metrics.csv
    python3 scripts/generate_report.py reports/metrics.csv --output custom.html
"""

import argparse
import json
import os
import pandas as pd
from datetime import datetime


METRICS = [
    {
        "key": "Total de Registros",
        "calc": lambda df: df['inserted'].sum() + df['updated'].sum(),
        "format": "int",
        "tooltip": "Soma de registros inseridos + atualizados"
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
        "calc": lambda df: (df['inserted'].sum() + df['updated'].sum()) / df['timestamp'].max() if df['timestamp'].max() > 0 else 0,
        "format": "float",
        "unit": " regs/s",
        "tooltip": "Média de registros processados por segundo"
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
        "key": "Total Batches",
        "column": "batch",
        "count": True,
        "format": "int",
        "tooltip": "Quantidade total de batches processados"
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


def prepare_chart_data(df):
    throughput_per_batch = []
    for i, row in df.iterrows():
        batch_time_s = row['batch_time_ms'] / 1000.0
        if batch_time_s > 0:
            processed = row.get('inserted', 0) + row.get('updated', 0)
            throughput_per_batch.append(processed / batch_time_s)
        else:
            throughput_per_batch.append(0)

    return {
        "batchNumbers": [int(x) for x in df['batch'].values],
        "throughput": throughput_per_batch,
        "inserted": [int(x) for x in df['inserted'].values],
        "updated": [int(x) for x in df['updated'].values],
        "batchTimeMs": [float(x) for x in df['batch_time_ms'].values],
    }


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

    chart_data = prepare_chart_data(df)
    chart_data_json = json.dumps(chart_data)

    metrics_html = ""
    for metric in METRICS:
        value = calculate_metric(df, metric)
        formatted = format_value(value, metric.get("format", "int"), metric.get("unit", ""))
        tooltip_text = metric.get("tooltip", "").replace('"', '&quot;')

        metrics_html += f"""
            <div class="metric-card">
                <div class="metric-header">
                    <span class="metric-label">{metric["key"]}</span>
                    <span class="info-icon" data-tooltip="{tooltip_text}" onmouseenter="showTooltip(this)" onmouseleave="hideTooltip(this)">ℹ️</span>
                </div>
                <div class="metric-value">{formatted}</div>
            </div>
        """

    html_content = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Load Simulation Report - Grafana Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
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
            padding: 24px;
        }}

        .header {{
            background: linear-gradient(135deg, #16213e 0%, #0f3460 50%, #16213e 100%);
            border-radius: 12px;
            padding: 32px 40px;
            margin-bottom: 32px;
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
            margin-bottom: 36px;
        }}

        .section-title {{
            color: #fff;
            font-size: 18px;
            font-weight: 600;
            margin-bottom: 24px;
            padding-bottom: 10px;
            border-bottom: 2px solid #0f3460;
            display: flex;
            align-items: center;
            gap: 10px;
        }}

        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 24px;
        }}

        .metric-card {{
            background: #16213e;
            border-radius: 12px;
            padding: 28px;
            min-height: 120px;
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
            margin-bottom: 12px;
        }}

        .metric-label {{
            color: #888;
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 1px;
            font-weight: 500;
        }}

        .info-icon {{
            font-size: 14px;
            cursor: pointer;
            opacity: 0.6;
            transition: opacity 0.3s ease;
            position: relative;
        }}

        .info-icon:hover {{
            opacity: 1;
        }}

        .tooltip {{
            position: absolute;
            bottom: calc(100% + 10px);
            left: 50%;
            transform: translateX(-50%);
            background: #0f3460;
            color: #fff;
            padding: 10px 14px;
            border-radius: 8px;
            font-size: 12px;
            max-width: 280px;
            white-space: normal;
            text-transform: none;
            letter-spacing: 0;
            opacity: 0;
            visibility: hidden;
            transition: all 0.3s ease;
            z-index: 100;
            border: 1px solid #00D9FF;
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.4);
            pointer-events: none;
        }}

        .tooltip::after {{
            content: '';
            position: absolute;
            top: 100%;
            left: 50%;
            transform: translateX(-50%);
            border: 6px solid transparent;
            border-top-color: #0f3460;
        }}

        .tooltip.visible {{
            opacity: 1;
            visibility: visible;
        }}

        .metric-value {{
            font-size: 36px;
            font-weight: 700;
            color: #00D9FF;
        }}

        .charts-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 24px;
        }}

        .chart-card {{
            background: #16213e;
            border-radius: 12px;
            padding: 24px;
            border: 1px solid #0f3460;
            transition: all 0.3s ease;
        }}

        .chart-card:hover {{
            box-shadow: 0 8px 25px rgba(0, 217, 255, 0.1);
            border-color: #0f3460;
        }}

        .chart-card h3 {{
            color: #fff;
            font-size: 15px;
            font-weight: 600;
            margin-bottom: 16px;
            display: flex;
            align-items: center;
            gap: 8px;
        }}

        .chart-container {{
            position: relative;
            height: 250px;
        }}

        .footer {{
            text-align: center;
            color: #555;
            padding: 30px;
            font-size: 12px;
        }}

        @media (max-width: 1024px) {{
            .metrics-grid {{
                grid-template-columns: repeat(2, 1fr);
            }}
        }}

        @media (max-width: 768px) {{
            .metrics-grid {{
                grid-template-columns: 1fr;
            }}
            .charts-grid {{
                grid-template-columns: 1fr;
            }}
            .header {{
                padding: 24px;
            }}
            .header h1 {{
                font-size: 22px;
            }}
            .metric-value {{
                font-size: 30px;
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
                <div class="chart-card">
                    <h3>📈 Throughput over Time</h3>
                    <div class="chart-container">
                        <canvas id="chart-throughput"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>📊 Inserted vs Updated</h3>
                    <div class="chart-container">
                        <canvas id="chart-inserted-updated"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h3>⏱️ Batch Time</h3>
                    <div class="chart-container">
                        <canvas id="chart-batch-time"></canvas>
                    </div>
                </div>
            </div>
        </div>

        <div class="footer">
            Load Simulation Report | POC Parquet Staging PostgreSQL
        </div>
    </div>

    <script>
        const chartData = {chart_data_json};

        function showTooltip(el) {{
            const tooltip = el.querySelector('.tooltip') || createTooltip(el);
            tooltip.classList.add('visible');
        }}

        function hideTooltip(el) {{
            const tooltip = el.querySelector('.tooltip');
            if (tooltip) tooltip.classList.remove('visible');
        }}

        function createTooltip(el) {{
            const tooltip = document.createElement('div');
            tooltip.className = 'tooltip';
            tooltip.textContent = el.getAttribute('data-tooltip');
            el.appendChild(tooltip);
            return tooltip;
        }}

        const chartOptions = {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{
                legend: {{ display: false }},
                tooltip: {{
                    mode: 'index',
                    intersect: false,
                    backgroundColor: '#0f3460',
                    titleColor: '#fff',
                    bodyColor: '#fff',
                    borderColor: '#00D9FF',
                    borderWidth: 1,
                    padding: 12,
                    cornerRadius: 8
                }}
            }}
        }};

        new Chart(document.getElementById('chart-throughput'), {{
            type: 'line',
            data: {{
                labels: chartData.batchNumbers,
                datasets: [{{
                    label: 'Throughput (regs/s)',
                    data: chartData.throughput,
                    borderColor: '#00D9FF',
                    backgroundColor: 'rgba(0, 217, 255, 0.1)',
                    fill: true,
                    tension: 0.4,
                    pointRadius: 4,
                    pointHoverRadius: 6
                }}]
            }},
            options: chartOptions
        }});

        new Chart(document.getElementById('chart-inserted-updated'), {{
            type: 'bar',
            data: {{
                labels: chartData.batchNumbers,
                datasets: [
                    {{
                        label: 'Inserted',
                        data: chartData.inserted,
                        backgroundColor: 'rgba(0, 217, 255, 0.8)',
                        borderRadius: 4
                    }},
                    {{
                        label: 'Updated',
                        data: chartData.updated,
                        backgroundColor: 'rgba(255, 107, 107, 0.8)',
                        borderRadius: 4
                    }}
                ]
            }},
            options: {{
                ...chartOptions,
                scales: {{
                    x: {{
                        grid: {{ color: 'rgba(255,255,255,0.05)' }},
                        ticks: {{ color: '#888' }}
                    }},
                    y: {{
                        grid: {{ color: 'rgba(255,255,255,0.05)' }},
                        ticks: {{ color: '#888' }}
                    }}
                }}
            }}
        }});

        new Chart(document.getElementById('chart-batch-time'), {{
            type: 'line',
            data: {{
                labels: chartData.batchNumbers,
                datasets: [{{
                    label: 'Batch Time (ms)',
                    data: chartData.batchTimeMs,
                    borderColor: '#FFD93D',
                    backgroundColor: 'rgba(255, 217, 61, 0.1)',
                    fill: true,
                    tension: 0.4,
                    pointRadius: 4,
                    pointHoverRadius: 6
                }}]
            }},
            options: chartOptions
        }});
    </script>
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
