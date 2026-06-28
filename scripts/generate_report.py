"""Generate a single HTML report from simulation CSV metrics.

Usage:
    python3 scripts/generate_report.py metrics.csv
    python3 scripts/generate_report.py metrics.csv --output report.html
"""

import argparse
import os
import pandas as pd
from datetime import datetime

def generate_report(csv_file, output_file=None):
    """Generate a single HTML report with all charts and metrics."""
    
    df = pd.read_csv(csv_file)
    
    if output_file is None:
        base_name = os.path.splitext(os.path.basename(csv_file))[0]
        output_file = f"{base_name}_report.html"
    
    # Calculate summary statistics
    total_records = df['total_processed'].max() if 'total_processed' in df.columns else 0
    total_time = df['timestamp'].max() if 'timestamp' in df.columns and len(df) > 0 else 0
    avg_throughput = total_records / total_time if total_time > 0 else 0
    max_locks = df['pending_locks'].max() if 'pending_locks' in df.columns and len(df) > 0 else 0
    avg_batch_time = df['batch_time_ms'].mean() if 'batch_time_ms' in df.columns and len(df) > 0 else 0
    total_inserted = df['inserted'].sum() if 'inserted' in df.columns and len(df) > 0 else 0
    total_updated = df['updated'].sum() if 'updated' in df.columns and len(df) > 0 else 0
    
    html_content = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Load Simulation Report</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; color: #333; line-height: 1.6; }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
        h1 {{ color: #2c3e50; margin-bottom: 10px; }}
        h2 {{ color: #34495e; margin: 30px 0 15px; border-bottom: 2px solid #3498db; padding-bottom: 8px; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 30px; }}
        .header h1 {{ color: white; margin-bottom: 5px; }}
        .header .date {{ opacity: 0.9; font-size: 14px; }}
        .metrics-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 30px; }}
        .metric-card {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .metric-card .label {{ color: #7f8c8d; font-size: 12px; text-transform: uppercase; letter-spacing: 1px; }}
        .metric-card .value {{ font-size: 28px; font-weight: bold; color: #2c3e50; margin-top: 5px; }}
        .metric-card.highlight {{ background: linear-gradient(135deg, #3498db, #2980b9); color: white; }}
        .metric-card.highlight .label {{ color: rgba(255,255,255,0.8); }}
        .metric-card.highlight .value {{ color: white; }}
        .chart-container {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }}
        .chart-container h3 {{ color: #34495e; margin-bottom: 15px; font-size: 16px; }}
        .charts-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; }}
        .charts-row {{ display: grid; grid-template-columns: 1fr; gap: 20px; }}
        img {{ width: 100%; height: auto; border-radius: 5px; }}
        .summary {{ background: white; padding: 25px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 30px; }}
        .summary h3 {{ margin-bottom: 15px; }}
        .summary ul {{ list-style: none; }}
        .summary li {{ padding: 8px 0; border-bottom: 1px solid #eee; }}
        .summary li:last-child {{ border-bottom: none; }}
        .summary .label {{ color: #7f8c8d; }}
        .summary .value {{ font-weight: bold; color: #2c3e50; float: right; }}
        .footer {{ text-align: center; color: #7f8c8d; padding: 20px; font-size: 12px; }}
        .tag {{ display: inline-block; padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: bold; }}
        .tag.success {{ background: #d4edda; color: #155724; }}
        .tag.warning {{ background: #fff3cd; color: #856404; }}
        .tag.danger {{ background: #f8d7da; color: #721c24; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Load Simulation Report</h1>
            <div class="date">Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</div>
        </div>
        
        <h2>📈 Resumo</h2>
        <div class="metrics-grid">
            <div class="metric-card highlight">
                <div class="label">Registros Processados</div>
                <div class="value">{total_records:,.0f}</div>
            </div>
            <div class="metric-card">
                <div class="label">Tempo Total</div>
                <div class="value">{total_time:.1f}s</div>
            </div>
            <div class="metric-card">
                <div class="label">Throughput Médio</div>
                <div class="value">{avg_throughput:,.0f}/s</div>
            </div>
            <div class="metric-card">
                <div class="label">Tempo Médio/Batch</div>
                <div class="value">{avg_batch_time:.0f}ms</div>
            </div>
            <div class="metric-card">
                <div class="label">Total Inserted</div>
                <div class="value">{total_inserted:,.0f}</div>
            </div>
            <div class="metric-card">
                <div class="label">Total Updated</div>
                <div class="value">{total_updated:,.0f}</div>
            </div>
        </div>
        
        <h2>🔒 Status de Lock</h2>
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="label">Máximo Pending Locks</div>
                <div class="value">{max_locks}</div>
            </div>
            <div class="metric-card">
                <div class="label">Status</div>
                <div class="value">
                    {"<span class='tag success'>Sem Lock Contention</span>" if max_locks == 0 else "<span class='tag warning'>Com Lock</span>"}
                </div>
            </div>
        </div>
        
        <h2>📊 Gráficos</h2>
        
        <h3>Throughput por Batch</h3>
        <div class="chart-container">
            <img src="data:image/png;base64,{generate_chart(df, 'timestamp', 'batch_time_ms', 'Tempo do Batch (ms)', 'Tempo (s)', 'ms')}" alt="Batch Time">
        </div>
        
        <div class="charts-grid">
            <div class="chart-container">
                <h3>Registros por Batch</h3>
                <img src="data:image/png;base64,{generate_chart(df, 'timestamp', 'inserted', 'Inserted', 'Tempo (s)', 'regs')}" alt="Inserted">
            </div>
            <div class="chart-container">
                <h3>Updates por Batch</h3>
                <img src="data:image/png;base64,{generate_chart(df, 'timestamp', 'updated', 'Updated', 'Tempo (s)', 'regs')}" alt="Updated">
            </div>
        </div>
        
        {generate_dead_tuples_chart(df) if has_dead_tuples(df) else ''}
        
        {generate_cache_chart(df) if has_cache_data(df) else ''}
        
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


def generate_chart(df, x_col, y_col, ylabel, xlabel, unit):
    """Generate a base64 encoded PNG chart."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    
    plt.figure(figsize=(10, 5))
    plt.plot(df[x_col], df[y_col], 'b-', linewidth=2, marker='o', markersize=4)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    from io import BytesIO
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
    plt.close()
    
    buffer.seek(0)
    import base64
    return base64.b64encode(buffer.read()).decode()


def has_dead_tuples(df):
    return 'dead_custody' in df.columns or 'dead_buffer' in df.columns


def has_cache_data(df):
    return 'cache_hit_ratio' in df.columns and df['cache_hit_ratio'].notna().any()


def generate_dead_tuples_chart(df):
    """Generate dead tuples chart if data available."""
    if not has_dead_tuples(df):
        return ""
    
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from io import BytesIO
    import base64
    
    plt.figure(figsize=(10, 5))
    
    if 'dead_custody' in df.columns:
        plt.plot(df['timestamp'], df['dead_custody'], 'b-', linewidth=2, label='Principal', marker='o', markersize=4)
    if 'dead_buffer' in df.columns:
        plt.plot(df['timestamp'], df['dead_buffer'], 'orange', linewidth=2, label='Staging', marker='s', markersize=4)
    
    plt.xlabel('Tempo (s)')
    plt.ylabel('Dead Tuples')
    plt.title('Acúmulo de Dead Tuples')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
    plt.close()
    buffer.seek(0)
    
    chart_base64 = base64.b64encode(buffer.read()).decode()
    
    return f"""
        <h3>Dead Tuples</h3>
        <div class="chart-container">
            <img src="data:image/png;base64,{chart_base64}" alt="Dead Tuples">
        </div>
    """


def generate_cache_chart(df):
    """Generate cache hit ratio chart if data available."""
    if not has_cache_data(df):
        return ""
    
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from io import BytesIO
    import base64
    
    plt.figure(figsize=(10, 5))
    plt.plot(df['timestamp'], df['cache_hit_ratio'], 'g-', linewidth=2, marker='o', markersize=4)
    plt.xlabel('Tempo (s)')
    plt.ylabel('Cache Hit Ratio (%)')
    plt.title('Cache Hit Ratio')
    plt.ylim(0, 100)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
    plt.close()
    buffer.seek(0)
    
    chart_base64 = base64.b64encode(buffer.read()).decode()
    
    return f"""
        <h3>Cache Hit Ratio</h3>
        <div class="chart-container">
            <img src="data:image/png;base64,{chart_base64}" alt="Cache Hit Ratio">
        </div>
    """


def main():
    parser = argparse.ArgumentParser(description="Generate HTML report from simulation CSV")
    parser.add_argument("csv_file", help="Path to CSV file")
    parser.add_argument("--output", "-o", help="Output HTML file")
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_file):
        print(f"Error: File not found: {args.csv_file}")
        return
    
    generate_report(args.csv_file, args.output)


if __name__ == "__main__":
    main()
