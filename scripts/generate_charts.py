"""Generate charts from simulation CSV metrics.

Usage:
    python3 scripts/generate_charts.py metrics.csv
    python3 scripts/generate_charts.py metrics.csv --output charts/
"""

import argparse
import pandas as pd
import matplotlib.pyplot as plt
import os

def generate_charts(csv_file, output_dir="."):
    """Generate charts from simulation metrics CSV."""
    
    df = pd.read_csv(csv_file)
    
    # Create output directory if needed
    os.makedirs(output_dir, exist_ok=True)
    
    base_name = os.path.splitext(os.path.basename(csv_file))[0]
    
    print(f"Generating charts from {len(df)} data points...")
    
    # Chart 1: Throughput over time
    plt.figure(figsize=(12, 6))
    if 'batch_time_ms' in df.columns:
        plt.plot(df['timestamp'], df['batch_time_ms'], 'b-', linewidth=2)
        plt.xlabel('Tempo (s)')
        plt.ylabel('Tempo do Batch (ms)')
        plt.title('Tempo de Processamento por Batch')
        plt.grid(True)
        plt.savefig(f'{output_dir}/{base_name}_batch_time.png', dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  ✓ {base_name}_batch_time.png")
    
    # Chart 2: Pending locks over time
    if 'pending_locks' in df.columns:
        plt.figure(figsize=(12, 6))
        plt.plot(df['timestamp'], df['pending_locks'], 'r-', linewidth=2)
        plt.xlabel('Tempo (s)')
        plt.ylabel('Locks Pendentes')
        plt.title('Lock Contention durante Merge')
        plt.grid(True)
        plt.savefig(f'{output_dir}/{base_name}_locks.png', dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  ✓ {base_name}_locks.png")
    
    # Chart 3: Dead tuples accumulation
    if 'dead_custody' in df.columns and 'dead_buffer' in df.columns:
        plt.figure(figsize=(12, 6))
        plt.plot(df['timestamp'], df['dead_custody'], 'b-', label='custody_position', linewidth=2)
        plt.plot(df['timestamp'], df['dead_buffer'], 'orange', label='custody_position_buffer', linewidth=2)
        plt.xlabel('Tempo (s)')
        plt.ylabel('Dead Tuples')
        plt.title('Acúmulo de Dead Tuples durante Merge')
        plt.legend()
        plt.grid(True)
        plt.savefig(f'{output_dir}/{base_name}_dead_tuples.png', dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  ✓ {base_name}_dead_tuples.png")
    
    # Chart 4: Cache hit ratio
    if 'cache_hit_ratio' in df.columns:
        plt.figure(figsize=(12, 6))
        plt.plot(df['timestamp'], df['cache_hit_ratio'], 'g-', linewidth=2)
        plt.xlabel('Tempo (s)')
        plt.ylabel('Cache Hit Ratio (%)')
        plt.title('Cache Hit Ratio durante Merge')
        plt.ylim(0, 100)
        plt.grid(True)
        plt.savefig(f'{output_dir}/{base_name}_cache.png', dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  ✓ {base_name}_cache.png")
    
    # Chart 5: Combined overview (summary dashboard)
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(f'Simulation Metrics: {base_name}', fontsize=14, fontweight='bold')
    
    if 'batch_time_ms' in df.columns:
        axes[0, 0].plot(df['timestamp'], df['batch_time_ms'], 'b-', linewidth=2)
        axes[0, 0].set_xlabel('Tempo (s)')
        axes[0, 0].set_ylabel('ms')
        axes[0, 0].set_title('Batch Time')
        axes[0, 0].grid(True)
    
    if 'pending_locks' in df.columns:
        axes[0, 1].plot(df['timestamp'], df['pending_locks'], 'r-', linewidth=2)
        axes[0, 1].set_xlabel('Tempo (s)')
        axes[0, 1].set_ylabel('Locks')
        axes[0, 1].set_title('Pending Locks')
        axes[0, 1].grid(True)
    
    if 'dead_custody' in df.columns:
        axes[1, 0].plot(df['timestamp'], df['dead_custody'], 'b-', label='custody', linewidth=2)
        if 'dead_buffer' in df.columns:
            axes[1, 0].plot(df['timestamp'], df['dead_buffer'], 'orange', label='buffer', linewidth=2)
        axes[1, 0].set_xlabel('Tempo (s)')
        axes[1, 0].set_ylabel('Dead Tuples')
        axes[1, 0].set_title('Dead Tuples')
        axes[1, 0].legend()
        axes[1, 0].grid(True)
    
    if 'cache_hit_ratio' in df.columns:
        axes[1, 1].plot(df['timestamp'], df['cache_hit_ratio'], 'g-', linewidth=2)
        axes[1, 1].set_xlabel('Tempo (s)')
        axes[1, 1].set_ylabel('%')
        axes[1, 1].set_title('Cache Hit Ratio')
        axes[1, 1].set_ylim(0, 100)
        axes[1, 1].grid(True)
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/{base_name}_dashboard.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  ✓ {base_name}_dashboard.png")
    
    print(f"\nCharts saved to: {output_dir}/")


def main():
    parser = argparse.ArgumentParser(description="Generate charts from simulation CSV")
    parser.add_argument("csv_file", help="Path to CSV file")
    parser.add_argument("--output", "-o", default=".", help="Output directory")
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_file):
        print(f"Error: File not found: {args.csv_file}")
        return
    
    generate_charts(args.csv_file, args.output)


if __name__ == "__main__":
    main()
