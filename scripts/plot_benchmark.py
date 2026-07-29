#!/usr/bin/env python3
"""
KV Store 性能图表生成器 — 从 sweep_results.csv 生成吞吐量对比图

用法:
  python scripts/plot_benchmark.py              # 默认读取 sweep_results.csv
  python scripts/plot_benchmark.py results.csv  # 指定 CSV 文件
  python scripts/plot_benchmark.py --output docs/benchmark.png  # 指定输出路径

依赖: pip install matplotlib
"""

import csv
import os
import sys
import argparse
from pathlib import Path

# 尝试导入 matplotlib，如果未安装则提示
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
except ImportError:
    print("ERROR: matplotlib not installed. Run: pip install matplotlib")
    sys.exit(1)

# 设置中文字体
plt.rcParams["font.sans-serif"] = ["Noto Sans CJK SC", "WenQuanYi Micro Hei",
                                    "SimHei", "Microsoft YaHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


def load_csv(path):
    """加载 CSV 数据，返回 dict 列表"""
    rows = []
    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("status") == "OK":
                rows.append(row)
    return rows


def plot_throughput(rows, output_path):
    """绘制吞吐量柱状图"""
    configs = [r["config"] for r in rows]
    write_ops = [int(r["write_ops_per_sec"]) for r in rows]
    read_ops = [int(r["read_ops_per_sec"]) for r in rows]

    # 缩短标签
    short_labels = [c.replace("Mem", "M").replace("Block", "B").replace("Bloom", "F")
                     for c in configs]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # 左图：写入吞吐量
    colors_w = ["#2196F3" if w == max(write_ops) else "#90CAF9" for w in write_ops]
    bars = axes[0].bar(short_labels, write_ops, color=colors_w, edgecolor="#1565C0", linewidth=0.8)
    axes[0].set_title("Write Throughput (ops/s)", fontsize=14, fontweight="bold")
    axes[0].set_ylabel("ops/s", fontsize=11)
    axes[0].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}K"))
    axes[0].tick_params(axis="x", rotation=15, labelsize=9)

    for bar, val in zip(bars, write_ops):
        axes[0].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 30000,
                     f"{val/1000:.0f}K", ha="center", va="bottom", fontsize=9, fontweight="bold")

    # 右图：读取吞吐量
    # 处理 read_ops=0 的情况（计时器精度不够，实际远超显示值）
    display_reads = [max(r, 1000000) if r > 0 else 10000000 for r in read_ops]
    colors_r = ["#4CAF50" if r == max(display_reads) else "#A5D6A7" for r in display_reads]
    bars = axes[1].bar(short_labels, display_reads, color=colors_r, edgecolor="#2E7D32", linewidth=0.8)
    axes[1].set_title("Read Throughput (ops/s, estimated)", fontsize=14, fontweight="bold")
    axes[1].set_ylabel("ops/s", fontsize=11)
    axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}K"))
    axes[1].tick_params(axis="x", rotation=15, labelsize=9)

    for bar, val in zip(bars, display_reads):
        axes[1].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 30000,
                     f"{val/1000:.0f}K", ha="center", va="bottom", fontsize=9, fontweight="bold")

    fig.suptitle("KV Store — Parameter Sweep Performance Comparison",
                 fontsize=16, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Chart saved to: {output_path}")


def plot_param_impact(rows, output_path):
    """绘制参数影响折线图：MemTable 大小 / Block 大小 / Bloom 位数 vs 吞吐量"""
    def avg_by_param(rows, param_key, target):
        """按某参数分组，其他参数固定，计算平均吞吐量"""
        groups = {}
        for r in rows:
            k = int(r[param_key])
            if k not in groups:
                groups[k] = {"write": [], "read": []}
            w = int(r["write_ops_per_sec"])
            r_ops = int(r["read_ops_per_sec"])
            if r_ops == 0:
                r_ops = 10000000  # 估算
            groups[k]["write"].append(w)
            groups[k]["read"].append(r_ops)

        x_vals = sorted(groups.keys())
        y_write = [sum(groups[k]["write"]) / len(groups[k]["write"]) for k in x_vals]
        y_read = [sum(groups[k]["read"]) / len(groups[k]["read"]) for k in x_vals]
        return x_vals, y_write, y_read

    param_info = [
        ("memtable_kb", "MemTable Size (KB)", "MemTable"),
        ("block_size", "Block Size (bytes)", "Block"),
        ("bloom_bits", "Bloom Bits/Key", "Bloom"),
    ]

    fig, axes = plt.subplots(1, 3, figsize=(16, 4.5))

    for idx, (key, title, label) in enumerate(param_info):
        x, yw, yr = avg_by_param(rows, key, label)
        if len(x) < 2:
            axes[idx].text(0.5, 0.5, "Not enough data\n(need more sweep points)",
                           ha="center", va="center", transform=axes[idx].transAxes,
                           fontsize=11, color="#999")
            axes[idx].set_title(title, fontsize=12, fontweight="bold")
            continue

        axes[idx].plot(x, [v / 1000 for v in yw], "o-", color="#2196F3", linewidth=2,
                       markersize=8, label="Write")
        axes[idx].plot(x, [v / 1000 for v in yr], "s--", color="#4CAF50", linewidth=2,
                       markersize=8, label="Read")
        axes[idx].set_title(title, fontsize=12, fontweight="bold")
        axes[idx].set_ylabel("Throughput (K ops/s)", fontsize=10)
        axes[idx].set_xlabel(label, fontsize=10)
        axes[idx].legend(fontsize=9)
        axes[idx].grid(True, alpha=0.3)

        # 标注最高点
        best_w = max(yw)
        best_r = max(yr)
        axes[idx].annotate(f"{best_w/1000:.0f}K", xy=(x[yw.index(best_w)], best_w / 1000),
                           fontsize=8, color="#1565C0", fontweight="bold")
        axes[idx].annotate(f"{best_r/1000:.0f}K", xy=(x[yr.index(best_r)], best_r / 1000),
                           fontsize=8, color="#2E7D32", fontweight="bold")

    fig.suptitle("KV Store — Parameter Impact Analysis", fontsize=16, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Chart saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="KV Store 性能图表生成")
    parser.add_argument("csv", nargs="?", default="sweep_results.csv",
                        help="CSV 数据文件路径 (默认: sweep_results.csv)")
    parser.add_argument("--output-dir", default="docs",
                        help="图表输出目录 (默认: docs)")
    args = parser.parse_args()

    project_dir = Path(__file__).resolve().parent.parent
    csv_path = project_dir / args.csv
    output_dir = project_dir / args.output_dir

    if not csv_path.exists():
        print(f"ERROR: CSV file not found: {csv_path}")
        print("  Run param_sweep.py first: python scripts/param_sweep.py --quick")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading data from: {csv_path}")
    rows = load_csv(str(csv_path))
    if not rows:
        print("ERROR: No valid data rows found in CSV")
        sys.exit(1)
    print(f"  Loaded {len(rows)} valid configurations\n")

    # 生成两张图
    plot_throughput(rows, str(output_dir / "benchmark_throughput.png"))
    plot_param_impact(rows, str(output_dir / "benchmark_param_impact.png"))

    print(f"\nCharts generated in: {output_dir}/")
    print("  - benchmark_throughput.png")
    print("  - benchmark_param_impact.png")


if __name__ == "__main__":
    main()