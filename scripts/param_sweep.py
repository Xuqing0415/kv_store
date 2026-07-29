#!/usr/bin/env python3
"""
KV Store 参数扫描脚本 — 自动测试不同配置的性能影响并导出 CSV

用法:
  python param_sweep.py              # 运行全部参数组合
  python param_sweep.py --quick      # 快速模式（减少组合数）
  python param_sweep.py --csv out.csv # 指定输出文件

测试参数:
  - MemTable 大小: 256KB(默认) / 1MB / 4MB / 16MB
  - SSTable 块大小: 4KB / 16KB / 64KB
  - BloomFilter bits/key: 10 / 14 / 20
"""

import subprocess
import os
import sys
import time
import csv
import argparse
import shutil
from datetime import datetime

# ===== 配置 =====
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILD_DIR = os.path.join(PROJECT_DIR, "build_sweep")
BENCHMARK_EXE = os.path.join(BUILD_DIR, "kv_test.exe")
TEST_DATA_DIR = os.path.join(PROJECT_DIR, "test_db")

# 参数扫描范围
PARAM_SETS = [
    # (memtable_kb, sstable_block, bloom_bits)
    (256,   4096,  10),
    (256,   4096,  14),
    (256,   4096,  20),
    (256,   16384, 14),
    (256,   65536, 14),
    (1024,  4096,  14),
    (4096,  4096,  14),
    (16384, 4096,  14),
]

QUICK_PARAM_SETS = [
    (256,   4096,  10),
    (256,   4096,  20),
    (256,   65536, 14),
    (4096,  4096,  14),
]


def clean_dir(path):
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)


def run_cmd(cmd, cwd=None, capture=True):
    """运行命令并返回 (returncode, stdout)"""
    kwargs = {"cwd": cwd, "shell": True}
    if capture:
        kwargs["capture_output"] = True
        kwargs["text"] = True
    result = subprocess.run(cmd, **kwargs)
    stdout = result.stdout if capture else ""
    stderr = result.stderr if capture else ""
    if result.returncode != 0 and capture:
        sys.stderr.write(stderr)
    return result.returncode, stdout, stderr


def build_with_params(memtable_kb, block_size, bloom_bits):
    """使用指定参数编译项目"""
    memtable_bytes = memtable_kb * 1024

    cmake_args = [
        f"-DKV_MEMTABLE_SIZE_LIMIT={memtable_bytes}",
        f"-DKV_SSTABLE_BLOCK_SIZE={block_size}",
        f"-DKV_BLOOM_BITS_PER_KEY={bloom_bits}",
        "-DCMAKE_BUILD_TYPE=Release",
        "-DBUILD_TESTS=OFF",
        "-DBUILD_EXAMPLES=OFF",
    ]

    # 清理并重新配置
    clean_dir(BUILD_DIR)
    os.makedirs(BUILD_DIR, exist_ok=True)

    # CMake 配置
    cmd = f'cmake .. -G "MinGW Makefiles" {" ".join(cmake_args)}'
    rc, _, _ = run_cmd(cmd, cwd=BUILD_DIR)
    if rc != 0:
        return False

    # 编译
    rc, _, _ = run_cmd("cmake --build . --target kv_test -j 8", cwd=BUILD_DIR)
    return rc == 0


def parse_benchmark_output(stdout):
    """解析 benchmark 输出，提取性能指标"""
    result = {
        "write_ops_per_sec": 0,
        "write_time_sec": 0,
        "read_hits": 0,
        "read_misses": 0,
        "read_time_sec": 0,
        "scan_count": 0,
        "verify_errors": 0,
    }

    for line in stdout.split("\n"):
        line = line.strip()
        # Write 5000 entries, cost 0.123 sec, 40650 ops/sec
        if "Write" in line and "ops/sec" in line:
            parts = line.split()
            for i, p in enumerate(parts):
                if p.endswith("sec,"):
                    try:
                        result["write_time_sec"] = float(parts[i - 1])
                    except (ValueError, IndexError):
                        pass
                if p == "ops/sec":
                    try:
                        result["write_ops_per_sec"] = float(parts[i - 1])
                    except (ValueError, IndexError):
                        pass
        # Read 1000 random keys, hit 999, miss 1, cost 0.001 sec
        if "Read" in line and "random" in line:
            parts = line.split()
            for i, p in enumerate(parts):
                if p == "hit":
                    try:
                        result["read_hits"] = int(parts[i + 1].rstrip(","))
                    except (ValueError, IndexError):
                        pass
                if p == "miss":
                    try:
                        result["read_misses"] = int(parts[i + 1].rstrip(","))
                    except (ValueError, IndexError):
                        pass
                if p == "sec" and "cost" in line:
                    try:
                        result["read_time_sec"] = float(parts[i - 1])
                    except (ValueError, IndexError):
                        pass
        # Scan all keys: 5000 entries found
        if "Scan all keys" in line:
            try:
                result["scan_count"] = int(line.split(":")[1].strip().split()[0])
            except (ValueError, IndexError):
                pass
        # Data integrity check: 0 errors found
        if "errors found" in line:
            try:
                result["verify_errors"] = int(line.split(":")[1].strip().split()[0])
            except (ValueError, IndexError):
                pass

    return result


def run_benchmark():
    """运行 benchmark 并返回结果"""
    clean_dir(TEST_DATA_DIR)
    rc, stdout, stderr = run_cmd(f"{BENCHMARK_EXE}", cwd=PROJECT_DIR)
    if rc != 0:
        return None
    result = parse_benchmark_output(stdout)
    return result


def main():
    parser = argparse.ArgumentParser(description="KV Store 参数扫描")
    parser.add_argument("--quick", action="store_true", help="快速模式")
    parser.add_argument("--csv", default="sweep_results.csv", help="输出 CSV 文件")
    parser.add_argument("--repeat", type=int, default=1, help="每组重复次数")
    args = parser.parse_args()

    param_sets = QUICK_PARAM_SETS if args.quick else PARAM_SETS
    results = []

    print("=" * 80)
    print(f"  KV Store Parameter Sweep — {len(param_sets)} configs x {args.repeat} runs")
    print("=" * 80)
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    for idx, (mem_kb, block_sz, bloom_bits) in enumerate(param_sets):
        mem_label = f"{mem_kb}KB" if mem_kb < 1024 else f"{mem_kb // 1024}MB"
        block_label = f"{block_sz // 1024}KB"
        config_name = f"Mem{mem_label}_Block{block_label}_Bloom{bloom_bits}"

        print(f"[{idx + 1}/{len(param_sets)}] {config_name} ...", end=" ", flush=True)

        # 编译
        if not build_with_params(mem_kb, block_sz, bloom_bits):
            print("BUILD FAILED")
            results.append({
                "config": config_name,
                "memtable_kb": mem_kb,
                "block_size": block_sz,
                "bloom_bits": bloom_bits,
                "status": "BUILD_FAILED",
            })
            continue

        # 运行 benchmark（多次取平均）
        all_runs = []
        for run_idx in range(args.repeat):
            result = run_benchmark()
            if result is None:
                print("RUN FAILED")
                all_runs = None
                break
            all_runs.append(result)

        if all_runs is None:
            results.append({
                "config": config_name,
                "memtable_kb": mem_kb,
                "block_size": block_sz,
                "bloom_bits": bloom_bits,
                "status": "RUN_FAILED",
            })
            continue

        # 计算平均值
        avg = {}
        for key in all_runs[0]:
            vals = [r[key] for r in all_runs]
            avg[key] = sum(vals) / len(vals)

        write_ops = int(avg["write_ops_per_sec"])
        total_reads = avg["read_hits"] + avg["read_misses"]
        read_time = avg["read_time_sec"]
        if read_time > 0.0001:
            read_ops = int(total_reads / read_time)
        elif total_reads > 0:
            read_ops = int(total_reads / 0.0001)  # 计时器精度不够，估算
        else:
            read_ops = 0
        errors = int(avg["verify_errors"])

        status = "OK" if errors == 0 else "DATA_ERROR"
        print(f"WRITE={write_ops} ops/s, READ={read_ops} ops/s, SCAN={int(avg['scan_count'])}, {status}")

        results.append({
            "config": config_name,
            "memtable_kb": mem_kb,
            "block_size": block_sz,
            "bloom_bits": bloom_bits,
            "write_ops_per_sec": write_ops,
            "read_ops_per_sec": read_ops,
            "write_time_sec": round(avg["write_time_sec"], 4),
            "read_time_sec": round(avg["read_time_sec"], 4),
            "read_hits": int(avg["read_hits"]),
            "read_misses": int(avg["read_misses"]),
            "scan_count": int(avg["scan_count"]),
            "verify_errors": errors,
            "status": status,
        })

    # 输出 CSV
    csv_path = os.path.join(PROJECT_DIR, args.csv)
    fieldnames = [
        "config", "memtable_kb", "block_size", "bloom_bits",
        "write_ops_per_sec", "read_ops_per_sec",
        "write_time_sec", "read_time_sec",
        "read_hits", "read_misses", "scan_count",
        "verify_errors", "status"
    ]

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            row = {k: r.get(k, "") for k in fieldnames}
            writer.writerow(row)

    # 打印汇总
    print()
    print("=" * 80)
    print(f"  Results saved to: {csv_path}")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # 打印摘要表
    ok_results = [r for r in results if r.get("status") == "OK"]
    if ok_results:
        print()
        print(f"{'Config':<35} {'Write(ops/s)':>14} {'Read(ops/s)':>14}")
        print("-" * 65)
        for r in ok_results:
            print(f"{r['config']:<35} {r['write_ops_per_sec']:>14,} {r['read_ops_per_sec']:>14,}")


if __name__ == "__main__":
    main()