#!/usr/bin/env python3
"""
KV Raft 集群 — 10 万条持续写入 + 混沌测试

测试流程：
  1. 启动 3 节点 Raft 集群
  2. 持续写入 100,000 条递增序列号数据
  3. 写入过程中随机 kill 节点并重启（模拟故障）
  4. 写入完成后，分别从 3 个节点读取全量数据
  5. 比对 3 节点数据是否完全一致

Usage:
  python scripts/test_chaos_100k.py
  python scripts/test_chaos_100k.py --num-keys 50000 --kill-count 5
  python scripts/test_chaos_100k.py --no-kill  # 仅写入，不注入故障
"""

import subprocess
import time
import socket
import sys
import os
import signal
import random
import argparse
import threading
from collections import defaultdict
from kv_client import HAKVClient

# ================================================================
# 配置
# ================================================================
NODES = [
    {"id": "node1", "raft_port": 8001, "resp_port": 6379, "metrics_port": 9091},
    {"id": "node2", "raft_port": 8002, "resp_port": 6380, "metrics_port": 9092},
    {"id": "node3", "raft_port": 8003, "resp_port": 6381, "metrics_port": 9093},
]

BINARY = "kv_raft.exe" if sys.platform == "win32" else "./kv_raft"
BUILD_DIR = "build_sweep" if sys.platform == "win32" else "build"

CLUSTER_NODES = [
    ("127.0.0.1", n["resp_port"], n["metrics_port"]) for n in NODES
]

PASS = 0
FAIL = 0
processes = []


def check(msg, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {msg}")
    else:
        FAIL += 1
        print(f"  [FAIL] {msg}")
    return cond


def tcp_connect(host, port, timeout=2):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, port))
        s.close()
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


# ================================================================
# 集群管理
# ================================================================
def start_cluster():
    global processes

    bin_path = os.path.join(BUILD_DIR, BINARY)
    if not os.path.exists(bin_path):
        bin_path = os.path.join("..", BUILD_DIR, BINARY)

    if not os.path.exists(bin_path):
        print(f"ERROR: Binary not found at {bin_path}")
        print("Build with: cmake --build build_sweep --target kv_raft")
        return False

    # Clean data directories
    import shutil
    for node in NODES:
        path = f"cluster/{node['id']}"
        if os.path.exists(path):
            for f in os.listdir(path):
                fp = os.path.join(path, f)
                if os.path.isfile(fp):
                    os.remove(fp)

    print("Starting 3-node cluster...")
    for node in NODES:
        cmd = [
            bin_path,
            "--id", node["id"],
            "--raft-port", str(node["raft_port"]),
            "--resp-port", str(node["resp_port"]),
            "--metrics-port", str(node["metrics_port"]),
            "--peer", f"{NODES[0]['id']}:127.0.0.1:{NODES[0]['raft_port']}",
            "--peer", f"{NODES[1]['id']}:127.0.0.1:{NODES[1]['raft_port']}",
            "--peer", f"{NODES[2]['id']}:127.0.0.1:{NODES[2]['raft_port']}",
            "--data-dir", f"cluster/{node['id']}",
        ]
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        processes.append(p)
        print(f"  Started {node['id']} (PID={p.pid})")

    print("Waiting for cluster to stabilize (5 seconds)...")
    time.sleep(5)
    return True


def stop_cluster():
    global processes
    for p in processes:
        if p and p.poll() is None:
            if sys.platform == "win32":
                p.terminate()
            else:
                p.send_signal(signal.SIGTERM)
    for p in processes:
        if p and p.poll() is None:
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
    processes = []


def kill_node(node):
    global processes
    idx = NODES.index(node)
    if idx < len(processes):
        p = processes[idx]
        if p and p.poll() is None:
            print(f"\n  [CHAOS] Killing {node['id']} (PID={p.pid})...")
            if sys.platform == "win32":
                p.terminate()
                try:
                    p.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    p.kill()
                    p.wait()
            else:
                p.send_signal(signal.SIGKILL)
                p.wait()
            processes[idx] = None
            return True
    return False


def restart_node(node):
    global processes
    idx = NODES.index(node)

    bin_path = os.path.join(BUILD_DIR, BINARY)
    if not os.path.exists(bin_path):
        bin_path = os.path.join("..", BUILD_DIR, BINARY)

    cmd = [
        bin_path,
        "--id", node["id"],
        "--raft-port", str(node["raft_port"]),
        "--resp-port", str(node["resp_port"]),
        "--metrics-port", str(node["metrics_port"]),
        "--peer", f"{NODES[0]['id']}:127.0.0.1:{NODES[0]['raft_port']}",
        "--peer", f"{NODES[1]['id']}:127.0.0.1:{NODES[1]['raft_port']}",
        "--peer", f"{NODES[2]['id']}:127.0.0.1:{NODES[2]['raft_port']}",
        "--data-dir", f"cluster/{node['id']}",
    ]

    p = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    processes[idx] = p
    print(f"  [CHAOS] Restarted {node['id']} (PID={p.pid})")
    return p


# ================================================================
# 混沌写入 Worker
# ================================================================
class ChaosWriter:
    """持续写入 + 故障注入的混沌测试写入器"""

    def __init__(self, client, num_keys, chaos_callback=None):
        self.client = client
        self.num_keys = num_keys
        self.chaos_callback = chaos_callback

        self.lock = threading.Lock()
        self.written_keys = set()
        self.write_errors = 0
        self.write_retries = 0
        self.running = True

        self.start_time = None
        self.end_time = None

    def write(self):
        """持续写入所有 key"""
        self.start_time = time.time()
        i = 0
        consecutive_errors = 0

        while self.running and i < self.num_keys:
            try:
                key = f"chaos:key_{i:06d}"
                value = f"value_{i}_ts_{int(time.time() * 1000)}"
                resp = self.client.set(key, value)

                if isinstance(resp, dict) and "error" in resp:
                    self.write_errors += 1
                    consecutive_errors += 1
                    if consecutive_errors > 10:
                        print(f"\n  [WARN] {consecutive_errors} consecutive write errors, pausing...")
                        time.sleep(1)
                        consecutive_errors = 0
                    else:
                        time.sleep(0.05)
                    continue

                consecutive_errors = 0
                with self.lock:
                    self.written_keys.add(i)
                i += 1

                # 进度报告
                if i % 5000 == 0:
                    elapsed = time.time() - self.start_time
                    rate = i / elapsed if elapsed > 0 else 0
                    with self.lock:
                        written = len(self.written_keys)
                    print(f"  [PROGRESS] {written}/{self.num_keys} keys "
                          f"({rate:.0f} ops/s, {self.write_errors} errors)")

            except Exception as e:
                self.write_errors += 1
                consecutive_errors += 1
                time.sleep(0.1)

        self.end_time = time.time()
        self.running = False

    def get_stats(self):
        with self.lock:
            written = len(self.written_keys)
        elapsed = self.end_time - self.start_time if self.end_time else time.time() - self.start_time
        return {
            "written": written,
            "target": self.num_keys,
            "errors": self.write_errors,
            "retries": self.client.retry_count,
            "leader_switches": self.client.leader_switches,
            "elapsed_sec": elapsed,
            "ops_per_sec": written / elapsed if elapsed > 0 else 0,
        }


# ================================================================
# 混沌故障注入器
# ================================================================
class ChaosInjector:
    """在写入过程中随机注入故障"""

    def __init__(self, kill_count, min_interval=3.0, max_interval=8.0):
        self.kill_count = kill_count
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.kills_done = 0
        self.running = True

    def run(self, writer):
        """在后台线程中运行故障注入"""
        for k in range(self.kill_count):
            if not writer.running:
                break

            # 等待随机间隔
            wait = random.uniform(self.min_interval, self.max_interval)
            time.sleep(wait)

            if not writer.running:
                break

            # 随机选择一个节点
            target = random.choice(NODES)
            self.kills_done += 1

            with writer.lock:
                progress = len(writer.written_keys)
            print(f"\n  [CHAOS] Kill #{self.kills_done}/{self.kill_count}: "
                  f"{target['id']} (progress: {progress}/{writer.num_keys})")

            kill_node(target)

            # 等待新 Leader 选出
            time.sleep(0.5)

            # 等待一段时间后重启
            restart_delay = random.uniform(2.0, 5.0)
            time.sleep(restart_delay)

            if writer.running:
                restart_node(target)
                time.sleep(2)  # 等待节点追上日志

        self.running = False


# ================================================================
# 数据一致性验证
# ================================================================
def verify_data_consistency(client, num_keys):
    global PASS, FAIL
    print("\n" + "=" * 60)
    print("  Data Consistency Verification")
    print("=" * 60)
    print()

    # 等待集群稳定
    print("  Waiting for cluster to stabilize (5 seconds)...")
    time.sleep(5)

    # 从每个节点读取数据
    node_data = {}
    for node in NODES:
        if not tcp_connect("127.0.0.1", node["resp_port"], timeout=2):
            print(f"  WARNING: {node['id']} not reachable, skipping")
            continue

        print(f"  Reading from {node['id']} (port {node['resp_port']})...")
        node_client = HAKVClient([("127.0.0.1", node["resp_port"], node["metrics_port"])],
                                 timeout=5.0)

        data = {}
        read_count = 0
        read_errors = 0
        for i in range(num_keys):
            key = f"chaos:key_{i:06d}"
            resp = node_client.get(key)
            if isinstance(resp, dict) and "error" in resp:
                read_errors += 1
                continue
            if resp is not None:
                data[i] = resp
                read_count += 1

        node_data[node["id"]] = data
        node_client.close()
        print(f"    {node['id']}: {read_count} keys found, {read_errors} errors")

    # 比对数据
    print(f"\n  Comparing data across nodes...")
    ref_node = None
    ref_data = None
    all_match = True

    for nid, data in node_data.items():
        if ref_data is None:
            ref_node = nid
            ref_data = data
            continue

        only_in_ref = set(ref_data.keys()) - set(data.keys())
        only_in_this = set(data.keys()) - set(ref_data.keys())
        value_diff = set()
        for k in set(ref_data.keys()) & set(data.keys()):
            if ref_data[k] != data[k]:
                value_diff.add(k)

        total_diff = len(only_in_ref) + len(only_in_this) + len(value_diff)
        if total_diff == 0:
            check(f"Data consistency: {ref_node} vs {nid}", True)
        else:
            check(f"Data consistency: {ref_node} vs {nid} ({total_diff} diffs)", False)
            all_match = False
            if only_in_ref:
                print(f"      Only in {ref_node}: {len(only_in_ref)} keys")
                if len(only_in_ref) <= 5:
                    for k in sorted(only_in_ref)[:5]:
                        print(f"        key_{k:06d}")
            if only_in_this:
                print(f"      Only in {nid}: {len(only_in_this)} keys")
                if len(only_in_this) <= 5:
                    for k in sorted(only_in_this)[:5]:
                        print(f"        key_{k:06d}")
            if value_diff:
                print(f"      Value mismatch: {len(value_diff)} keys")
                if len(value_diff) <= 5:
                    for k in sorted(value_diff)[:5]:
                        print(f"        key_{k:06d}: ref={ref_data[k][:50]}... this={data[k][:50]}...")

    # 最终判定
    if all_match:
        check("ALL NODES HAVE IDENTICAL DATA", True)
    else:
        check("ALL NODES HAVE IDENTICAL DATA", False)

    # 覆盖率
    total_keys = sum(len(d) for d in node_data.values())
    expected = num_keys * len(node_data)
    if expected > 0:
        coverage = total_keys / expected * 100
        print(f"\n  Data coverage: {total_keys}/{expected} ({coverage:.1f}%)")
        check(f"Data coverage >= 95%", coverage >= 95.0)

    print()
    return node_data


# ================================================================
# Main
# ================================================================
def main():
    global PASS, FAIL, processes

    parser = argparse.ArgumentParser(
        description="KV Raft Cluster — 100K Chaos Test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_chaos_100k.py                          # Full test: 100k keys + 3 kills
  python test_chaos_100k.py --num-keys 50000          # 50k keys
  python test_chaos_100k.py --kill-count 5             # 5 random kills
  python test_chaos_100k.py --no-kill                  # Write only, no chaos
  python test_chaos_100k.py --consistency-only         # Only verify existing data
        """
    )
    parser.add_argument("--num-keys", type=int, default=100000,
                        help="Number of keys to write (default: 100000)")
    parser.add_argument("--kill-count", type=int, default=3,
                        help="Number of random node kills during write (default: 3)")
    parser.add_argument("--no-kill", action="store_true",
                        help="Disable chaos injection (write only)")
    parser.add_argument("--consistency-only", action="store_true",
                        help="Only verify consistency (skip write phase)")
    args = parser.parse_args()

    NUM_KEYS = args.num_keys
    KILL_COUNT = 0 if args.no_kill else args.kill_count

    print("=" * 60)
    print("  KV Raft Cluster — Chaos Test")
    print("=" * 60)
    print(f"  Keys:        {NUM_KEYS:,}")
    print(f"  Kill count:  {KILL_COUNT}")
    print(f"  Consistency: {'only' if args.consistency_only else 'full'}")
    print()

    # Check if nodes are already running
    any_running = any(tcp_connect("127.0.0.1", n["raft_port"], timeout=1) for n in NODES)
    if any_running:
        print("ERROR: Cluster nodes are already running. Please stop them first.")
        print("  Use: taskkill /F /IM kv_raft.exe")
        return 1

    if not args.consistency_only:
        # Phase 1: Start cluster
        if not start_cluster():
            return 1

        try:
            # Phase 2: Create HA client
            print("\n" + "=" * 60)
            print("  Phase 1: Continuous Write + Chaos")
            print("=" * 60)
            print()

            client = HAKVClient(CLUSTER_NODES, timeout=10.0, leader_cache_ttl=3.0)

            # Wait for leader
            print("  Discovering leader...")
            leader = client._find_leader(force=True)
            if leader:
                print(f"  Leader: {leader[0]}:{leader[1]} (metrics={leader[2]})")
            else:
                print("  ERROR: No leader found!")
                stop_cluster()
                return 1

            # Phase 3: Start chaos writer
            writer = ChaosWriter(client, NUM_KEYS)

            # Start chaos injector in background thread
            injector = ChaosInjector(KILL_COUNT, min_interval=3.0, max_interval=10.0)
            if KILL_COUNT > 0:
                chaos_thread = threading.Thread(
                    target=injector.run, args=(writer,), daemon=True
                )
                chaos_thread.start()

            # Phase 4: Write all keys
            print(f"\n  Starting continuous write of {NUM_KEYS:,} keys...")
            print(f"  Chaos injection: {KILL_COUNT} random kills")
            print()
            writer.write()

            # Wait for chaos injector to finish
            if KILL_COUNT > 0:
                chaos_thread.join(timeout=10)

            # Print stats
            stats = writer.get_stats()
            print(f"\n  Write phase complete:")
            print(f"    Written:     {stats['written']:,}/{stats['target']:,} keys")
            print(f"    Errors:      {stats['errors']}")
            print(f"    Retries:     {stats['retries']}")
            print(f"    Leader chg:  {stats['leader_switches']}")
            print(f"    Time:        {stats['elapsed_sec']:.1f}s")
            print(f"    Throughput:  {stats['ops_per_sec']:.0f} ops/s")

            check(f"At least 90% keys written ({stats['written']}/{stats['target']})",
                  stats['written'] >= int(NUM_KEYS * 0.9))

            client.close()

            # Phase 5: Verify consistency
            # Create a fresh client for verification
            verify_client = HAKVClient(CLUSTER_NODES, timeout=10.0)
            verify_data_consistency(verify_client, NUM_KEYS)
            verify_client.close()

        finally:
            print("\n" + "=" * 60)
            print(f"  Results: {PASS} passed, {FAIL} failed")
            print("=" * 60)

            print("\n  Stopping cluster...")
            stop_cluster()

            # Cleanup data
            import shutil
            for node in NODES:
                path = f"cluster/{node['id']}"
                if os.path.exists(path):
                    for f in os.listdir(path):
                        fp = os.path.join(path, f)
                        if os.path.isfile(fp):
                            os.remove(fp)
    else:
        # Consistency-only mode
        verify_client = HAKVClient(CLUSTER_NODES, timeout=10.0)
        verify_data_consistency(verify_client, NUM_KEYS)
        verify_client.close()

    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())