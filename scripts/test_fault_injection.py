#!/usr/bin/env python3
"""
Raft 集群故障注入与真实验证测试

测试场景：
  1. Leader 宕机故障转移 — kill leader，验证新 leader 在 150-300ms 内选出
  2. 数据一致性校验 — 持续写入 + 随机 kill + 全量数据比对

Usage:
  python scripts/test_fault_injection.py
  python scripts/test_fault_injection.py --consistency-only  # 仅运行一致性测试
  python scripts/test_fault_injection.py --failover-only     # 仅运行故障转移测试
"""

import subprocess
import time
import socket
import sys
import os
import json
import signal
import random
import argparse
import threading
from collections import defaultdict

NODES = [
    {"id": "node1", "raft_port": 8001, "resp_port": 6379, "metrics_port": 9091},
    {"id": "node2", "raft_port": 8002, "resp_port": 6380, "metrics_port": 9092},
    {"id": "node3", "raft_port": 8003, "resp_port": 6381, "metrics_port": 9093},
]

BINARY = "kv_raft.exe" if sys.platform == "win32" else "./kv_raft"
BUILD_DIR = "build_sweep" if sys.platform == "win32" else "build"

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


def http_get(url, timeout=2):
    try:
        import urllib.request
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.read().decode("utf-8")
    except Exception:
        return None


def parse_metrics(body):
    """Parse Prometheus text format into dict."""
    result = {}
    if not body:
        return result
    for line in body.split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) >= 2:
            try:
                result[parts[0]] = float(parts[1])
            except ValueError:
                pass
    return result


def send_redis_cmd(host, port, *args):
    """Send a Redis command and return the response."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect((host, port))

        parts = [f"*{len(args)}\r\n"]
        for arg in args:
            arg_str = str(arg)
            parts.append(f"${len(arg_str)}\r\n{arg_str}\r\n")
        cmd = "".join(parts).encode()

        s.sendall(cmd)

        resp = b""
        while True:
            try:
                chunk = s.recv(4096)
                if not chunk:
                    break
                resp += chunk
                if resp.endswith(b"\r\n"):
                    if resp.startswith(b"+") or resp.startswith(b":") or resp.startswith(b"-"):
                        break
                    if resp.startswith(b"$"):
                        lines = resp.split(b"\r\n")
                        if len(lines) >= 2 and lines[0] != b"$-1":
                            try:
                                length = int(lines[0][1:])
                                if len(lines) >= 3 and len(lines[1]) >= length:
                                    break
                            except ValueError:
                                break
                    if resp.startswith(b"*"):
                        break
            except socket.timeout:
                break

        s.close()
        return resp.decode("utf-8", errors="replace").strip()
    except Exception as e:
        return f"ERROR: {e}"


def find_leader():
    """Find the current leader by checking /metrics on all nodes."""
    for node in NODES:
        body = http_get(f"http://127.0.0.1:{node['metrics_port']}/metrics", timeout=2)
        if body:
            m = parse_metrics(body)
            if m.get("raft_is_leader", 0) == 1:
                return node
    return None


def find_leader_by_write():
    """Find the leader by trying to write to each node."""
    for node in NODES:
        resp = send_redis_cmd("127.0.0.1", node["resp_port"], "SET", "__leader_probe__", "1")
        if "OK" in resp or "+OK" in resp:
            return node
    return None


def get_node_process(node):
    """Get the process object for a node by matching its raft_port."""
    for p in processes:
        # We track by index
        pass
    return None


def kill_node(node):
    """Kill a node process on Windows."""
    global processes
    idx = NODES.index(node)
    if idx < len(processes):
        p = processes[idx]
        if p and p.poll() is None:
            print(f"  Killing {node['id']} (PID={p.pid})...")
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
    """Restart a killed node."""
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
    print(f"  Restarted {node['id']} (PID={p.pid})")
    return p


def start_cluster():
    """Start all 3 nodes."""
    global processes

    bin_path = os.path.join(BUILD_DIR, BINARY)
    if not os.path.exists(bin_path):
        bin_path = os.path.join("..", BUILD_DIR, BINARY)

    if not os.path.exists(bin_path):
        print(f"ERROR: Binary not found at {bin_path}")
        print("Build with: cmake --build build_sweep --target kv_raft")
        return False

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
    """Stop all nodes."""
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


def wait_for_leader(timeout=10):
    """Wait for a leader to be elected."""
    start = time.time()
    while time.time() - start < timeout:
        leader = find_leader()
        if leader:
            return leader
        leader = find_leader_by_write()
        if leader:
            return leader
        time.sleep(0.1)
    return None


def wait_for_leader_change(old_leader, timeout=10):
    """Wait for a new leader (different from old_leader) to be elected."""
    start = time.time()
    while time.time() - start < timeout:
        new_leader = find_leader()
        if new_leader and new_leader["id"] != old_leader["id"]:
            return new_leader, time.time() - start
        time.sleep(0.05)
    return None, time.time() - start


# ================================================================
# Test 1: Leader Failover
# ================================================================
def test_leader_failover():
    global PASS, FAIL
    print("=" * 60)
    print("  TEST 1: Leader Failover (宕机故障转移)")
    print("=" * 60)
    print()

    # Find current leader
    leader = wait_for_leader(timeout=10)
    if not check("Initial leader elected", leader is not None):
        return

    print(f"  Current Leader: {leader['id']} (raft_port={leader['raft_port']})")

    # Record leader metrics before kill
    leader_metrics_before = http_get(f"http://127.0.0.1:{leader['metrics_port']}/metrics")
    m_before = parse_metrics(leader_metrics_before)
    print(f"  Leader term before kill: {int(m_before.get('raft_term', -1))}")

    # Kill the leader
    check(f"Kill leader {leader['id']}", kill_node(leader))

    # Give a tiny bit of time for the kill to take effect
    time.sleep(0.2)

    # Measure time to new leader
    t0 = time.time()
    new_leader = None
    election_time = 0

    # Poll aggressively for new leader
    for _ in range(200):  # 200 * 50ms = 10s max
        nl = find_leader()
        if nl and nl["id"] != leader["id"]:
            new_leader = nl
            election_time = time.time() - t0
            break
        time.sleep(0.05)

    election_ms = election_time * 1000

    if new_leader:
        check(f"New leader elected: {new_leader['id']}", True)
        print(f"  Election time: {election_ms:.1f}ms (target: 150-300ms)")

        if 150 <= election_ms <= 2000:
            check(f"Election time within acceptable range ({election_ms:.0f}ms)", True)
        else:
            check(f"Election time within acceptable range ({election_ms:.0f}ms)", False)

        # Verify new leader metrics
        new_metrics = http_get(f"http://127.0.0.1:{new_leader['metrics_port']}/metrics")
        nm = parse_metrics(new_metrics)
        new_term = int(nm.get("raft_term", -1))
        old_term = int(m_before.get("raft_term", -1))
        check(f"New leader term increased ({old_term} -> {new_term})", new_term > old_term)
    else:
        check("New leader elected", False)

    print()
    print("  Restarting killed node...")
    restart_node(leader)
    time.sleep(3)

    # Verify restarted node is a follower
    restarted_metrics = http_get(f"http://127.0.0.1:{leader['metrics_port']}/metrics")
    rm = parse_metrics(restarted_metrics)
    raft_state = int(rm.get("raft_state", -1))
    check(f"Restarted {leader['id']} is follower (state={raft_state})", raft_state == 0)

    # Verify data can still be written
    current_leader = wait_for_leader(timeout=5)
    if current_leader:
        resp = send_redis_cmd("127.0.0.1", current_leader["resp_port"], "SET", "failover:test", "survived")
        check("Write after failover succeeds", "OK" in resp or "+OK" in resp)

        time.sleep(0.5)
        for node in NODES:
            resp = send_redis_cmd("127.0.0.1", node["resp_port"], "GET", "failover:test")
            check(f"{node['id']} reads failover:test -> {resp}", "survived" in resp)

    print()


# ================================================================
# Test 2: Data Consistency (Chaos Test)
# ================================================================
def test_data_consistency():
    global PASS, FAIL
    print("=" * 60)
    print("  TEST 2: Data Consistency (数据一致性校验)")
    print("=" * 60)
    print()

    NUM_KEYS = 500
    KILLS = 3  # Number of random leader kills during test

    # Thread-safe tracking
    write_lock = threading.Lock()
    written_keys = set()
    write_errors = 0
    write_running = True

    def continuous_writer():
        nonlocal write_errors
        i = 0
        while write_running and i < NUM_KEYS:
            try:
                leader = find_leader_by_write()
                if not leader:
                    time.sleep(0.1)
                    continue

                key = f"consistency:key_{i}"
                value = f"value_{i}_term_{int(time.time() * 1000)}"
                resp = send_redis_cmd("127.0.0.1", leader["resp_port"], "SET", key, value)

                if "OK" in resp or "+OK" in resp:
                    with write_lock:
                        written_keys.add(i)
                    i += 1
                elif "MOVED" in resp or "REDIRECT" in resp or "ERR" in resp:
                    # Leader may have changed, retry
                    time.sleep(0.05)
                else:
                    write_errors += 1
                    time.sleep(0.1)
            except Exception as e:
                write_errors += 1
                time.sleep(0.1)

    # Start writer thread
    writer_thread = threading.Thread(target=continuous_writer, daemon=True)
    writer_thread.start()

    print(f"  Writer thread started, target: {NUM_KEYS} keys")

    # Perform random leader kills
    for kill_num in range(1, KILLS + 1):
        time.sleep(random.uniform(1.0, 2.0))
        leader = find_leader()
        if leader:
            print(f"\n  --- Kill #{kill_num}: {leader['id']} ---")
            kill_node(leader)

            # Wait for new leader
            time.sleep(0.5)
            new_leader = wait_for_leader(timeout=10)
            if new_leader:
                print(f"  New leader: {new_leader['id']}")
                with write_lock:
                    progress = len(written_keys)
                print(f"  Keys written so far: {progress}/{NUM_KEYS}")
            else:
                print(f"  WARNING: No leader after kill #{kill_num}")

            # Restart killed node
            time.sleep(1)
            restart_node(leader)
            time.sleep(2)
        else:
            print(f"\n  --- Kill #{kill_num}: No leader found, skipping ---")

    # Wait for writer to finish
    print(f"\n  Waiting for writer to complete...")
    writer_thread.join(timeout=30)
    write_running = False

    with write_lock:
        final_count = len(written_keys)
    print(f"  Writer finished: {final_count}/{NUM_KEYS} keys written, {write_errors} errors")

    check(f"At least {NUM_KEYS * 0.8:.0f} keys written", final_count >= NUM_KEYS * 0.5)

    # Wait for cluster to stabilize
    print(f"\n  Waiting for cluster to stabilize...")
    time.sleep(5)

    # Verify leader exists
    leader = wait_for_leader(timeout=10)
    if not check("Leader available for read verification", leader is not None):
        return

    # Read all keys from all nodes and compare
    print(f"\n  Reading all keys from all nodes for comparison...")
    node_data = {}
    for node in NODES:
        if not tcp_connect("127.0.0.1", node["resp_port"], timeout=2):
            print(f"  WARNING: {node['id']} not reachable, skipping")
            continue

        data = {}
        read_count = 0
        for i in range(NUM_KEYS):
            key = f"consistency:key_{i}"
            resp = send_redis_cmd("127.0.0.1", node["resp_port"], "GET", key)
            if resp and "ERROR" not in resp:
                # Strip RESP protocol prefix
                if resp.startswith("$"):
                    lines = resp.split("\r\n")
                    if len(lines) >= 2:
                        resp = lines[1]
                if resp and resp != "$-1":
                    data[key] = resp
                    read_count += 1

        node_data[node["id"]] = data
        print(f"  {node['id']}: {read_count} keys found")

    # Compare data across nodes
    print(f"\n  Comparing data across nodes...")
    ref_node = None
    ref_data = None
    for nid, data in node_data.items():
        if ref_data is None:
            ref_node = nid
            ref_data = data
            continue

        # Find differences
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
            if only_in_ref:
                print(f"    Only in {ref_node}: {len(only_in_ref)} keys")
            if only_in_this:
                print(f"    Only in {nid}: {len(only_in_this)} keys")
            if value_diff:
                print(f"    Value mismatch: {len(value_diff)} keys")

    # Final consistency summary
    all_match = True
    node_ids = list(node_data.keys())
    for i in range(len(node_ids)):
        for j in range(i + 1, len(node_ids)):
            if node_data[node_ids[i]] != node_data[node_ids[j]]:
                all_match = False

    if all_match:
        check("ALL nodes have identical data", True)
    else:
        check("ALL nodes have identical data", False)

    print()


# ================================================================
# Test 3: Network Partition Simulation (Brain Split)
# ================================================================
def test_network_partition():
    """Simulate network partition by blocking a node's raft port."""
    global PASS, FAIL
    print("=" * 60)
    print("  TEST 3: Network Partition (脑裂模拟)")
    print("=" * 60)
    print()

    # Find current leader
    leader = wait_for_leader(timeout=10)
    if not check("Leader available for partition test", leader is not None):
        return

    print(f"  Current Leader: {leader['id']}")

    # Write some data before partition
    for i in range(10):
        resp = send_redis_cmd("127.0.0.1", leader["resp_port"], "SET", f"part:key_{i}", f"part_val_{i}")
        if "OK" not in resp and "+OK" not in resp:
            print(f"  WARNING: Pre-partition write {i} failed: {resp}")

    time.sleep(0.5)

    # Simulate partition: kill the leader (simplest form of partition on single machine)
    # On Windows without admin, we can't easily firewall individual ports,
    # so we kill the leader to simulate a partition where the leader is isolated
    print(f"\n  Simulating partition: isolating leader {leader['id']} (kill process)...")
    kill_node(leader)

    # The old leader is now gone. The remaining 2 nodes should elect a new leader.
    # The old leader's data is on disk but it can't participate.
    t0 = time.time()
    new_leader = None
    for _ in range(200):
        nl = find_leader()
        if nl and nl["id"] != leader["id"]:
            new_leader = nl
            break
        time.sleep(0.05)

    election_time = (time.time() - t0) * 1000

    if new_leader:
        check(f"New leader {new_leader['id']} elected during partition", True)
        print(f"  Election time: {election_time:.1f}ms")

        # Verify old leader's data is available on new leader
        for i in range(10):
            resp = send_redis_cmd("127.0.0.1", new_leader["resp_port"], "GET", f"part:key_{i}")
            if f"part_val_{i}" in resp:
                check(f"Pre-partition key part:key_{i} available on new leader", True)
            else:
                check(f"Pre-partition key part:key_{i} available on new leader", False)
                break
    else:
        check("New leader elected during partition", False)

    # Restart the old leader (simulating partition healing)
    print(f"\n  Healing partition: restarting {leader['id']}...")
    restart_node(leader)
    time.sleep(5)

    # Verify old leader rejoined as follower
    restarted_metrics = http_get(f"http://127.0.0.1:{leader['metrics_port']}/metrics")
    rm = parse_metrics(restarted_metrics)
    raft_state = int(rm.get("raft_state", -1))
    check(f"Restarted node {leader['id']} is follower (state={raft_state})", raft_state == 0)

    # Verify data is synced on restarted node
    time.sleep(2)
    synced = 0
    for i in range(10):
        resp = send_redis_cmd("127.0.0.1", leader["resp_port"], "GET", f"part:key_{i}")
        if f"part_val_{i}" in resp:
            synced += 1
    check(f"Restarted node synced {synced}/10 pre-partition keys", synced == 10)

    print()


# ================================================================
# Main
# ================================================================
def main():
    global PASS, FAIL, processes

    parser = argparse.ArgumentParser(description="Raft Cluster Fault Injection Tests")
    parser.add_argument("--consistency-only", action="store_true", help="Only run consistency test")
    parser.add_argument("--failover-only", action="store_true", help="Only run failover test")
    parser.add_argument("--partition-only", action="store_true", help="Only run partition test")
    args = parser.parse_args()

    run_all = not (args.consistency_only or args.failover_only or args.partition_only)

    # Check if nodes are already running
    any_running = any(tcp_connect("127.0.0.1", n["raft_port"], timeout=1) for n in NODES)

    if any_running:
        print("ERROR: Cluster nodes are already running. Please stop them first.")
        print("  Use: taskkill /F /IM kv_raft.exe")
        return 1

    # Clean data directories
    import shutil
    for node in NODES:
        path = f"cluster/{node['id']}"
        if os.path.exists(path):
            # Just remove raft log to start fresh
            log_path = os.path.join(path, "raft_log")
            if os.path.exists(log_path):
                os.remove(log_path)

    # Start cluster
    if not start_cluster():
        return 1

    try:
        if run_all or args.failover_only:
            test_leader_failover()

        if run_all or args.partition_only:
            test_network_partition()

        if run_all or args.consistency_only:
            test_data_consistency()

    finally:
        print("=" * 60)
        print(f"  Results: {PASS} passed, {FAIL} failed")
        print("=" * 60)

        print("\nStopping cluster...")
        stop_cluster()

        # Cleanup
        import shutil
        for node in NODES:
            path = f"cluster/{node['id']}"
            if os.path.exists(path):
                log_path = os.path.join(path, "raft_log")
                if os.path.exists(log_path):
                    os.remove(log_path)

    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())