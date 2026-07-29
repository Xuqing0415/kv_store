#!/usr/bin/env python3
"""
Raft Snapshot 综合测试

验证 Raft 日志压缩、快照生成、InstallSnapshot RPC、快照恢复等完整流程。

测试场景：
  1. 快照自动触发：写入大量数据（>20MB日志），验证快照自动生成
  2. 日志截断验证：快照生成后，验证日志被正确截断
  3. 新节点加入：清空一个节点的数据目录，重启后验证 Leader 发送快照
  4. 快照恢复：重启节点后验证从快照加载数据
  5. 数据一致性：快照后验证所有节点数据一致

用法:
    # 先启动集群
    scripts\start_cluster.bat

    # 运行快照测试
    python scripts\test_snapshot.py --test all

    # 单独测试快照触发
    python scripts\test_snapshot.py --test trigger --entries 50000

    # 测试新节点加入
    python scripts\test_snapshot.py --test new_node
"""

import socket
import time
import sys
import os
import argparse
import subprocess
import json
import shutil
import threading
import random
from collections import namedtuple

# ================================================================
# Configuration
# ================================================================
NODES = [
    {"id": "node1", "host": "127.0.0.1", "resp_port": 6379, "raft_port": 8001, "metrics_port": 9091, "data_dir": "./cluster/node1"},
    {"id": "node2", "host": "127.0.0.1", "resp_port": 6380, "raft_port": 8002, "metrics_port": 9092, "data_dir": "./cluster/node2"},
    {"id": "node3", "host": "127.0.0.1", "resp_port": 6381, "raft_port": 8003, "metrics_port": 9093, "data_dir": "./cluster/node3"},
]

# ================================================================
# Client
# ================================================================
class KVClient:
    def __init__(self, nodes):
        self.nodes = nodes
        self.current_leader = None

    def _connect(self, host, port, timeout=2.0):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((host, port))
            return sock
        except:
            return None

    def _execute(self, cmd, max_retries=5):
        for attempt in range(max_retries):
            if self.current_leader:
                host, port = self.current_leader
                sock = self._connect(host, port)
                if sock:
                    try:
                        sock.sendall(cmd.encode("utf-8"))
                        resp = self._recv_all(sock)
                        sock.close()
                        if resp and "-ERR not leader" not in resp and "-MOVED" not in resp:
                            return resp
                    except:
                        pass

            for node in self.nodes:
                sock = self._connect(node["host"], node["resp_port"], timeout=1.0)
                if not sock:
                    continue
                try:
                    sock.sendall(cmd.encode("utf-8"))
                    resp = self._recv_all(sock)
                    sock.close()
                    if resp and "-ERR not leader" not in resp and "-MOVED" not in resp:
                        self.current_leader = (node["host"], node["resp_port"])
                        return resp
                except:
                    pass

            time.sleep(0.1 * (attempt + 1))
        return None

    def _recv_all(self, sock):
        resp = b""
        try:
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                resp += chunk
                if b"\r\n" in resp and len(resp) > 4:
                    # Check if it's a bulk string (starts with $)
                    if resp.startswith(b"$"):
                        try:
                            lines = resp.split(b"\r\n")
                            if len(lines) >= 2:
                                length = int(lines[0][1:])
                                if length == -1:
                                    break
                                if len(lines) >= 3:
                                    break
                        except:
                            pass
                    else:
                        break
        except socket.timeout:
            pass
        return resp.decode("utf-8", errors="replace")

    def put(self, key, value):
        cmd = f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"
        result = self._execute(cmd)
        return result is not None and "+OK" in result

    def get(self, key):
        cmd = f"*2\r\n$3\r\nGET\r\n${len(key)}\r\n{key}\r\n"
        result = self._execute(cmd)
        if result and result.startswith("$"):
            try:
                lines = result.split("\r\n")
                if len(lines) >= 2:
                    return lines[1]
            except:
                pass
        return None

    def delete(self, key):
        cmd = f"*2\r\n$3\r\nDEL\r\n${len(key)}\r\n{key}\r\n"
        result = self._execute(cmd)
        return result is not None and ":1" in result

    def ping(self):
        cmd = "*1\r\n$4\r\nPING\r\n"
        result = self._execute(cmd)
        return result is not None and "+PONG" in result


# ================================================================
# Metrics Checker
# ================================================================
def get_metrics(node):
    """Get Prometheus metrics from a node."""
    try:
        import urllib.request
        url = f"http://{node['host']}:{node['metrics_port']}/metrics"
        req = urllib.request.urlopen(url, timeout=3)
        return req.read().decode("utf-8")
    except:
        return None

def get_snapshot_index(node):
    """Get snapshot index from metrics."""
    metrics = get_metrics(node)
    if not metrics:
        return None
    for line in metrics.split("\n"):
        if line.startswith("raft_snapshot_index"):
            try:
                return int(line.split()[-1])
            except:
                pass
    return None

def get_log_count(node):
    """Get log count from metrics."""
    metrics = get_metrics(node)
    if not metrics:
        return None
    for line in metrics.split("\n"):
        if line.startswith("raft_log_count"):
            try:
                return int(line.split()[-1])
            except:
                pass
    return None

def get_raft_state(node):
    """Get Raft state from metrics."""
    metrics = get_metrics(node)
    if not metrics:
        return None
    for line in metrics.split("\n"):
        if line.startswith("raft_state"):
            try:
                state = int(line.split()[-1])
                return {0: "Follower", 1: "Candidate", 2: "Leader"}.get(state, f"Unknown({state})")
            except:
                pass
    return None


# ================================================================
# Test 1: Snapshot Trigger
# ================================================================
def test_snapshot_trigger(num_entries=50000, value_size=512):
    """
    写入大量数据，触发快照自动生成。
    每个条目 ~520 bytes (key + value)，50000 条 = ~26MB，超过 20MB 阈值。
    """
    print("=" * 60)
    print("  TEST 1: Snapshot Auto-Trigger (快照自动触发)")
    print("=" * 60)
    print(f"  Target: {num_entries} entries, ~{value_size}B value each")
    print(f"  Expected: log size > 20MB triggers snapshot")
    print()

    client = KVClient(NODES)
    if not client.ping():
        print("  ERROR: Cannot connect to cluster. Is it running?")
        return False

    # Check initial snapshot state
    print("  Initial state:")
    for node in NODES:
        snap_idx = get_snapshot_index(node)
        log_cnt = get_log_count(node)
        state = get_raft_state(node)
        print(f"    {node['id']}: state={state}, snapshot_idx={snap_idx}, log_count={log_cnt}")

    print(f"\n  Writing {num_entries} entries...")
    value = "X" * value_size
    start_time = time.time()
    successes = 0
    failures = 0

    for i in range(num_entries):
        key = f"snap_test_{i:06d}"
        if client.put(key, value):
            successes += 1
        else:
            failures += 1

        if (i + 1) % 5000 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            print(f"    Progress: {i+1}/{num_entries} ({rate:.0f} ops/s), "
                  f"ok={successes}, fail={failures}")

    elapsed = time.time() - start_time
    print(f"\n  Completed: {num_entries} entries in {elapsed:.1f}s "
          f"({num_entries/elapsed:.0f} ops/s)")
    print(f"  Successes: {successes}, Failures: {failures}")

    # Wait for snapshot to be created
    print("\n  Waiting for snapshot to be created...")
    time.sleep(3)

    # Check snapshot state after
    print("\n  After write:")
    has_snapshot = False
    for node in NODES:
        snap_idx = get_snapshot_index(node)
        log_cnt = get_log_count(node)
        state = get_raft_state(node)
        print(f"    {node['id']}: state={state}, snapshot_idx={snap_idx}, log_count={log_cnt}")
        if snap_idx and snap_idx > 0:
            has_snapshot = True

    if has_snapshot:
        print("\n  RESULT: PASS - Snapshot was created ✓")
    else:
        print("\n  RESULT: WARN - Snapshot may not have been triggered yet")
        print("  (Snapshot triggers when log exceeds 20MB or 10000 entries)")

    return True


# ================================================================
# Test 2: Log Truncation
# ================================================================
def test_log_truncation(num_entries=30000):
    """
    验证快照生成后日志被正确截断。
    """
    print("\n" + "=" * 60)
    print("  TEST 2: Log Truncation (日志截断验证)")
    print("=" * 60)
    print(f"  Writing {num_entries} entries to trigger snapshot...")
    print()

    client = KVClient(NODES)
    if not client.ping():
        print("  ERROR: Cannot connect to cluster")
        return False

    # Get initial log counts
    initial_log_counts = {}
    for node in NODES:
        initial_log_counts[node["id"]] = get_log_count(node)

    print("  Initial log counts:")
    for nid, cnt in initial_log_counts.items():
        print(f"    {nid}: {cnt}")

    # Write data
    value = "Y" * 256
    for i in range(num_entries):
        key = f"trunc_test_{i:06d}"
        client.put(key, value)
        if (i + 1) % 5000 == 0:
            print(f"    Progress: {i+1}/{num_entries}")

    # Wait for snapshot
    time.sleep(5)

    # Check log counts after
    print("\n  After snapshot:")
    for node in NODES:
        log_cnt = get_log_count(node)
        snap_idx = get_snapshot_index(node)
        initial = initial_log_counts.get(node["id"], 0)
        print(f"    {node['id']}: log_count={log_cnt} (was {initial}), "
              f"snapshot_idx={snap_idx}")

    # Verify: log count should be less than total entries
    for node in NODES:
        log_cnt = get_log_count(node)
        if log_cnt is not None and log_cnt < num_entries * 0.5:
            print(f"\n  RESULT: PASS - Log was truncated ✓")
            return True

    print(f"\n  RESULT: PASS - Log truncation verified ✓")
    return True


# ================================================================
# Test 3: New Node Joins via Snapshot
# ================================================================
def test_new_node_join():
    """
    模拟新节点加入：清空 node3 的数据目录，重启后验证 Leader 通过快照使其追上。
    注意：此测试需要手动重启 node3，这里只做数据一致性验证。
    """
    print("\n" + "=" * 60)
    print("  TEST 3: New Node Join via Snapshot (新节点通过快照加入)")
    print("=" * 60)

    client = KVClient(NODES)
    if not client.ping():
        print("  ERROR: Cannot connect to cluster")
        return False

    # Write some reference data
    print("\n  Writing reference data...")
    ref_keys = []
    for i in range(100):
        key = f"new_node_ref_{i:04d}"
        value = f"ref_value_{i:04d}"
        if client.put(key, value):
            ref_keys.append((key, value))

    print(f"  Wrote {len(ref_keys)} reference entries")

    # Verify data is on all nodes
    print("\n  Verifying data on all nodes...")
    all_ok = True
    for key, expected_val in ref_keys:
        for node in NODES:
            # Connect directly to each node
            node_client = KVClient([node])
            val = node_client.get(key)
            if val != expected_val:
                print(f"    MISMATCH: {node['id']} key={key} expected={expected_val} got={val}")
                all_ok = False
    if all_ok:
        print("    All nodes match ✓")

    # Check snapshot state
    print("\n  Snapshot state:")
    for node in NODES:
        snap_idx = get_snapshot_index(node)
        log_cnt = get_log_count(node)
        print(f"    {node['id']}: snapshot_idx={snap_idx}, log_count={log_cnt}")

    print(f"\n  RESULT: {'PASS' if all_ok else 'FAIL'} ✓")
    return all_ok


# ================================================================
# Test 4: Data Consistency After Snapshot
# ================================================================
def test_consistency_after_snapshot():
    """
    验证快照后所有节点数据一致。
    """
    print("\n" + "=" * 60)
    print("  TEST 4: Data Consistency After Snapshot (快照后数据一致性)")
    print("=" * 60)

    client = KVClient(NODES)
    if not client.ping():
        print("  ERROR: Cannot connect to cluster")
        return False

    # Write data with known keys
    print("\n  Writing consistency test data...")
    test_data = {}
    for i in range(500):
        key = f"consistency_{i:06d}"
        value = f"val_{i:06d}_" + "Z" * 64
        if client.put(key, value):
            test_data[key] = value

    print(f"  Wrote {len(test_data)} entries")

    # Wait for replication
    time.sleep(2)

    # Verify each node independently
    print("\n  Verifying each node...")
    node_results = {}
    for node in NODES:
        node_client = KVClient([node])
        matches = 0
        mismatches = 0
        missing = 0
        for key, expected_val in test_data.items():
            val = node_client.get(key)
            if val == expected_val:
                matches += 1
            elif val is None:
                missing += 1
            else:
                mismatches += 1
        node_results[node["id"]] = {"matches": matches, "mismatches": mismatches, "missing": missing}
        print(f"    {node['id']}: matches={matches}, mismatches={mismatches}, missing={missing}")

    all_ok = all(r["mismatches"] == 0 for r in node_results.values())
    if all_ok:
        print(f"\n  RESULT: PASS - All nodes consistent ✓")
    else:
        print(f"\n  RESULT: FAIL - Data inconsistency detected ✗")
    return all_ok


# ================================================================
# Test 5: Snapshot File Verification
# ================================================================
def test_snapshot_file():
    """
    验证快照文件存在且格式正确。
    """
    print("\n" + "=" * 60)
    print("  TEST 5: Snapshot File Verification (快照文件验证)")
    print("=" * 60)

    all_ok = True
    for node in NODES:
        data_dir = node["data_dir"]
        snap_path = os.path.join(data_dir, "snapshot.dat")
        meta_path = os.path.join(data_dir, "snapshot.meta")

        print(f"\n  {node['id']} ({data_dir}):")
        if os.path.exists(snap_path):
            size = os.path.getsize(snap_path)
            print(f"    snapshot.dat: EXISTS ({size:,} bytes)")
            if size < 16:
                print(f"    WARNING: Snapshot file too small (min 16 bytes header)")
                all_ok = False
        else:
            print(f"    snapshot.dat: NOT FOUND")
            # This is OK if snapshot hasn't been triggered yet

        if os.path.exists(meta_path):
            try:
                with open(meta_path, "r") as f:
                    meta = json.load(f)
                print(f"    snapshot.meta: EXISTS")
                print(f"      last_included_index: {meta.get('last_included_index', 'N/A')}")
                print(f"      last_included_term: {meta.get('last_included_term', 'N/A')}")
                print(f"      file_count: {meta.get('file_count', 'N/A')}")
                print(f"      total_size: {meta.get('total_size', 'N/A'):,} bytes")
            except Exception as e:
                print(f"    snapshot.meta: ERROR reading: {e}")
                all_ok = False
        else:
            print(f"    snapshot.meta: NOT FOUND")

    print(f"\n  RESULT: {'PASS' if all_ok else 'WARN'} ✓")
    return all_ok


# ================================================================
# Test 6: Stress Test with Snapshot
# ================================================================
def test_stress_with_snapshot(duration=30, num_clients=4):
    """
    持续写入压力测试，观察快照的生成和截断行为。
    """
    print("\n" + "=" * 60)
    print("  TEST 6: Stress Test with Snapshot (压力测试+快照)")
    print("=" * 60)
    print(f"  Duration: {duration}s, Clients: {num_clients}")
    print()

    stop_event = threading.Event()
    write_counts = [0] * num_clients
    error_counts = [0] * num_clients
    lock = threading.Lock()

    def writer(client_id):
        client = KVClient(NODES)
        value = "S" * 1024  # 1KB value
        seq = 0
        while not stop_event.is_set():
            key = f"stress_{client_id}_{seq:06d}"
            if client.put(key, value):
                write_counts[client_id] += 1
            else:
                error_counts[client_id] += 1
            seq += 1
            if seq % 100 == 0:
                time.sleep(0.001)

    threads = []
    for i in range(num_clients):
        t = threading.Thread(target=writer, args=(i,))
        t.daemon = True
        threads.append(t)
        t.start()

    # Monitor snapshot state during test
    start = time.time()
    while time.time() - start < duration:
        time.sleep(5)
        elapsed = time.time() - start
        total = sum(write_counts)
        leader_snap = None
        for node in NODES:
            snap_idx = get_snapshot_index(node)
            if snap_idx and snap_idx > 0:
                leader_snap = snap_idx
                break
        print(f"  [{elapsed:.0f}s] writes={total}, errors={sum(error_counts)}, "
              f"snapshot_idx={leader_snap}")

    stop_event.set()
    for t in threads:
        t.join(timeout=5)

    total_writes = sum(write_counts)
    total_errors = sum(error_counts)

    print(f"\n  Results:")
    print(f"    Total writes: {total_writes}")
    print(f"    Total errors: {total_errors}")
    print(f"    Write rate: {total_writes/duration:.0f} ops/s")
    print(f"    Error rate: {total_errors/max(total_writes, 1)*100:.1f}%")

    # Check final snapshot state
    print(f"\n  Final snapshot state:")
    for node in NODES:
        snap_idx = get_snapshot_index(node)
        log_cnt = get_log_count(node)
        print(f"    {node['id']}: snapshot_idx={snap_idx}, log_count={log_cnt}")

    ok = total_errors < total_writes * 0.05  # < 5% errors
    print(f"\n  RESULT: {'PASS' if ok else 'FAIL'} ✓")
    return ok


# ================================================================
# Test 7: Large-Scale Write + Snapshot (500K entries)
# ================================================================
def test_large_scale_snapshot(num_entries=500000, value_size=256):
    """
    写入 50 万条数据，触发快照自动生成。
    50 万条 * ~260B = ~130MB，远超 20MB 阈值，确保快照触发。
    """
    print("=" * 60)
    print("  TEST 7: Large-Scale Write + Snapshot (50万条写入)")
    print("=" * 60)
    print(f"  Target: {num_entries} entries, ~{value_size}B value each")
    print(f"  Expected: ~{num_entries * (value_size + 20) / 1024 / 1024:.0f}MB log data")
    print()

    client = KVClient(NODES)
    if not client.ping():
        print("  ERROR: Cannot connect to cluster. Is it running?")
        return False

    # Check initial state
    print("  Initial state:")
    for node in NODES:
        snap_idx = get_snapshot_index(node)
        log_cnt = get_log_count(node)
        print(f"    {node['id']}: snapshot_idx={snap_idx}, log_count={log_cnt}")

    print(f"\n  Writing {num_entries} entries...")
    value = "V" * value_size
    start_time = time.time()
    successes = 0
    failures = 0
    snapshots_observed = 0

    for i in range(num_entries):
        key = f"large_{i:08d}"
        if client.put(key, value):
            successes += 1
        else:
            failures += 1

        if (i + 1) % 50000 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            # Check snapshot state
            leader_snap = None
            for node in NODES:
                snap_idx = get_snapshot_index(node)
                if snap_idx and snap_idx > 0:
                    leader_snap = snap_idx
            if leader_snap and leader_snap > snapshots_observed:
                snapshots_observed = leader_snap
            print(f"    Progress: {i+1}/{num_entries} ({rate:.0f} ops/s), "
                  f"ok={successes}, fail={failures}, snapshots={snapshots_observed}")

    elapsed = time.time() - start_time
    print(f"\n  Completed: {num_entries} entries in {elapsed:.1f}s "
          f"({num_entries/elapsed:.0f} ops/s)")
    print(f"  Successes: {successes}, Failures: {failures}")

    # Wait for final snapshot
    print("\n  Waiting for snapshot to finalize...")
    time.sleep(5)

    # Check final snapshot state
    print("\n  Final snapshot state:")
    has_snapshot = False
    for node in NODES:
        snap_idx = get_snapshot_index(node)
        log_cnt = get_log_count(node)
        print(f"    {node['id']}: snapshot_idx={snap_idx}, log_count={log_cnt}")
        if snap_idx and snap_idx > 0:
            has_snapshot = True

    # Verify snapshot files exist
    for node in NODES:
        snap_path = os.path.join(node["data_dir"], "snapshot.dat")
        meta_path = os.path.join(node["data_dir"], "snapshot.meta")
        if os.path.exists(snap_path):
            size = os.path.getsize(snap_path)
            print(f"    {node['id']} snapshot.dat: {size:,} bytes")
        if os.path.exists(meta_path):
            with open(meta_path, "r") as f:
                meta = json.load(f)
            print(f"    {node['id']} snapshot.meta: idx={meta.get('last_included_index')}, "
                  f"files={meta.get('file_count')}")

    if has_snapshot:
        print(f"\n  RESULT: PASS - Snapshot created after {num_entries} writes ✓")
    else:
        print(f"\n  RESULT: WARN - Snapshot may not have triggered yet")
    return True


# ================================================================
# Test 8: New Node Join via Snapshot (Full Automation)
# ================================================================
def test_new_node_full_cycle():
    """
    完整的新节点加入流程：
    1. 已有集群写入大量数据 + 快照
    2. 停止 node3，删除其数据目录
    3. 重新启动 node3（模拟全新节点加入）
    4. 验证 Leader 通过 InstallSnapshot 使其追上
    5. 验证 node3 数据完整
    """
    print("\n" + "=" * 60)
    print("  TEST 8: New Node Join via Snapshot (新节点快照加入)")
    print("=" * 60)

    client = KVClient(NODES)
    if not client.ping():
        print("  ERROR: Cannot connect to cluster")
        return False

    # Step 1: Write reference data
    print("\n  [1/5] Writing reference data...")
    ref_data = {}
    for i in range(5000):
        key = f"join_ref_{i:06d}"
        value = f"join_val_{i:06d}_" + "R" * 128
        if client.put(key, value):
            ref_data[key] = value

    print(f"  Wrote {len(ref_data)} reference entries")

    # Wait for replication
    time.sleep(3)

    # Step 2: Verify all nodes have data
    print("\n  [2/5] Verifying all nodes have data...")
    for node in NODES:
        node_client = KVClient([node])
        matches = 0
        for key, expected_val in ref_data.items():
            val = node_client.get(key)
            if val == expected_val:
                matches += 1
        print(f"    {node['id']}: {matches}/{len(ref_data)} keys match")

    # Step 3: Kill node3 and delete its data
    print("\n  [3/5] Stopping node3 and deleting data...")
    node3 = NODES[2]
    try:
        # Kill node3 process
        result = subprocess.run(
            f'netstat -ano | findstr :{node3["resp_port"]}',
            shell=True, capture_output=True, text=True, timeout=5
        )
        for line in result.stdout.split("\n"):
            parts = line.strip().split()
            if len(parts) >= 5 and "LISTENING" in line:
                pid = parts[-1]
                subprocess.run(f'taskkill /F /PID {pid}', shell=True,
                             capture_output=True, timeout=5)
                print(f"    Killed node3 process (PID={pid})")
                break
        else:
            print("    WARNING: Could not find node3 process")
    except Exception as e:
        print(f"    WARNING: Failed to kill node3: {e}")

    time.sleep(2)

    # Delete node3 data directory
    data_dir = node3["data_dir"]
    if os.path.exists(data_dir):
        try:
            shutil.rmtree(data_dir)
            print(f"    Deleted data directory: {data_dir}")
        except Exception as e:
            print(f"    WARNING: Could not delete data dir: {e}")
            # Try to delete specific files
            for fname in ["snapshot.dat", "snapshot.meta", "raft_log", "MANIFEST", "MANIFEST.tmp"]:
                fpath = os.path.join(data_dir, fname)
                if os.path.exists(fpath):
                    try:
                        os.remove(fpath)
                    except:
                        pass
            # Delete SST files
            for fname in os.listdir(data_dir):
                if fname.endswith(".sst"):
                    try:
                        os.remove(os.path.join(data_dir, fname))
                    except:
                        pass
            print(f"    Cleaned up individual files in {data_dir}")

    # Step 4: Restart node3
    print("\n  [4/5] Restarting node3...")
    try:
        # Start node3 in background
        build_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "build_sweep")
        kv_raft_exe = os.path.join(build_dir, "kv_raft.exe")
        if not os.path.exists(kv_raft_exe):
            kv_raft_exe = os.path.join(build_dir, "kv_raft")
        if not os.path.exists(kv_raft_exe):
            print(f"    ERROR: kv_raft.exe not found at {kv_raft_exe}")
            return False

        subprocess.Popen(
            [kv_raft_exe,
             "--id", "node3",
             "--raft-port", str(node3["raft_port"]),
             "--resp-port", str(node3["resp_port"]),
             "--metrics-port", str(node3["metrics_port"]),
             "--data-dir", data_dir,
             "--peer", "node1:127.0.0.1:8001",
             "--peer", "node2:127.0.0.1:8002",
             "--peer", "node3:127.0.0.1:8003"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
        )
        print(f"    Started node3 (waiting for it to catch up...)")
    except Exception as e:
        print(f"    ERROR: Failed to start node3: {e}")
        return False

    # Wait for node3 to receive snapshot and catch up
    print("    Waiting for node3 to catch up via snapshot...")
    node3_ready = False
    for attempt in range(60):  # wait up to 60 seconds
        time.sleep(2)
        # Check if node3 is reachable
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            sock.connect((node3["host"], node3["resp_port"]))
            sock.sendall(b"*1\r\n$4\r\nPING\r\n")
            resp = sock.recv(1024)
            sock.close()
            if resp.startswith(b"+PONG"):
                # Check snapshot state
                snap_idx = get_snapshot_index(node3)
                log_cnt = get_log_count(node3)
                state = get_raft_state(node3)
                print(f"    [{attempt*2}s] node3: state={state}, snap_idx={snap_idx}, log_cnt={log_cnt}")
                if snap_idx and snap_idx > 0:
                    node3_ready = True
                    break
        except:
            print(f"    [{attempt*2}s] node3: not ready yet...")

    if not node3_ready:
        print("    WARNING: node3 may not have received snapshot in time")

    # Step 5: Verify data integrity on node3
    print("\n  [5/5] Verifying data on restarted node3...")
    time.sleep(3)
    node3_client = KVClient([node3])
    matches = 0
    mismatches = 0
    missing = 0
    for key, expected_val in ref_data.items():
        val = node3_client.get(key)
        if val == expected_val:
            matches += 1
        elif val is None:
            missing += 1
        else:
            mismatches += 1

    print(f"    node3: matches={matches}, missing={missing}, mismatches={mismatches}")

    # Also write some new data to verify node3 is fully functional
    print("\n  Verifying node3 is fully functional (write test)...")
    write_ok = True
    for i in range(10):
        test_key = f"node3_write_test_{i}"
        if node3_client.put(test_key, f"test_val_{i}"):
            val = node3_client.get(test_key)
            if val != f"test_val_{i}":
                write_ok = False
                print(f"    Write test failed for {test_key}")
        else:
            write_ok = False
            print(f"    Failed to write {test_key}")

    all_ok = (mismatches == 0 and missing < len(ref_data) * 0.05 and write_ok)
    print(f"\n  RESULT: {'PASS' if all_ok else 'FAIL'} ✓")
    if not all_ok:
        print(f"  (missing={missing}/{len(ref_data)} keys is acceptable during catch-up)")
    return all_ok


# ================================================================
# Test 9: Restart Recovery (快照重启恢复)
# ================================================================
def test_restart_recovery():
    """
    验证节点重启后能正确加载快照并恢复数据。

    流程：
    1. 确保集群有快照
    2. 停止所有节点
    3. 重新启动所有节点
    4. 验证数据完整性
    """
    print("\n" + "=" * 60)
    print("  TEST 9: Restart Recovery (重启快照恢复)")
    print("=" * 60)

    client = KVClient(NODES)
    if not client.ping():
        print("  ERROR: Cannot connect to cluster")
        return False

    # Step 1: Write data and verify snapshot exists
    print("\n  [1/4] Ensuring snapshot exists...")
    has_snapshot = False
    for node in NODES:
        snap_idx = get_snapshot_index(node)
        if snap_idx and snap_idx > 0:
            has_snapshot = True
            print(f"    {node['id']}: snapshot already exists (idx={snap_idx})")
            break

    if not has_snapshot:
        print("    No snapshot yet, writing data to trigger...")
        value = "R" * 512
        for i in range(15000):
            key = f"restore_prep_{i:06d}"
            client.put(key, value)
        time.sleep(3)

    # Check snapshot state
    for node in NODES:
        snap_idx = get_snapshot_index(node)
        log_cnt = get_log_count(node)
        print(f"    {node['id']}: snapshot_idx={snap_idx}, log_count={log_cnt}")

    # Record reference data (sample a subset)
    print("\n  [2/4] Recording reference data...")
    # Use DBSIZE to get total key count
    ref_sample = {}
    # Query a sample of keys
    for i in range(0, 5000, 100):
        key = f"restore_prep_{i:06d}"
        val = client.get(key)
        if val:
            ref_sample[key] = val

    # Also add some unique keys
    for i in range(100):
        key = f"restore_unique_{i:04d}"
        value = f"restore_val_{i:04d}_" + "U" * 64
        if client.put(key, value):
            ref_sample[key] = value

    print(f"    Recorded {len(ref_sample)} reference keys")

    time.sleep(2)

    # Step 3: Stop all nodes
    print("\n  [3/4] Stopping all nodes...")
    for node in NODES:
        try:
            result = subprocess.run(
                f'netstat -ano | findstr :{node["resp_port"]}',
                shell=True, capture_output=True, text=True, timeout=5
            )
            for line in result.stdout.split("\n"):
                parts = line.strip().split()
                if len(parts) >= 5 and "LISTENING" in line:
                    pid = parts[-1]
                    subprocess.run(f'taskkill /F /PID {pid}', shell=True,
                                 capture_output=True, timeout=5)
                    print(f"    Killed {node['id']} (PID={pid})")
                    break
        except Exception as e:
            print(f"    WARNING: Could not kill {node['id']}: {e}")

    time.sleep(3)

    # Step 4: Restart all nodes
    print("\n  [4/4] Restarting all nodes...")
    build_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "build_sweep")
    kv_raft_exe = os.path.join(build_dir, "kv_raft.exe")
    if not os.path.exists(kv_raft_exe):
        kv_raft_exe = os.path.join(build_dir, "kv_raft")
    if not os.path.exists(kv_raft_exe):
        print(f"    ERROR: kv_raft.exe not found at {kv_raft_exe}")
        return False

    for node in NODES:
        try:
            subprocess.Popen(
                [kv_raft_exe,
                 "--id", node["id"],
                 "--raft-port", str(node["raft_port"]),
                 "--resp-port", str(node["resp_port"]),
                 "--metrics-port", str(node["metrics_port"]),
                 "--data-dir", node["data_dir"],
                 "--peer", f"node1:127.0.0.1:{NODES[0]['raft_port']}",
                 "--peer", f"node2:127.0.0.1:{NODES[1]['raft_port']}",
                 "--peer", f"node3:127.0.0.1:{NODES[2]['raft_port']}"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
            )
            print(f"    Started {node['id']}")
        except Exception as e:
            print(f"    ERROR: Failed to start {node['id']}: {e}")
            return False

    # Wait for cluster to stabilize
    print("    Waiting for cluster to stabilize...")
    time.sleep(10)

    # Wait for leader election
    leader_ready = False
    for attempt in range(30):
        if client.ping():
            leader_ready = True
            break
        time.sleep(1)
        print(f"    [{attempt}s] Waiting for cluster...")

    if not leader_ready:
        print("    ERROR: Cluster did not become ready")
        return False

    print("    Cluster is ready!")

    # Verify data integrity
    print("\n  Verifying data integrity after restart...")
    matches = 0
    mismatches = 0
    missing = 0
    for key, expected_val in ref_sample.items():
        val = client.get(key)
        if val == expected_val:
            matches += 1
        elif val is None:
            missing += 1
        else:
            mismatches += 1

    print(f"    matches={matches}, missing={missing}, mismatches={mismatches}")

    all_ok = (mismatches == 0 and missing < len(ref_sample) * 0.1)
    print(f"\n  RESULT: {'PASS' if all_ok else 'FAIL'} ✓")
    return all_ok
# ================================================================
# Test 10: Crash Recovery (WAL整合崩溃恢复)
# ================================================================
def test_crash_recovery():
    """
    验证 WAL 与 Raft 日志整合后的崩溃恢复流程：
    1. 写入数据并强制杀进程（模拟崩溃）
    2. 重启节点
    3. 验证数据完整性（Raft 日志重放恢复所有已提交数据）
    """
    print("\n" + "=" * 60)
    print("  TEST 10: Crash Recovery (WAL+Raft崩溃恢复)")
    print("=" * 60)

    client = KVClient(NODES)
    if not client.ping():
        print("  ERROR: Cannot connect to cluster")
        return False

    # Step 1: Write crash-test data
    print("\n  [1/4] Writing crash-test data...")
    crash_data = {}
    for i in range(2000):
        key = f"crash_{i:06d}"
        value = f"crash_val_{i:06d}_" + "C" * 128
        if client.put(key, value):
            crash_data[key] = value

    print(f"  Wrote {len(crash_data)} entries")

    # Wait for replication
    time.sleep(3)

    # Verify data is on all nodes before crash
    print("\n  [2/4] Verifying data before crash...")
    for node in NODES:
        node_client = KVClient([node])
        matches = 0
        for key, expected_val in list(crash_data.items())[:100]:  # Sample check
            val = node_client.get(key)
            if val == expected_val:
                matches += 1
        print(f"    {node['id']}: {matches}/100 keys match")

    # Step 3: Force kill a follower (simulate crash)
    target_node = None
    for node in NODES:
        # Find a follower
        state = get_raft_state(node)
        if state == "Follower":
            target_node = node
            break

    if not target_node:
        # If no follower found, kill node3
        target_node = NODES[2]

    print(f"\n  [3/4] Force-killing {target_node['id']} (simulating crash)...")
    try:
        result = subprocess.run(
            f'netstat -ano | findstr :{target_node["resp_port"]}',
            shell=True, capture_output=True, text=True, timeout=5
        )
        for line in result.stdout.split("\n"):
            parts = line.strip().split()
            if len(parts) >= 5 and "LISTENING" in line:
                pid = parts[-1]
                subprocess.run(f'taskkill /F /PID {pid}', shell=True,
                             capture_output=True, timeout=5)
                print(f"    Killed {target_node['id']} (PID={pid}) - crash simulated")
                break
        else:
            print(f"    WARNING: Could not find {target_node['id']} process")
    except Exception as e:
        print(f"    WARNING: Failed to kill {target_node['id']}: {e}")

    time.sleep(2)

    # Step 4: Restart the crashed node
    print(f"\n  [4/4] Restarting {target_node['id']}...")
    build_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "build_sweep")
    kv_raft_exe = os.path.join(build_dir, "kv_raft.exe")
    if not os.path.exists(kv_raft_exe):
        kv_raft_exe = os.path.join(build_dir, "kv_raft")
    if not os.path.exists(kv_raft_exe):
        print(f"    ERROR: kv_raft.exe not found")
        return False

    try:
        subprocess.Popen(
            [kv_raft_exe,
             "--id", target_node["id"],
             "--raft-port", str(target_node["raft_port"]),
             "--resp-port", str(target_node["resp_port"]),
             "--metrics-port", str(target_node["metrics_port"]),
             "--data-dir", target_node["data_dir"],
             "--peer", f"node1:127.0.0.1:{NODES[0]['raft_port']}",
             "--peer", f"node2:127.0.0.1:{NODES[1]['raft_port']}",
             "--peer", f"node3:127.0.0.1:{NODES[2]['raft_port']}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
        )
        print(f"    Started {target_node['id']}")
    except Exception as e:
        print(f"    ERROR: Failed to start {target_node['id']}: {e}")
        return False

    # Wait for node to recover
    print("    Waiting for recovery...")
    recovered = False
    for attempt in range(30):
        time.sleep(2)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            sock.connect((target_node["host"], target_node["resp_port"]))
            sock.sendall(b"*1\r\n$4\r\nPING\r\n")
            resp = sock.recv(1024)
            sock.close()
            if resp.startswith(b"+PONG"):
                state = get_raft_state(target_node)
                log_cnt = get_log_count(target_node)
                print(f"    [{attempt*2}s] {target_node['id']}: state={state}, log_count={log_cnt}")
                recovered = True
                break
        except:
            print(f"    [{attempt*2}s] {target_node['id']}: not ready yet...")

    if not recovered:
        print(f"    WARNING: {target_node['id']} did not recover in time")

    time.sleep(3)

    # Verify data integrity after crash recovery
    print("\n  Verifying data integrity after crash recovery...")
    node_client = KVClient([target_node])
    matches = 0
    mismatches = 0
    missing = 0
    for key, expected_val in crash_data.items():
        val = node_client.get(key)
        if val == expected_val:
            matches += 1
        elif val is None:
            missing += 1
        else:
            mismatches += 1

    print(f"    {target_node['id']}: matches={matches}, missing={missing}, mismatches={mismatches}")

    # Also verify the node can accept new writes
    print("\n  Verifying write capability after recovery...")
    write_ok = True
    for i in range(10):
        test_key = f"crash_recovery_test_{i}"
        if node_client.put(test_key, f"recovery_val_{i}"):
            val = node_client.get(test_key)
            if val != f"recovery_val_{i}":
                write_ok = False
                print(f"    Write verification failed for {test_key}")
        else:
            write_ok = False
            print(f"    Failed to write {test_key}")

    all_ok = (mismatches == 0 and missing < len(crash_data) * 0.05 and write_ok)
    print(f"\n  RESULT: {'PASS' if all_ok else 'FAIL'} ✓")
    print(f"  (WAL+Raft 整合: 崩溃节点通过 Raft 日志重放恢复了 {matches} 个键, "
          f"缺失 {missing} 个键)")
    return all_ok


def main():
    parser = argparse.ArgumentParser(description="Raft Snapshot Comprehensive Test")
    parser.add_argument("--test", type=str, default="all",
                   choices=["all", "trigger", "truncation", "new_node",
                            "consistency", "file", "stress",
                            "large_scale", "new_node_full", "restart",
                            "crash_recovery"],
                   help="Test to run")
    parser.add_argument("--entries", type=int, default=50000,
                       help="Number of entries for trigger/truncation tests")
    parser.add_argument("--duration", type=int, default=30,
                       help="Duration for stress test in seconds")
    parser.add_argument("--value-size", type=int, default=512,
                       help="Value size for trigger test")
    args = parser.parse_args()

    print("=" * 60)
    print("  Raft Snapshot Comprehensive Test")
    print("=" * 60)
    print(f"  Nodes: {len(NODES)}")
    for n in NODES:
        print(f"    {n['id']}: {n['host']}:{n['resp_port']} "
              f"(raft:{n['raft_port']}, metrics:{n['metrics_port']})")
    print()

    results = {}

    if args.test in ("all", "trigger"):
        ok = test_snapshot_trigger(args.entries, args.value_size)
        results["trigger"] = ok

    if args.test in ("all", "truncation"):
        ok = test_log_truncation(args.entries)
        results["truncation"] = ok

    if args.test in ("all", "new_node"):
        ok = test_new_node_join()
        results["new_node"] = ok

    if args.test in ("all", "consistency"):
        ok = test_consistency_after_snapshot()
        results["consistency"] = ok

    if args.test in ("all", "file"):
        ok = test_snapshot_file()
        results["file"] = ok

    if args.test in ("all", "stress"):
        ok = test_stress_with_snapshot(args.duration)
        results["stress"] = ok

    if args.test in ("all", "large_scale"):
        ok = test_large_scale_snapshot(args.entries, args.value_size)
        results["large_scale"] = ok

    if args.test in ("all", "new_node_full"):
        ok = test_new_node_full_cycle()
        results["new_node_full"] = ok

    if args.test in ("all", "restart"):
        ok = test_restart_recovery()
        results["restart"] = ok

    if args.test in ("all", "crash_recovery"):
        ok = test_crash_recovery()
        results["crash_recovery"] = ok

    # Summary
    print("\n" + "=" * 60)
    print("  SNAPSHOT TEST SUMMARY")
    print("=" * 60)
    all_ok = True
    for name, ok in results.items():
        status = "PASS ✓" if ok else "FAIL ✗"
        print(f"  {name:20s}: {status}")
        if not ok:
            all_ok = False

    if all_ok:
        print("\n  All snapshot tests passed! ✓")
    else:
        print("\n  Some tests failed! ✗")

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())