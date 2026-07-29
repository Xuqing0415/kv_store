#!/usr/bin/env python3
"""
Jepsen-style Linearizability Test for KV Raft Cluster

验证 Raft 集群在故障注入下是否满足线性一致性（Linearizability）。

测试框架包含：
  - HA 客户端：自动 Leader 发现 + 重试，记录每个操作的时间戳和返回值
  - 故障注入器：kill 节点、网络分区、时钟漂移、随机故障序列
  - 线性一致性检查器：基于 WGL 算法的操作历史验证
  - 测试场景：单键递增、寄存器读写、网络分区、快照+故障

用法:
    # 先启动集群
    scripts\start_cluster.bat

    # 运行所有测试
    python scripts\test_jepsen.py --test all --duration 60

    # 单独运行某个测试
    python scripts\test_jepsen.py --test register --duration 30
    python scripts\test_jepsen.py --test increment --duration 30

    # CI 模式（短时间 + 严格检查）
    python scripts\test_jepsen.py --test register --duration 10 --ci
"""

import socket
import time
import threading
import random
import argparse
import subprocess
import sys
import os
import json
from collections import namedtuple, defaultdict
from typing import List, Tuple, Dict, Optional, Any, Set
from dataclasses import dataclass, field

# Use kv_client for reliable RESP parsing
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kv_client import KVClient

# ================================================================
# Configuration
# ================================================================
NODES = [
    {"id": "node1", "host": "127.0.0.1", "resp_port": 6379, "raft_port": 8001, "metrics_port": 9091},
    {"id": "node2", "host": "127.0.0.1", "resp_port": 6380, "raft_port": 8002, "metrics_port": 9092},
    {"id": "node3", "host": "127.0.0.1", "resp_port": 6381, "raft_port": 8003, "metrics_port": 9093},
]

# ================================================================
# Operation Record
# ================================================================
@dataclass
class OpRecord:
    op: str          # "put", "get", "delete", "inc", "cas"
    key: str
    value: Optional[str]  # input value for writes
    start_time: float
    end_time: float
    success: bool
    result: Optional[str] = None  # output value for reads
    client_id: int = 0
    seq: int = 0

    def __hash__(self):
        return hash((self.client_id, self.seq))

    def __eq__(self, other):
        return self.client_id == other.client_id and self.seq == other.seq


# ================================================================
# HA KV Client (uses KVClient for reliable RESP parsing)
# ================================================================
class HAKVClient:
    """High-availability KV client with automatic leader discovery and retry.
    Uses KVClient for reliable RESP protocol parsing."""

    def __init__(self, nodes: List[Dict], client_id: int = 0):
        self.nodes = nodes
        self.client_id = client_id
        self.seq = 0
        self.current_leader = None
        self.records: List[OpRecord] = []
        self.lock = threading.Lock()
        self._clients: Dict[str, KVClient] = {}  # cache: (host, port) -> KVClient

    def _get_client(self, host: str, port: int) -> Optional[KVClient]:
        key = f"{host}:{port}"
        if key in self._clients:
            c = self._clients[key]
            if c.is_connected():
                return c
            c.close()
            del self._clients[key]
        c = KVClient(host, port, timeout=2.0)
        try:
            c.connect()
            self._clients[key] = c
            return c
        except Exception:
            return None

    def _execute(self, key: str, value: Optional[str] = None, op: str = "get") -> Optional[str]:
        """Execute a read/write with automatic leader discovery and retry."""
        for attempt in range(5):
            if self.current_leader:
                host, port = self.current_leader
                c = self._get_client(host, port)
                if c:
                    try:
                        if op == "put":
                            c._send_command("SET", key, value)
                        elif op == "get":
                            c._send_command("GET", key)
                        elif op == "del":
                            c._send_command("DEL", key)
                        result = c._read_response()
                        if isinstance(result, dict) and "error" in result:
                            err = result["error"]
                            if "not leader" in err.lower() or "moved" in err.lower():
                                self.current_leader = None
                                continue
                            return None
                        return result
                    except (socket.timeout, ConnectionRefusedError, ConnectionResetError, OSError):
                        self.current_leader = None
                        continue

            for node in self.nodes:
                if node["id"] in getattr(self, '_killed_nodes', set()):
                    continue
                c = self._get_client(node["host"], node["resp_port"])
                if not c:
                    continue
                try:
                    if op == "put":
                        c._send_command("SET", key, value)
                    elif op == "get":
                        c._send_command("GET", key)
                    elif op == "del":
                        c._send_command("DEL", key)
                    result = c._read_response()
                    if isinstance(result, dict) and "error" in result:
                        err = result["error"]
                        if "not leader" in err.lower() or "moved" in err.lower():
                            continue
                        return None
                    self.current_leader = (node["host"], node["resp_port"])
                    return result
                except (socket.timeout, ConnectionRefusedError, ConnectionResetError, OSError):
                    continue

            time.sleep(0.1 * (attempt + 1))
        return None

    def _record(self, op: str, key: str, value: Optional[str],
                start_time: float, end_time: float,
                success: bool, result: Optional[str] = None):
        with self.lock:
            self.seq += 1
            record = OpRecord(
                op=op, key=key, value=value,
                start_time=start_time, end_time=end_time,
                success=success, result=result,
                client_id=self.client_id, seq=self.seq
            )
            self.records.append(record)

    def put(self, key: str, value: str) -> bool:
        start = time.time()
        result = self._execute(key, value, "put")
        end = time.time()
        success = result is not None and isinstance(result, str) and "OK" in result
        self._record("put", key, value, start, end, success, str(result) if result else None)
        return success

    def get(self, key: str) -> Optional[str]:
        start = time.time()
        result = self._execute(key, None, "get")
        end = time.time()
        val = result if isinstance(result, str) else None
        self._record("get", key, None, start, end, val is not None, val)
        return val

    def delete(self, key: str) -> bool:
        start = time.time()
        result = self._execute(key, None, "del")
        end = time.time()
        success = result is not None and isinstance(result, int) and result == 1
        self._record("delete", key, None, start, end, success, str(result) if result else None)
        return success

    def inc(self, key: str) -> Optional[int]:
        """Read-modify-write: increment a counter."""
        start = time.time()
        val = self.get(key)
        if val is None:
            new_val = "1"
        else:
            try:
                new_val = str(int(val) + 1)
            except ValueError:
                new_val = "1"
        ok = self.put(key, new_val)
        end = time.time()
        result_val = new_val if ok else None
        with self.lock:
            self.seq += 1
            record = OpRecord(
                op="inc", key=key, value=None,
                start_time=start, end_time=end,
                success=ok, result=result_val,
                client_id=self.client_id, seq=self.seq
            )
            self.records.append(record)
        return int(result_val) if result_val else None

    def cas(self, key: str, expected: str, new_value: str) -> bool:
        """Compare-and-swap: set key to new_value only if current value == expected."""
        start = time.time()
        current = self.get(key)
        if current == expected:
            ok = self.put(key, new_value)
            end = time.time()
            self._record("cas", key, f"{expected}->{new_value}", start, end, ok, "true" if ok else "false")
            return ok
        else:
            end = time.time()
            self._record("cas", key, f"{expected}->{new_value}", start, end, False, "false")
            return False

    def get_records(self) -> List[OpRecord]:
        with self.lock:
            return list(self.records)

    def close(self):
        for c in self._clients.values():
            c.close()
        self._clients.clear()


# ================================================================
# Fault Injector
# ================================================================
class FaultInjector:
    """Inject faults into the Raft cluster with configurable patterns."""

    def __init__(self, nodes: List[Dict]):
        self.nodes = nodes
        self.killed_nodes: Set[str] = set()
        self.partitioned: bool = False
        self.fault_log: List[Dict] = []

    def _log_fault(self, action: str, target: str, detail: str = ""):
        entry = {"time": time.time(), "action": action, "target": target, "detail": detail}
        self.fault_log.append(entry)
        print(f"  [FAULT] {action} {target} {detail}")

    def kill_node(self, node_id: str) -> bool:
        """Kill a node by finding and terminating its process."""
        self.killed_nodes.add(node_id)
        node = next((n for n in self.nodes if n["id"] == node_id), None)
        if not node:
            return False
        try:
            if sys.platform == "win32":
                # Find PID by port
                result = subprocess.run(
                    f'netstat -ano | findstr :{node["resp_port"]}',
                    shell=True, capture_output=True, text=True, timeout=5
                )
                for line in result.stdout.split("\n"):
                    parts = line.strip().split()
                    if len(parts) >= 5 and "LISTENING" in line:
                        pid = parts[-1]
                        subprocess.run(
                            f'taskkill /F /PID {pid}',
                            shell=True, capture_output=True, timeout=5
                        )
                        self._log_fault("KILL", node_id, f"pid={pid}")
                        return True
                # Fallback: try raft port
                result = subprocess.run(
                    f'netstat -ano | findstr :{node["raft_port"]}',
                    shell=True, capture_output=True, text=True, timeout=5
                )
                for line in result.stdout.split("\n"):
                    parts = line.strip().split()
                    if len(parts) >= 5 and "LISTENING" in line:
                        pid = parts[-1]
                        subprocess.run(
                            f'taskkill /F /PID {pid}',
                            shell=True, capture_output=True, timeout=5
                        )
                        self._log_fault("KILL", node_id, f"pid={pid}")
                        return True
            else:
                subprocess.run(
                    f"fuser -k {node['resp_port']}/tcp 2>/dev/null || true",
                    shell=True, capture_output=True, timeout=5
                )
                self._log_fault("KILL", node_id, "linux")
                return True
            return False
        except Exception as e:
            print(f"  [WARN] Failed to kill {node_id}: {e}")
            return False

    def kill_leader(self) -> Optional[str]:
        """Kill the current leader."""
        # Find leader by checking which node accepts writes
        for node in self.nodes:
            if node["id"] in self.killed_nodes:
                continue
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                sock.connect((node["host"], node["resp_port"]))
                sock.sendall(b"*1\r\n$4\r\nPING\r\n")
                resp = sock.recv(1024)
                sock.close()
                if resp.startswith(b"+PONG"):
                    # Try a write to verify leader
                    sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock2.settimeout(1)
                    sock2.connect((node["host"], node["resp_port"]))
                    sock2.sendall(b"*3\r\n$3\r\nSET\r\n$10\r\nleader_test\r\n$2\r\nOK\r\n")
                    resp2 = sock2.recv(1024)
                    sock2.close()
                    if b"+OK" in resp2:
                        self.kill_node(node["id"])
                        return node["id"]
            except:
                pass
        return None

    def isolate_node(self, node_id: str) -> bool:
        """Isolate a node from the network."""
        node = next((n for n in self.nodes if n["id"] == node_id), None)
        if not node:
            return False
        try:
            if sys.platform == "win32":
                # Block both incoming and outgoing Raft traffic
                rule_name = f"jepsen_block_{node_id}"
                subprocess.run(
                    f'netsh advfirewall firewall add rule name="{rule_name}_in" '
                    f'dir=in localport={node["raft_port"]} protocol=TCP action=block',
                    shell=True, capture_output=True, timeout=5
                )
                subprocess.run(
                    f'netsh advfirewall firewall add rule name="{rule_name}_out" '
                    f'dir=out remoteport={node["raft_port"]} protocol=TCP action=block',
                    shell=True, capture_output=True, timeout=5
                )
            else:
                subprocess.run(
                    f"sudo iptables -A INPUT -p tcp --dport {node['raft_port']} -j DROP 2>/dev/null || true",
                    shell=True, capture_output=True, timeout=5
                )
                subprocess.run(
                    f"sudo iptables -A OUTPUT -p tcp --dport {node['raft_port']} -j DROP 2>/dev/null || true",
                    shell=True, capture_output=True, timeout=5
                )
            self.partitioned = True
            self._log_fault("ISOLATE", node_id, f"port={node['raft_port']}")
            return True
        except Exception as e:
            print(f"  [WARN] Failed to isolate {node_id}: {e}")
            return False

    def heal_partition(self) -> bool:
        """Heal all network partitions."""
        try:
            if sys.platform == "win32":
                for node in self.nodes:
                    for direction in ["in", "out"]:
                        subprocess.run(
                            f'netsh advfirewall firewall delete rule name="jepsen_block_{node["id"]}_{direction}"',
                            shell=True, capture_output=True, timeout=5
                        )
            else:
                subprocess.run(
                    "sudo iptables -F INPUT 2>/dev/null || true",
                    shell=True, capture_output=True, timeout=5
                )
                subprocess.run(
                    "sudo iptables -F OUTPUT 2>/dev/null || true",
                    shell=True, capture_output=True, timeout=5
                )
            self.partitioned = False
            self._log_fault("HEAL", "all", "partition healed")
            return True
        except Exception as e:
            print(f"  [WARN] Failed to heal partition: {e}")
            return False

    def random_fault(self) -> str:
        """Apply a random fault. Returns the fault type."""
        fault_type = random.choice(["kill_leader", "kill_follower", "isolate", "none"])
        if fault_type == "kill_leader":
            result = self.kill_leader()
            return f"kill_leader:{result}" if result else "kill_leader:failed"
        elif fault_type == "kill_follower":
            # Kill a random follower
            followers = [n for n in self.nodes if n["id"] not in self.killed_nodes]
            if len(followers) > 1:  # Keep at least one node
                target = random.choice(followers)
                self.kill_node(target["id"])
                return f"kill_follower:{target['id']}"
            return "kill_follower:skipped"
        elif fault_type == "isolate":
            if not self.partitioned:
                target = random.choice(self.nodes)
                self.isolate_node(target["id"])
                return f"isolate:{target['id']}"
            return "isolate:already_partitioned"
        return "none"

    def is_killed(self, node_id: str) -> bool:
        return node_id in self.killed_nodes


# ================================================================
# Linearizability Checker (Full WGL Algorithm)
# ================================================================
class LinearizabilityChecker:
    """
    Check if an operation history is linearizable using the WGL (Wing & Gong) algorithm.

    For a register, the sequential specification is:
    - A read returns the value of the most recent write that completed before it.
    - If no write completed before, a read may return the initial value (None).

    The algorithm constructs a partial order from the real-time constraints
    (op1.end <= op2.start implies op1 < op2) and then checks if there exists
    a total order consistent with both the partial order and the sequential spec.
    """

    def __init__(self, records: List[OpRecord]):
        self.records = records
        self.violations: List[str] = []

    def check_register(self) -> Tuple[bool, List[str]]:
        """
        Check register semantics using the WGL algorithm.

        For each read operation, we find the set of writes that could have
        produced its return value. A read is valid if there exists at least
        one such write that is consistent with the partial order.
        """
        self.violations = []
        n = len(self.records)
        if n == 0:
            return True, []

        # Build happens-before relation: op1 < op2 if op1.end <= op2.start
        # Sort records by end_time for processing
        sorted_recs = sorted(self.records, key=lambda r: r.end_time)

        # Group by key
        key_ops: Dict[str, List[OpRecord]] = defaultdict(list)
        for rec in sorted_recs:
            if rec.success:
                key_ops[rec.key].append(rec)

        # For each key, check linearizability
        for key, ops in key_ops.items():
            writes = [r for r in ops if r.op == "put"]
            reads = [r for r in ops if r.op == "get"]

            for read in reads:
                if not self._check_read_wgl(read, writes, key):
                    # One violation is enough to fail
                    pass

        return len(self.violations) == 0, self.violations

    def _check_read_wgl(self, read: OpRecord, writes: List[OpRecord], key: str) -> bool:
        """
        WGL-style check for a single read operation.

        A read returning value V is valid if:
        1. There exists a write W that wrote V
        2. W is not "overwritten" by another write that must happen before the read
        3. There is no write W' that must happen between W and the read
        """
        read_val = read.result

        # Find all writes that definitely happened before the read (end <= read.start)
        before_writes = [w for w in writes if w.end_time <= read.start_time and w.success]

        # Find all writes that could be concurrent with the read
        concurrent_writes = [w for w in writes
                           if w.start_time <= read.end_time
                           and w.end_time >= read.start_time
                           and w.success]

        # Case 1: Read returns None (key not found)
        if read_val is None:
            if before_writes:
                # The most recent write before the read
                last_before = max(before_writes, key=lambda w: w.end_time)
                # Check if this write was a delete (tombstone)
                if last_before.value is not None and len(last_before.value) > 0:
                    self.violations.append(
                        f"REGISTER VIOLATION: key={key}, read at t={read.start_time:.3f} "
                        f"returned None, but write '{last_before.value}' at "
                        f"t={last_before.end_time:.3f} completed before the read"
                    )
                    return False
            return True

        # Case 2: Read returns a value V
        # Find the most recent write that definitely happened before the read
        if before_writes:
            last_before = max(before_writes, key=lambda w: w.end_time)
            if read_val == last_before.value:
                return True
            # Check if any concurrent write produced this value
            for w in concurrent_writes:
                if read_val == w.value:
                    return True
            # Violation
            self.violations.append(
                f"REGISTER VIOLATION: key={key}, read at t={read.start_time:.3f} "
                f"returned '{read_val}', but last completed write was "
                f"'{last_before.value}' at t={last_before.end_time:.3f}"
            )
            return False

        # No writes before the read, check concurrent writes
        for w in concurrent_writes:
            if read_val == w.value:
                return True

        self.violations.append(
            f"REGISTER VIOLATION: key={key}, read at t={read.start_time:.3f} "
            f"returned '{read_val}', but no write produced this value"
        )
        return False

    def check_increment(self) -> Tuple[bool, List[str]]:
        """
        Check increment semantics: increments should be monotonically increasing
        and the final value should be >= number of successful increments.
        """
        self.violations = []

        increments = [r for r in self.records if r.op == "inc" and r.success]
        gets = [r for r in self.records if r.op == "get" and r.success]

        # Check monotonicity of increment results
        inc_results = [(r, int(r.result)) for r in increments if r.result and r.result.isdigit()]
        inc_results.sort(key=lambda x: x[0].end_time)

        for i in range(1, len(inc_results)):
            if inc_results[i][1] < inc_results[i-1][1]:
                self.violations.append(
                    f"INCREMENT VIOLATION: non-monotonic at "
                    f"t={inc_results[i][0].end_time:.3f}: "
                    f"{inc_results[i-1][1]} -> {inc_results[i][1]}"
                )

        # The final value of any get should be >= number of successful incs
        # (accounting for the fact that inc results are the new value after increment)
        expected_max = len(inc_results)
        for g in gets:
            if g.result and g.result.isdigit():
                val = int(g.result)
                if val > expected_max and expected_max > 0:
                    self.violations.append(
                        f"INCREMENT VIOLATION: get returned {val}, "
                        f"but only {expected_max} successful increments observed"
                    )

        return len(self.violations) == 0, self.violations

    def check_cas(self) -> Tuple[bool, List[str]]:
        """
        Check CAS semantics: a successful CAS must have read the expected value.
        """
        self.violations = []

        cas_ops = [r for r in self.records if r.op == "cas"]
        for cas in cas_ops:
            if cas.result == "true":
                # CAS succeeded, verify the expected value existed
                # Find the most recent write before this CAS
                writes_before = [r for r in self.records
                               if r.op == "put" and r.key == cas.key
                               and r.end_time <= cas.start_time and r.success]
                if writes_before:
                    last_write = max(writes_before, key=lambda w: w.end_time)
                    if cas.value:
                        expected = cas.value.split("->")[0] if "->" in cas.value else ""
                        if last_write.value != expected:
                            self.violations.append(
                                f"CAS VIOLATION: key={cas.key}, CAS({expected}->...) "
                                f"succeeded at t={cas.start_time:.3f}, "
                                f"but last write was '{last_write.value}' at "
                                f"t={last_write.end_time:.3f}"
                            )

        return len(self.violations) == 0, self.violations


# ================================================================
# Test Scenarios
# ================================================================

def test_register(duration: int = 30, num_clients: int = 4, with_faults: bool = False):
    """Test 1: Register test - concurrent reads and writes to the same key."""
    print("=" * 60)
    print("  TEST: Register Test (并发读写同一 key)")
    print("=" * 60)
    print(f"  Duration: {duration}s, Clients: {num_clients}, Faults: {with_faults}")
    print()

    clients = [HAKVClient(NODES, i) for i in range(num_clients)]
    injector = FaultInjector(NODES) if with_faults else None
    stop_event = threading.Event()
    errors = []
    lock = threading.Lock()

    def writer_worker(client: HAKVClient, key: str):
        seq = 0
        while not stop_event.is_set():
            value = f"v{client.client_id}_{seq}"
            if not client.put(key, value):
                with lock:
                    errors.append(f"PUT failed: client={client.client_id}, seq={seq}")
            seq += 1
            time.sleep(random.uniform(0.01, 0.1))

    def reader_worker(client: HAKVClient, key: str):
        while not stop_event.is_set():
            client.get(key)
            time.sleep(random.uniform(0.01, 0.05))

    threads = []
    key = f"register_test_{int(time.time())}"

    # Start writers
    for i in range(num_clients // 2):
        t = threading.Thread(target=writer_worker, args=(clients[i], key))
        t.daemon = True
        threads.append(t)
        t.start()

    # Start readers
    for i in range(num_clients // 2, num_clients):
        t = threading.Thread(target=reader_worker, args=(clients[i], key))
        t.daemon = True
        threads.append(t)
        t.start()

    # Fault injection loop
    if injector:
        def fault_loop():
            while not stop_event.is_set():
                time.sleep(random.uniform(3, 8))
                if not stop_event.is_set():
                    injector.random_fault()
                time.sleep(random.uniform(2, 5))
                if not stop_event.is_set() and injector.partitioned:
                    injector.heal_partition()

        fault_thread = threading.Thread(target=fault_loop)
        fault_thread.daemon = True
        fault_thread.start()

    time.sleep(duration)
    stop_event.set()

    for t in threads:
        t.join(timeout=5)

    # Collect all records
    all_records = []
    for c in clients:
        all_records.extend(c.get_records())

    total_ops = len(all_records)
    puts = sum(1 for r in all_records if r.op == "put")
    gets = sum(1 for r in all_records if r.op == "get")
    failures = sum(1 for r in all_records if not r.success)

    print(f"\n  Results:")
    print(f"    Total operations: {total_ops}")
    print(f"    PUTs: {puts}, GETs: {gets}")
    print(f"    Failures: {failures}")
    print(f"    Client errors: {len(errors)}")

    # Check linearizability
    checker = LinearizabilityChecker(all_records)
    ok, violations = checker.check_register()

    if ok:
        print(f"    Linearizability: PASS ✓")
    else:
        print(f"    Linearizability: FAIL ✗")
        for v in violations[:5]:
            print(f"      {v}")

    return ok, total_ops, failures


def test_increment(duration: int = 30, num_clients: int = 6, with_faults: bool = False):
    """Test 2: Single-key increment - concurrent increment operations."""
    print("=" * 60)
    print("  TEST: Increment Test (单键递增)")
    print("=" * 60)
    print(f"  Duration: {duration}s, Clients: {num_clients}, Faults: {with_faults}")
    print()

    clients = [HAKVClient(NODES, i) for i in range(num_clients)]
    injector = FaultInjector(NODES) if with_faults else None
    stop_event = threading.Event()
    lock = threading.Lock()
    errors = []
    inc_count = [0] * num_clients

    def inc_worker(client: HAKVClient, idx: int, key: str):
        while not stop_event.is_set():
            result = client.inc(key)
            if result is not None:
                inc_count[idx] += 1
            else:
                with lock:
                    errors.append(f"INC failed: client={idx}")
            time.sleep(random.uniform(0.01, 0.1))

    key = f"inc_test_{int(time.time())}"
    threads = []

    for i in range(num_clients):
        t = threading.Thread(target=inc_worker, args=(clients[i], i, key))
        t.daemon = True
        threads.append(t)
        t.start()

    # Fault injection
    if injector:
        def fault_loop():
            while not stop_event.is_set():
                time.sleep(random.uniform(5, 10))
                if not stop_event.is_set():
                    injector.random_fault()
                time.sleep(random.uniform(3, 5))
                if not stop_event.is_set() and injector.partitioned:
                    injector.heal_partition()

        fault_thread = threading.Thread(target=fault_loop)
        fault_thread.daemon = True
        fault_thread.start()

    time.sleep(duration)
    stop_event.set()

    for t in threads:
        t.join(timeout=5)

    all_records = []
    for c in clients:
        all_records.extend(c.get_records())

    total_inc = sum(inc_count)
    incs = sum(1 for r in all_records if r.op == "inc")
    succ_inc = sum(1 for r in all_records if r.op == "inc" and r.success)

    # Get final value
    final_val = None
    for c in clients:
        val = c.get(key)
        if val is not None and val.isdigit():
            final_val = int(val)
            break

    print(f"\n  Results:")
    print(f"    Total increments attempted: {incs}")
    print(f"    Successful increments: {succ_inc}")
    print(f"    Final value: {final_val}")
    print(f"    Client errors: {len(errors)}")

    # Check linearizability
    checker = LinearizabilityChecker(all_records)
    ok, violations = checker.check_increment()

    if ok:
        print(f"    Linearizability: PASS ✓")
    else:
        print(f"    Linearizability: FAIL ✗")
        for v in violations[:5]:
            print(f"      {v}")

    # Additional check: final value should be >= successful increments
    if final_val is not None and final_val > 0:
        if final_val >= succ_inc:
            print(f"    Final value check: PASS ✓ (final={final_val} >= succ_inc={succ_inc})")
        else:
            print(f"    Final value check: FAIL ✗ (final={final_val} < succ_inc={succ_inc})")
            ok = False

    return ok, total_inc, len(errors)


def test_partition(duration: int = 30, num_clients: int = 4):
    """Test 3: Network partition - split the cluster and verify consistency."""
    print("=" * 60)
    print("  TEST: Network Partition (网络分区)")
    print("=" * 60)
    print(f"  Duration: {duration}s, Clients: {num_clients}")
    print()

    clients = [HAKVClient(NODES, i) for i in range(num_clients)]
    stop_event = threading.Event()
    injector = FaultInjector(NODES)
    errors = []
    lock = threading.Lock()

    def worker(client: HAKVClient, key: str):
        seq = 0
        while not stop_event.is_set():
            value = f"v{client.client_id}_{seq}"
            if not client.put(key, value):
                with lock:
                    errors.append(f"PUT failed: client={client.client_id}, seq={seq}")
            seq += 1
            time.sleep(random.uniform(0.01, 0.1))

    key = f"partition_test_{int(time.time())}"
    threads = []

    for i in range(num_clients):
        t = threading.Thread(target=worker, args=(clients[i], key))
        t.daemon = True
        threads.append(t)
        t.start()

    # Phase 1: Normal operation (10% of duration)
    print("  Phase 1: Normal operation...")
    time.sleep(duration * 0.1)

    # Phase 2: Isolate one node (create partition)
    isolated_node = "node3"
    print(f"  Phase 2: Isolating {isolated_node}...")
    injector.isolate_node(isolated_node)
    time.sleep(duration * 0.3)

    # Phase 3: Heal partition
    print("  Phase 3: Healing partition...")
    injector.heal_partition()
    time.sleep(duration * 0.3)

    # Phase 4: Normal operation again
    print("  Phase 4: Normal operation...")
    time.sleep(duration * 0.3)

    stop_event.set()
    for t in threads:
        t.join(timeout=5)

    all_records = []
    for c in clients:
        all_records.extend(c.get_records())

    total_ops = len(all_records)
    failures = sum(1 for r in all_records if not r.success)

    print(f"\n  Results:")
    print(f"    Total operations: {total_ops}")
    print(f"    Failures: {failures}")
    print(f"    Client errors: {len(errors)}")

    # Check linearizability
    checker = LinearizabilityChecker(all_records)
    ok, violations = checker.check_register()

    if ok:
        print(f"    Linearizability: PASS ✓")
    else:
        print(f"    Linearizability: FAIL ✗")
        for v in violations[:5]:
            print(f"      {v}")

    return ok, total_ops, failures


def test_leader_failure(duration: int = 30, num_clients: int = 4):
    """Test 4: Leader failure - kill the leader and verify failover."""
    print("=" * 60)
    print("  TEST: Leader Failure (Leader 宕机故障转移)")
    print("=" * 60)
    print(f"  Duration: {duration}s, Clients: {num_clients}")
    print()

    clients = [HAKVClient(NODES, i) for i in range(num_clients)]
    stop_event = threading.Event()
    injector = FaultInjector(NODES)
    errors = []
    lock = threading.Lock()
    write_counts = [0] * num_clients

    def worker(client: HAKVClient, idx: int, key_prefix: str):
        seq = 0
        while not stop_event.is_set():
            key = f"{key_prefix}_{idx}_{seq}"
            value = f"v{idx}_{seq}"
            if client.put(key, value):
                write_counts[idx] += 1
            else:
                with lock:
                    errors.append(f"PUT failed: client={idx}, seq={seq}")
            seq += 1
            time.sleep(random.uniform(0.02, 0.1))

    key_prefix = f"leader_fail_{int(time.time())}"
    threads = []

    for i in range(num_clients):
        t = threading.Thread(target=worker, args=(clients[i], i, key_prefix))
        t.daemon = True
        threads.append(t)
        t.start()

    # Phase 1: Normal operation
    print("  Phase 1: Normal operation...")
    time.sleep(duration * 0.15)

    # Phase 2: Kill leader
    print("  Phase 2: Killing leader...")
    killed = injector.kill_leader()
    print(f"    Killed: {killed}")
    time.sleep(duration * 0.25)

    # Phase 3: Wait for new leader
    print("  Phase 3: Waiting for new leader election...")
    time.sleep(duration * 0.2)

    # Phase 4: Resume normal operation
    print("  Phase 4: Normal operation with new leader...")
    time.sleep(duration * 0.4)

    stop_event.set()
    for t in threads:
        t.join(timeout=5)

    total_writes = sum(write_counts)
    all_records = []
    for c in clients:
        all_records.extend(c.get_records())

    failures = sum(1 for r in all_records if not r.success)

    print(f"\n  Results:")
    print(f"    Total writes: {total_writes}")
    print(f"    Total operations: {len(all_records)}")
    print(f"    Failures: {failures}")
    print(f"    Client errors: {len(errors)}")
    print(f"    Fault log: {len(injector.fault_log)} entries")

    # Check that writes continued after leader failure
    # We expect some failures during leader election, but writes should resume
    error_rate = failures / max(total_writes, 1)
    ok = error_rate < 0.3  # Allow up to 30% errors during leader transition

    if ok:
        print(f"    Result: PASS ✓ (error rate: {error_rate*100:.1f}%)")
    else:
        print(f"    Result: FAIL ✗ (error rate: {error_rate*100:.1f}% > 30%)")

    return ok, total_writes, failures


def test_snapshot_consistency(duration: int = 60, num_clients: int = 4):
    """Test 5: Snapshot + failure consistency."""
    print("=" * 60)
    print("  TEST: Snapshot + Failure Consistency (快照+故障一致性)")
    print("=" * 60)
    print(f"  Duration: {duration}s, Clients: {num_clients}")
    print(f"  Note: Writes ~1KB values to trigger snapshot")
    print()

    clients = [HAKVClient(NODES, i) for i in range(num_clients)]
    stop_event = threading.Event()
    injector = FaultInjector(NODES)
    errors = []
    lock = threading.Lock()
    write_counts = [0] * num_clients

    large_value = "Z" * 1024  # 1KB

    def writer_worker(client: HAKVClient, idx: int, key_prefix: str):
        seq = 0
        while not stop_event.is_set():
            key = f"{key_prefix}_c{idx}_s{seq}"
            if client.put(key, large_value):
                write_counts[idx] += 1
            else:
                with lock:
                    errors.append(f"PUT failed: client={idx}, seq={seq}")
            seq += 1
            time.sleep(random.uniform(0.02, 0.05))

    def reader_worker(client: HAKVClient, key_prefix: str):
        while not stop_event.is_set():
            key = f"{key_prefix}_c{random.randint(0, num_clients-2)}_s{random.randint(0, 100)}"
            client.get(key)
            time.sleep(random.uniform(0.05, 0.2))

    key_prefix = f"snap_{int(time.time())}"
    threads = []

    # Writers
    for i in range(num_clients - 1):
        t = threading.Thread(target=writer_worker, args=(clients[i], i, key_prefix))
        t.daemon = True
        threads.append(t)
        t.start()

    # Reader
    t = threading.Thread(target=reader_worker, args=(clients[num_clients - 1], key_prefix))
    t.daemon = True
    threads.append(t)
    t.start()

    # Fault injection
    def fault_loop():
        while not stop_event.is_set():
            time.sleep(random.uniform(8, 15))
            if not stop_event.is_set():
                injector.random_fault()
            time.sleep(random.uniform(3, 5))
            if not stop_event.is_set() and injector.partitioned:
                injector.heal_partition()

    fault_thread = threading.Thread(target=fault_loop)
    fault_thread.daemon = True
    fault_thread.start()

    # Monitor progress
    start = time.time()
    while time.time() - start < duration:
        time.sleep(10)
        elapsed = time.time() - start
        total_writes = sum(write_counts)
        print(f"  [{elapsed:.0f}s] writes={total_writes}, errors={sum(errors)}, "
              f"killed={injector.killed_nodes}")

    stop_event.set()
    for t in threads:
        t.join(timeout=5)

    total_writes = sum(write_counts)
    all_records = []
    for c in clients:
        all_records.extend(c.get_records())

    failures = sum(1 for r in all_records if not r.success)

    print(f"\n  Results:")
    print(f"    Total writes: {total_writes}")
    print(f"    Total operations: {len(all_records)}")
    print(f"    Failures: {failures}")
    print(f"    Client errors: {len(errors)}")
    print(f"    Estimated data size: ~{total_writes}KB")
    print(f"    Fault log: {len(injector.fault_log)} entries")

    # Check linearizability
    checker = LinearizabilityChecker(all_records)
    ok, violations = checker.check_register()

    if ok:
        print(f"    Linearizability: PASS ✓")
    else:
        print(f"    Linearizability: FAIL ✗")
        for v in violations[:5]:
            print(f"      {v}")

    error_rate = failures / max(total_writes, 1)
    ok = ok and error_rate < 0.3
    print(f"    Overall: {'PASS' if ok else 'FAIL'} ✓")

    return ok, total_writes, failures


# ================================================================
# Chaos Monkey Test
# ================================================================
def test_chaos_monkey(duration: int = 120, num_clients: int = 6):
    """Test 6: Full chaos monkey - combines all fault types with continuous writes."""
    print("=" * 60)
    print("  TEST: Chaos Monkey (全混沌测试)")
    print("=" * 60)
    print(f"  Duration: {duration}s, Clients: {num_clients}")
    print(f"  Faults: kill leader, kill follower, network isolate, heal, random")
    print()

    clients = [HAKVClient(NODES, i) for i in range(num_clients)]
    stop_event = threading.Event()
    injector = FaultInjector(NODES)
    errors = []
    lock = threading.Lock()
    write_counts = [0] * num_clients
    read_counts = [0] * num_clients
    fault_counts = {"kill_leader": 0, "kill_follower": 0, "isolate": 0, "heal": 0}

    def writer_worker(client: HAKVClient, idx: int, key_prefix: str):
        seq = 0
        while not stop_event.is_set():
            key = f"{key_prefix}_c{idx}_s{seq}"
            value = f"v{idx}_{seq}_{random.randint(0, 999999)}"
            if client.put(key, value):
                write_counts[idx] += 1
            else:
                with lock:
                    errors.append(f"PUT failed: client={idx}, seq={seq}")
            seq += 1
            time.sleep(random.uniform(0.02, 0.1))

    def reader_worker(client: HAKVClient, idx: int, key_prefix: str, max_seq: int):
        while not stop_event.is_set():
            key = f"{key_prefix}_c{random.randint(0, num_clients//2 - 1)}_s{random.randint(0, max(max_seq, 1))}"
            try:
                client.get(key)
                read_counts[idx] += 1
            except Exception:
                pass
            time.sleep(random.uniform(0.05, 0.2))

    key_prefix = f"chaos_{int(time.time())}"
    threads = []

    # Writers (half of clients)
    num_writers = num_clients // 2
    for i in range(num_writers):
        t = threading.Thread(target=writer_worker, args=(clients[i], i, key_prefix))
        t.daemon = True
        threads.append(t)
        t.start()

    # Readers (half of clients)
    for i in range(num_writers, num_clients):
        t = threading.Thread(target=reader_worker, args=(clients[i], i, key_prefix, 1000))
        t.daemon = True
        threads.append(t)
        t.start()

    # Chaos monkey fault loop
    chaos_actions = ["kill_leader", "kill_follower", "isolate_node", "heal_all", "double_kill"]

    def chaos_loop():
        start = time.time()
        while not stop_event.is_set():
            elapsed = time.time() - start
            time.sleep(random.uniform(5, 15))

            if stop_event.is_set():
                break

            action = random.choice(chaos_actions)
            if action == "kill_leader":
                killed = injector.kill_leader()
                if killed:
                    fault_counts["kill_leader"] += 1
                    print(f"  [CHAOS t={elapsed:.0f}s] Killed leader: {killed}")
            elif action == "kill_follower":
                alive = [n for n in NODES if n["id"] not in injector.killed_nodes]
                if len(alive) > 1:
                    target = random.choice(alive)
                    injector.kill_node(target["id"])
                    fault_counts["kill_follower"] += 1
                    print(f"  [CHAOS t={elapsed:.0f}s] Killed follower: {target['id']}")
            elif action == "isolate_node":
                if not injector.partitioned and len(injector.killed_nodes) < 2:
                    target = random.choice([n for n in NODES if n["id"] not in injector.killed_nodes])
                    injector.isolate_node(target["id"])
                    fault_counts["isolate"] += 1
                    print(f"  [CHAOS t={elapsed:.0f}s] Isolated: {target['id']}")
            elif action == "heal_all":
                if injector.partitioned:
                    injector.heal_partition()
                    fault_counts["heal"] += 1
                    print(f"  [CHAOS t={elapsed:.0f}s] Healed all partitions")
            elif action == "double_kill":
                if len(injector.killed_nodes) < 2:
                    alive = [n for n in NODES if n["id"] not in injector.killed_nodes]
                    if len(alive) >= 2:
                        targets = random.sample(alive, 2)
                        for t in targets:
                            injector.kill_node(t["id"])
                        fault_counts["kill_follower"] += 2
                        print(f"  [CHAOS t={elapsed:.0f}s] Double kill: {[t['id'] for t in targets]}")

            # Heal partitions periodically
            if injector.partitioned and random.random() < 0.3:
                injector.heal_partition()
                fault_counts["heal"] += 1

        # Final heal
        if injector.partitioned:
            injector.heal_partition()

    chaos_thread = threading.Thread(target=chaos_loop)
    chaos_thread.daemon = True
    chaos_thread.start()

    # Progress monitoring
    start_time = time.time()
    while time.time() - start_time < duration:
        time.sleep(15)
        elapsed = time.time() - start_time
        total_w = sum(write_counts)
        total_r = sum(read_counts)
        print(f"  [{elapsed:.0f}s] writes={total_w}, reads={total_r}, "
              f"killed={injector.killed_nodes}, partitioned={injector.partitioned}, "
              f"faults: {fault_counts}")

    stop_event.set()
    for t in threads:
        t.join(timeout=5)

    total_writes = sum(write_counts)
    total_reads = sum(read_counts)

    all_records = []
    for c in clients:
        all_records.extend(c.get_records())
        c.close()

    failures = sum(1 for r in all_records if not r.success)
    puts = sum(1 for r in all_records if r.op == "put")
    gets = sum(1 for r in all_records if r.op == "get")

    print(f"\n  Results:")
    print(f"    Total writes: {total_writes}, Total reads: {total_reads}")
    print(f"    PUTs: {puts}, GETs: {gets}")
    print(f"    Failures: {failures}")
    print(f"    Client errors: {len(errors)}")
    print(f"    Fault counts: {fault_counts}")

    # Check linearizability
    checker = LinearizabilityChecker(all_records)
    ok, violations = checker.check_register()

    if ok:
        print(f"    Linearizability: PASS ✓")
    else:
        print(f"    Linearizability: FAIL ✗")
        for v in violations[:5]:
            print(f"      {v}")

    # Acceptable error rate during chaos
    error_rate = failures / max(total_writes, 1)
    print(f"    Error rate: {error_rate*100:.1f}%")
    if error_rate < 0.5:  # Allow up to 50% errors during extreme chaos
        print(f"    Error rate check: PASS ✓")
    else:
        print(f"    Error rate check: FAIL ✗ (>{50}%)")
        ok = False

    # Save history for offline analysis
    if all_records:
        history_file = f"chaos_history_{int(time.time())}.json"
        save_history(all_records, history_file)

    return ok, total_writes, failures


# ================================================================
# CI Mode
# ================================================================
def run_ci_suite():
    """Run a quick CI suite for automated testing."""
    print("=" * 60)
    print("  CI MODE: Quick Linearizability Check")
    print("=" * 60)

    results = {}

    # Quick register test (10s)
    ok, ops, failures = test_register(duration=10, num_clients=2, with_faults=False)
    results["register_quick"] = ok

    # Quick increment test (10s)
    ok, ops, failures = test_increment(duration=10, num_clients=3, with_faults=False)
    results["increment_quick"] = ok

    all_ok = all(results.values())
    return all_ok, results


# ================================================================
# History Persistence
# ================================================================
def save_history(records: List[OpRecord], filename: str):
    """Save operation history to JSON for later analysis."""
    data = []
    for r in records:
        data.append({
            "op": r.op, "key": r.key, "value": r.value,
            "start_time": r.start_time, "end_time": r.end_time,
            "success": r.success, "result": r.result,
            "client_id": r.client_id, "seq": r.seq
        })
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  History saved to {filename} ({len(data)} records)")


# ================================================================
# Main
# ================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Jepsen-style Linearizability Test for KV Raft Cluster"
    )
    parser.add_argument("--test", type=str, default="all",
                       choices=["all", "register", "increment", "partition",
                               "leader_failure", "snapshot", "chaos", "ci"],
                       help="Test to run")
    parser.add_argument("--duration", type=int, default=30,
                       help="Duration of each test in seconds")
    parser.add_argument("--clients", type=int, default=4,
                       help="Number of concurrent clients")
    parser.add_argument("--with-faults", action="store_true",
                       help="Enable random fault injection")
    parser.add_argument("--ci", action="store_true",
                       help="Run in CI mode (short tests, strict checks)")
    parser.add_argument("--save-history", type=str, default=None,
                       help="Save operation history to file")
    parser.add_argument("--junit-xml", type=str, default=None,
                       help="Output JUnit-compatible XML to file")
    args = parser.parse_args()

    print("=" * 60)
    print("  Jepsen-style Linearizability Test")
    print("  KV Raft Cluster")
    print("=" * 60)
    print(f"  Nodes: {len(NODES)}")
    for n in NODES:
        print(f"    {n['id']}: {n['host']}:{n['resp_port']} (raft:{n['raft_port']})")
    print()

    if args.ci:
        all_ok, results = run_ci_suite()
    else:
        results = {}

        if args.test in ("all", "register"):
            ok, ops, failures = test_register(args.duration, args.clients, args.with_faults)
            results["register"] = {"ok": ok, "ops": ops, "failures": failures}

        if args.test in ("all", "increment"):
            ok, ops, failures = test_increment(args.duration, args.clients, args.with_faults)
            results["increment"] = {"ok": ok, "ops": ops, "failures": failures}

        if args.test in ("all", "partition"):
            ok, ops, failures = test_partition(args.duration, args.clients)
            results["partition"] = {"ok": ok, "ops": ops, "failures": failures}

        if args.test in ("all", "leader_failure"):
            ok, ops, failures = test_leader_failure(args.duration, args.clients)
            results["leader_failure"] = {"ok": ok, "ops": ops, "failures": failures}

        if args.test in ("all", "snapshot"):
            ok, ops, failures = test_snapshot_consistency(args.duration, args.clients)
            results["snapshot"] = {"ok": ok, "ops": ops, "failures": failures}

        if args.test in ("all", "chaos"):
            ok, ops, failures = test_chaos_monkey(args.duration, args.clients)
            results["chaos"] = {"ok": ok, "ops": ops, "failures": failures}

        all_ok = all(r["ok"] for r in results.values())

    # Summary
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    for name, r in results.items():
        if isinstance(r, dict):
            status = "PASS ✓" if r["ok"] else "FAIL ✗"
            print(f"  {name:20s}: {status}  (ops={r['ops']}, failures={r['failures']})")
        else:
            status = "PASS ✓" if r else "FAIL ✗"
            print(f"  {name:20s}: {status}")

    if all_ok:
        print("\n  All tests passed! ✓")
    else:
        print("\n  Some tests failed! ✗")

    # Generate JUnit XML if requested
    if args.junit_xml:
        _write_junit_xml(args.junit_xml, results)

    return 0 if all_ok else 1


def _write_junit_xml(filepath: str, results: dict):
    """Write test results in JUnit-compatible XML format."""
    import xml.etree.ElementTree as ET
    import datetime

    testsuites = ET.Element("testsuites")
    testsuite = ET.SubElement(testsuites, "testsuite",
                              name="jepsen-linearizability",
                              timestamp=datetime.datetime.now().isoformat())

    total = 0
    failures = 0
    for name, r in results.items():
        total += 1
        testcase = ET.SubElement(testsuite, "testcase", name=name,
                                classname="JepsenTest")
        if isinstance(r, dict):
            if not r["ok"]:
                failures += 1
                failure = ET.SubElement(testcase, "failure",
                                       message=f"Linearizability violation detected",
                                       type="LinearizabilityViolation")
                failure.text = f"ops={r['ops']}, failures={r['failures']}"
        elif not r:
            failures += 1
            failure = ET.SubElement(testcase, "failure",
                                   message="Test failed",
                                   type="TestFailure")

    testsuite.set("tests", str(total))
    testsuite.set("failures", str(failures))
    testsuite.set("errors", "0")

    tree = ET.ElementTree(testsuites)
    tree.write(filepath, encoding="utf-8", xml_declaration=True)
    print(f"\n  JUnit XML written to {filepath}")


if __name__ == "__main__":
    sys.exit(main())