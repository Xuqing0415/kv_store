#!/usr/bin/env python3
"""
Raft Cluster Integration Test

Tests the 3-node Raft KV cluster:
  1. Leader election — a leader should be elected within ~5 seconds
  2. Write to leader, read from follower — data replication
  3. Multi-key write/read — consistency across nodes
  4. Metrics endpoint — Prometheus metrics available

Usage:
  python scripts/test_cluster.py
"""

import subprocess
import time
import socket
import sys
import os

NODES = [
    {"id": "node1", "raft_port": 8001, "resp_port": 6379, "metrics_port": 9091},
    {"id": "node2", "raft_port": 8002, "resp_port": 6380, "metrics_port": 9092},
    {"id": "node3", "raft_port": 8003, "resp_port": 6381, "metrics_port": 9093},
]

BINARY = "kv_raft.exe" if sys.platform == "win32" else "./kv_raft"
BUILD_DIR = "build_sweep" if sys.platform == "win32" else "build"

PASS = 0
FAIL = 0


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
    """Check if a TCP port is open."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, port))
        s.close()
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


def http_get(url, timeout=2):
    """Simple HTTP GET request."""
    try:
        import urllib.request
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.read().decode("utf-8")
    except Exception as e:
        return None


def send_redis_cmd(host, port, *args):
    """Send a Redis command and return the response."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect((host, port))

        # Build RESP command
        parts = [f"*{len(args)}\r\n"]
        for arg in args:
            arg_str = str(arg)
            parts.append(f"${len(arg_str)}\r\n{arg_str}\r\n")
        cmd = "".join(parts).encode()

        s.sendall(cmd)

        # Read response
        resp = b""
        while True:
            try:
                chunk = s.recv(4096)
                if not chunk:
                    break
                resp += chunk
                if resp.endswith(b"\r\n"):
                    # Check if it's a simple string or integer
                    if resp.startswith(b"+") or resp.startswith(b":") or resp.startswith(b"-"):
                        break
                    # For bulk strings, check if we have the full payload
                    if resp.startswith(b"$"):
                        lines = resp.split(b"\r\n")
                        if len(lines) >= 2 and lines[0] != b"$-1":
                            try:
                                length = int(lines[0][1:])
                                if len(lines) >= 3 and len(lines[1]) >= length:
                                    break
                            except ValueError:
                                break
                    # For arrays
                    if resp.startswith(b"*"):
                        break
            except socket.timeout:
                break

        s.close()
        return resp.decode("utf-8", errors="replace").strip()
    except Exception as e:
        return f"ERROR: {e}"


def main():
    global PASS, FAIL
    print("=" * 60)
    print("  Raft Cluster Integration Test")
    print("=" * 60)
    print()

    # Locate binary
    bin_path = os.path.join(BUILD_DIR, BINARY)
    if not os.path.exists(bin_path):
        bin_path = os.path.join("..", BUILD_DIR, BINARY)
    if not os.path.exists(bin_path):
        print(f"ERROR: Binary not found at {bin_path}")
        print("Build with: cmake --build build_sweep --target kv_raft")
        return 1

    print(f"Binary: {bin_path}")
    print()

    # Check if nodes are already running
    any_running = False
    for node in NODES:
        if tcp_connect("127.0.0.1", node["raft_port"], timeout=1):
            any_running = True
            break

    if not any_running:
        print("Starting 3-node cluster...")
        processes = []
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

        print()
        print("Waiting for cluster to stabilize (5 seconds)...")
        time.sleep(5)
        cleanup = True
    else:
        print("Nodes already running, using existing cluster.")
        cleanup = False

    print()

    # Test 1: All nodes are reachable
    print("--- Test 1: Node Connectivity ---")
    for node in NODES:
        ok = tcp_connect("127.0.0.1", node["resp_port"], timeout=2)
        check(f"{node['id']} RESP port {node['resp_port']} reachable", ok)

    print()

    # Test 2: PING all nodes
    print("--- Test 2: PING ---")
    for node in NODES:
        resp = send_redis_cmd("127.0.0.1", node["resp_port"], "PING")
        check(f"{node['id']} PING -> {resp}", "PONG" in resp)

    print()

    # Test 3: Write to one node, read from all
    print("--- Test 3: Cross-Node Replication ---")
    # Try writing to each node until one succeeds (the leader)
    leader = None
    for node in NODES:
        resp = send_redis_cmd("127.0.0.1", node["resp_port"], "SET", "cluster:test", "raft_works")
        if "OK" in resp or "+OK" in resp:
            leader = node
            print(f"  Leader is {node['id']} (port {node['resp_port']})")
            break
        elif "MOVED" in resp or "REDIRECT" in resp:
            print(f"  {node['id']} redirected: {resp}")

    check("Leader elected and write succeeded", leader is not None)

    if leader:
        time.sleep(0.5)  # Wait for replication
        for node in NODES:
            resp = send_redis_cmd("127.0.0.1", node["resp_port"], "GET", "cluster:test")
            check(f"{node['id']} GET cluster:test -> {resp}", "raft_works" in resp)

    print()

    # Test 4: Multi-key operations
    print("--- Test 4: Multi-Key Write/Read ---")
    keys_written = 0
    if leader:
        for i in range(10):
            resp = send_redis_cmd("127.0.0.1", leader["resp_port"], "SET", f"key_{i}", f"value_{i}")
            if "OK" in resp or "+OK" in resp:
                keys_written += 1

    check(f"Wrote {keys_written}/10 keys", keys_written == 10)

    if keys_written > 0:
        time.sleep(0.5)
        for node in NODES:
            resp = send_redis_cmd("127.0.0.1", node["resp_port"], "GET", "key_5")
            check(f"{node['id']} GET key_5 -> {resp}", "value_5" in resp)

    print()

    # Test 5: DELETE replication
    print("--- Test 5: DELETE Replication ---")
    if leader:
        resp = send_redis_cmd("127.0.0.1", leader["resp_port"], "DEL", "key_5")
        check(f"Leader DEL key_5 -> {resp}", "1" in resp or ":1" in resp)
        time.sleep(0.5)
        for node in NODES:
            resp = send_redis_cmd("127.0.0.1", node["resp_port"], "GET", "key_5")
            check(f"{node['id']} GET key_5 (after DEL) -> {resp}", "raft_works" not in resp and "value_5" not in resp)

    print()

    # Test 6: Metrics endpoint
    print("--- Test 6: Prometheus Metrics ---")
    for node in NODES:
        body = http_get(f"http://127.0.0.1:{node['metrics_port']}/metrics", timeout=2)
        has_metrics = body and "kv_puts_total" in body
        check(f"{node['id']} metrics endpoint", has_metrics)

    print()

    # Test 7: EXISTS
    print("--- Test 7: EXISTS ---")
    if leader:
        resp = send_redis_cmd("127.0.0.1", leader["resp_port"], "EXISTS", "key_0")
        check(f"Leader EXISTS key_0 -> {resp}", "1" in resp or ":1" in resp)
        resp = send_redis_cmd("127.0.0.1", leader["resp_port"], "EXISTS", "nonexistent")
        check(f"Leader EXISTS nonexistent -> {resp}", "0" in resp or ":0" in resp)

    print()

    # Summary
    print("=" * 60)
    print(f"  Results: {PASS} passed, {FAIL} failed")
    print("=" * 60)

    if cleanup and any_running is False:
        print()
        print("Stopping cluster nodes...")
        for p in processes:
            p.terminate()
        for p in processes:
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()

    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())