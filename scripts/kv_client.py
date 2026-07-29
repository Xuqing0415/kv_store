"""
KV Store Python 客户端驱动 — 通过 Redis RESP 协议与 kv_server / kv_raft 通信

用法:
  # 单节点模式
  from kv_client import KVClient
  client = KVClient("127.0.0.1", 6379)
  client.set("hello", "world")
  value = client.get("hello")

  # 高可用集群模式（自动感知 Leader 切换）
  from kv_client import HAKVClient
  client = HAKVClient([
      ("127.0.0.1", 6379, 9091),  # (resp_host, resp_port, metrics_port)
      ("127.0.0.1", 6380, 9092),
      ("127.0.0.1", 6381, 9093),
  ])
  client.set("hello", "world")  # 自动找到 Leader 并写入
  value = client.get("hello")   # 可从任意节点读取
"""

import socket
import struct
import re
import time
import random
import urllib.request
import threading
from contextlib import contextmanager


class KVClient:
    """KV Store 单节点客户端，兼容 Redis RESP 协议"""

    def __init__(self, host="127.0.0.1", port=6379, timeout=10.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._sock = None

    def connect(self):
        """建立连接"""
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(self.timeout)
        self._sock.connect((self.host, self.port))

    def close(self):
        """关闭连接"""
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    def is_connected(self):
        """检查是否已连接"""
        return self._sock is not None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    # ===== RESP 协议编解码 =====

    def _send_command(self, *args):
        """发送 RESP 数组命令"""
        cmd = f"*{len(args)}\r\n"
        for arg in args:
            arg_str = str(arg)
            cmd += f"${len(arg_str)}\r\n{arg_str}\r\n"
        self._sock.sendall(cmd.encode())

    def _read_line(self):
        """读取一行（以 \r\n 结尾）"""
        buf = b""
        while not buf.endswith(b"\r\n"):
            chunk = self._sock.recv(1)
            if not chunk:
                break
            buf += chunk
        return buf.rstrip(b"\r\n")

    def _read_response(self):
        """读取 RESP 响应"""
        line = self._read_line()
        if not line:
            return None

        prefix = line[0:1]
        data = line[1:]

        if prefix == b"+":
            # 简单字符串
            return data.decode("utf-8", errors="replace")
        elif prefix == b"-":
            # 错误
            return {"error": data.decode("utf-8", errors="replace")}
        elif prefix == b":":
            # 整数
            return int(data)
        elif prefix == b"$":
            # 批量字符串
            length = int(data)
            if length == -1:
                return None
            if length == 0:
                self._read_line()  # consume \r\n after empty string
                return ""
            buf = self._sock.recv(length + 2)
            return buf[:length].decode("utf-8", errors="replace")
        elif prefix == b"*":
            # 数组
            count = int(data)
            if count == -1:
                return None
            return [self._read_response() for _ in range(count)]
        else:
            return data.decode("utf-8", errors="replace")

    # ===== 命令封装 =====

    def ping(self):
        """PING 命令"""
        self._send_command("PING")
        return self._read_response()

    def set(self, key, value):
        """SET key value"""
        self._send_command("SET", key, value)
        return self._read_response()

    def get(self, key):
        """GET key"""
        self._send_command("GET", key)
        return self._read_response()

    def delete(self, *keys):
        """DEL key [key ...]"""
        self._send_command("DEL", *keys)
        return self._read_response()

    def exists(self, *keys):
        """EXISTS key [key ...]"""
        self._send_command("EXISTS", *keys)
        return self._read_response()

    def keys(self, pattern="*"):
        """KEYS pattern"""
        self._send_command("KEYS", pattern)
        return self._read_response()

    def scan(self, cursor=0):
        """SCAN cursor"""
        self._send_command("SCAN", cursor)
        return self._read_response()

    def dbsize(self):
        """DBSIZE"""
        self._send_command("DBSIZE")
        return self._read_response()

    def flushdb(self):
        """FLUSHDB"""
        self._send_command("FLUSHDB")
        return self._read_response()

    def health(self):
        """HEALTH — 获取节点健康状态"""
        self._send_command("HEALTH")
        return self._read_response()

    def scan_all(self, batch_size=100):
        """扫描所有 key（封装 SCAN 逻辑）"""
        cursor = 0
        all_keys = []
        while True:
            result = self.scan(cursor)
            if isinstance(result, list) and len(result) >= 2:
                cursor = int(result[0])
                all_keys.extend(result[1])
            else:
                break
            if cursor == 0:
                break
        return all_keys


# ================================================================
# 高可用集群客户端 — 自动感知 Leader 切换
# ================================================================

class HAKVClient:
    """高可用 KV Raft 集群客户端

    特性：
    - 自动发现 Leader：通过 /metrics 端点或写探测定位当前 Leader
    - 自动重试：写入遇到连接错误时，重新发现 Leader 并重试
    - Leader 缓存：缓存 Leader 地址，避免每次写入都探测
    - 读负载均衡：GET 请求可从任意节点读取（含本地缓存）

    用法:
        client = HAKVClient([
            ("127.0.0.1", 6379, 9091),
            ("127.0.0.1", 6380, 9092),
            ("127.0.0.1", 6381, 9093),
        ])
        client.set("key", "value")
        value = client.get("key")
    """

    def __init__(self, nodes, timeout=10.0, leader_cache_ttl=5.0):
        """
        Args:
            nodes: list of (resp_host, resp_port, metrics_port) tuples
            timeout: connection/read timeout in seconds
            leader_cache_ttl: leader cache TTL in seconds
        """
        self.nodes = nodes  # [(host, resp_port, metrics_port), ...]
        self.timeout = timeout
        self.leader_cache_ttl = leader_cache_ttl

        # Leader 缓存
        self._leader_node = None     # (host, resp_port, metrics_port)
        self._leader_found_at = 0.0

        # 连接池: {(host, port): KVClient}
        self._pool = {}
        self._pool_lock = threading.Lock()

        # 统计
        self.leader_switches = 0
        self.retry_count = 0
        self.write_count = 0
        self.read_count = 0

    def close(self):
        """关闭所有连接"""
        with self._pool_lock:
            for c in self._pool.values():
                c.close()
            self._pool.clear()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # ===== Leader 发现 =====

    def _discover_leader_via_metrics(self):
        """通过 /metrics 端点发现 Leader（最快、最可靠）"""
        for node in self.nodes:
            host, resp_port, metrics_port = node
            try:
                url = f"http://{host}:{metrics_port}/metrics"
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req, timeout=2) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                    for line in body.split("\n"):
                        if line.startswith("raft_is_leader ") or line.startswith("raft_is_leader\t"):
                            parts = line.split()
                            if len(parts) >= 2 and parts[1] == "1":
                                return node
            except Exception:
                continue
        return None

    def _discover_leader_via_write(self):
        """通过尝试写入来发现 Leader（兜底方案）"""
        probe_key = f"__ha_probe_{random.randint(0, 999999)}"
        for node in self.nodes:
            host, resp_port, _ = node
            try:
                c = self._get_connection(host, resp_port)
                c._send_command("SET", probe_key, "1")
                resp = c._read_response()
                if isinstance(resp, str) and "OK" in resp:
                    return node
            except Exception:
                continue
        return None

    def _find_leader(self, force=False):
        """查找当前 Leader

        Args:
            force: 如果为 True，强制重新发现（忽略缓存）

        Returns:
            (host, resp_port, metrics_port) 或 None
        """
        now = time.time()

        # 检查缓存
        if not force and self._leader_node is not None:
            if now - self._leader_found_at < self.leader_cache_ttl:
                return self._leader_node

        # 优先通过 /metrics 发现
        leader = self._discover_leader_via_metrics()
        if leader:
            self._update_leader_cache(leader)
            return leader

        # 兜底：通过写探测
        leader = self._discover_leader_via_write()
        if leader:
            self._update_leader_cache(leader)
            return leader

        return None

    def _update_leader_cache(self, node):
        """更新 Leader 缓存"""
        if self._leader_node is None or self._leader_node != node:
            if self._leader_node is not None:
                self.leader_switches += 1
        self._leader_node = node
        self._leader_found_at = time.time()

    def _invalidate_leader_cache(self):
        """使 Leader 缓存失效"""
        self._leader_node = None
        self._leader_found_at = 0.0

    # ===== 连接管理 =====

    def _get_connection(self, host, port):
        """获取或创建到指定节点的连接"""
        key = (host, port)
        with self._pool_lock:
            if key in self._pool:
                c = self._pool[key]
                if c.is_connected():
                    return c
                # 连接已断开，重新创建
                c.close()
                del self._pool[key]

            c = KVClient(host, port, timeout=self.timeout)
            try:
                c.connect()
            except Exception:
                return None
            self._pool[key] = c
            return c

    def _get_leader_connection(self):
        """获取到 Leader 的连接（自动发现）"""
        # 尝试使用缓存的 Leader
        if self._leader_node is not None:
            host, resp_port, _ = self._leader_node
            c = self._get_connection(host, resp_port)
            if c:
                return c, self._leader_node

        # 重新发现 Leader
        leader = self._find_leader(force=True)
        if leader:
            host, resp_port, _ = leader
            c = self._get_connection(host, resp_port)
            if c:
                return c, leader

        return None, None

    def _get_any_connection(self):
        """获取到任意可用节点的连接（用于读操作）"""
        shuffled = list(self.nodes)
        random.shuffle(shuffled)
        for node in shuffled:
            host, resp_port, _ = node
            c = self._get_connection(host, resp_port)
            if c:
                return c, node
        return None, None

    # ===== 核心 API =====

    def _execute_write(self, *args, max_retries=3):
        """执行写操作，自动处理 Leader 切换

        Args:
            *args: Redis 命令参数
            max_retries: 最大重试次数

        Returns:
            RESP 响应值
        """
        last_error = None
        for attempt in range(max_retries):
            c, leader = self._get_leader_connection()
            if not c:
                last_error = "No leader available"
                # 指数退避: 100ms, 200ms, 400ms, ...
                time.sleep(0.1 * (2 ** attempt))
                continue

            try:
                c._send_command(*args)
                resp = c._read_response()

                if isinstance(resp, dict) and "error" in resp:
                    err = resp["error"]
                    # 检查是否是重定向错误
                    if "REDIRECT" in err.upper() or "MOVED" in err.upper() or "NOT LEADER" in err.upper():
                        self._invalidate_leader_cache()
                        self.retry_count += 1
                        time.sleep(0.1 * (2 ** attempt))
                        continue
                    return resp

                return resp

            except (socket.timeout, ConnectionRefusedError, ConnectionResetError,
                    BrokenPipeError, OSError) as e:
                last_error = str(e)
                self._invalidate_leader_cache()
                if c:
                    c.close()
                self.retry_count += 1
                time.sleep(0.1 * (2 ** attempt))
                continue

        return {"error": f"Write failed after {max_retries} retries: {last_error}"}

    def _execute_read(self, *args, max_retries=2):
        """执行读操作，可从任意节点读取"""
        last_error = None
        for attempt in range(max_retries):
            c, node = self._get_any_connection()
            if not c:
                last_error = "No node available"
                time.sleep(0.1 * (2 ** attempt))
                continue

            try:
                c._send_command(*args)
                resp = c._read_response()
                return resp
            except (socket.timeout, ConnectionRefusedError, ConnectionResetError,
                    BrokenPipeError, OSError) as e:
                last_error = str(e)
                if c:
                    c.close()
                time.sleep(0.1 * (2 ** attempt))
                continue

        return {"error": f"Read failed after {max_retries} retries: {last_error}"}

    # ===== 命令封装 =====

    def ping(self):
        """PING — 向 Leader 发送"""
        c, leader = self._get_leader_connection()
        if not c:
            return {"error": "No leader available"}
        try:
            c._send_command("PING")
            return c._read_response()
        except Exception:
            return {"error": "PING failed"}

    def set(self, key, value):
        """SET key value — 自动路由到 Leader"""
        self.write_count += 1
        return self._execute_write("SET", key, value)

    def get(self, key):
        """GET key — 可从任意节点读取"""
        self.read_count += 1
        return self._execute_read("GET", key)

    def delete(self, *keys):
        """DEL key [key ...] — 自动路由到 Leader"""
        self.write_count += 1
        return self._execute_write("DEL", *keys)

    def exists(self, *keys):
        """EXISTS key [key ...]"""
        self.read_count += 1
        return self._execute_read("EXISTS", *keys)

    def keys(self, pattern="*"):
        """KEYS pattern"""
        self.read_count += 1
        return self._execute_read("KEYS", pattern)

    def scan(self, cursor=0):
        """SCAN cursor"""
        self.read_count += 1
        return self._execute_read("SCAN", cursor)

    def dbsize(self):
        """DBSIZE"""
        self.read_count += 1
        return self._execute_read("DBSIZE")

    def flushdb(self):
        """FLUSHDB"""
        self.write_count += 1
        return self._execute_write("FLUSHDB")

    def health(self):
        """HEALTH — 向 Leader 获取节点健康状态"""
        c, leader = self._get_leader_connection()
        if not c:
            return {"error": "No leader available"}
        try:
            c._send_command("HEALTH")
            return c._read_response()
        except Exception as e:
            return {"error": str(e)}

    def scan_all(self, batch_size=100):
        """扫描所有 key（封装 SCAN 逻辑）"""
        cursor = 0
        all_keys = []
        while True:
            result = self.scan(cursor)
            if isinstance(result, list) and len(result) >= 2:
                cursor = int(result[0])
                all_keys.extend(result[1])
            else:
                break
            if cursor == 0:
                break
        return all_keys

    # ===== 集群状态 =====

    def get_leader_info(self):
        """获取当前 Leader 信息"""
        leader = self._find_leader()
        if leader:
            return {
                "host": leader[0],
                "resp_port": leader[1],
                "metrics_port": leader[2],
                "cached_since": self._leader_found_at,
            }
        return None

    def get_stats(self):
        """获取客户端统计信息"""
        return {
            "leader_switches": self.leader_switches,
            "retry_count": self.retry_count,
            "write_count": self.write_count,
            "read_count": self.read_count,
            "pool_size": len(self._pool),
            "leader": self.get_leader_info(),
        }

    def get_cluster_metrics(self):
        """获取所有节点的 /metrics 数据"""
        results = {}
        for node in self.nodes:
            host, resp_port, metrics_port = node
            try:
                url = f"http://{host}:{metrics_port}/metrics"
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req, timeout=2) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                    metrics = {}
                    for line in body.split("\n"):
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        parts = line.split()
                        if len(parts) >= 2:
                            try:
                                metrics[parts[0]] = float(parts[1])
                            except ValueError:
                                pass
                    results[f"{host}:{resp_port}"] = metrics
            except Exception as e:
                results[f"{host}:{resp_port}"] = {"error": str(e)}
        return results

    def health_check(self):
        """通过 HTTP /health 端点获取所有节点的健康状态（JSON格式）"""
        import json
        results = {}
        for node in self.nodes:
            host, resp_port, metrics_port = node
            try:
                url = f"http://{host}:{metrics_port}/health"
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req, timeout=2) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                    results[f"{host}:{resp_port}"] = json.loads(body)
            except Exception as e:
                results[f"{host}:{resp_port}"] = {"error": str(e)}
        return results


# ===== 便捷函数 =====

def benchmark_set_get(client, num_ops=1000):
    """简单性能测试：SET + GET 延迟统计"""
    import time

    # SET 测试
    start = time.time()
    for i in range(num_ops):
        client.set(f"bench_key_{i}", f"bench_value_{i}")
    set_time = time.time() - start
    set_ops = num_ops / set_time if set_time > 0 else 0

    # GET 测试
    start = time.time()
    hits = 0
    for i in range(num_ops):
        val = client.get(f"bench_key_{i}")
        if val is not None:
            hits += 1
    get_time = time.time() - start
    get_ops = num_ops / get_time if get_time > 0 else 0

    return {
        "set_ops": num_ops,
        "set_time": set_time,
        "set_ops_per_sec": set_ops,
        "get_ops": num_ops,
        "get_time": get_time,
        "get_ops_per_sec": get_ops,
        "get_hits": hits,
    }


if __name__ == "__main__":
    # 简单自测
    import sys

    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 6379

    print(f"KV Store Client — connecting to {host}:{port}")
    with KVClient(host, port) as c:
        print(f"  PING: {c.ping()}")

        c.set("hello", "world")
        print(f"  SET hello=world: OK")

        val = c.get("hello")
        print(f"  GET hello: {val}")

        print(f"  EXISTS hello: {c.exists('hello')}")
        print(f"  EXISTS nonexit: {c.exists('nonexit')}")

        c.set("key1", "val1")
        c.set("key2", "val2")
        print(f"  KEYS *: {c.keys('*')}")
        print(f"  DBSIZE: {c.dbsize()}")

        print(f"  DEL hello: {c.delete('hello')}")
        print(f"  GET hello (after del): {c.get('hello')}")

        print(f"\n  Benchmarking 1000 SET/GET...")
        result = benchmark_set_get(c, 1000)
        print(f"  SET: {result['set_ops_per_sec']:.0f} ops/s")
        print(f"  GET: {result['get_ops_per_sec']:.0f} ops/s ({result['get_hits']}/{result['get_ops']} hits)")