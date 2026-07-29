"""
KV Store Python 客户端驱动 — 通过 Redis RESP 协议与 kv_server 通信

用法:
  from kv_client import KVClient
  client = KVClient("127.0.0.1", 6379)
  client.set("hello", "world")
  value = client.get("hello")
  client.delete("hello")
"""

import socket
import struct
import re


class KVClient:
    """KV Store 客户端，兼容 Redis RESP 协议"""

    def __init__(self, host="127.0.0.1", port=6379, timeout=10.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._sock = None

    def connect(self):
        """建立连接"""
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