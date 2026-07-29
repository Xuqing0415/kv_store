"""
KV Store 一致性哈希分片客户端 — 将 key 映射到多个 kv_server 实例

用法:
  from kv_shard import ShardedKVClient
  shards = [
      ("127.0.0.1", 6379),
      ("127.0.0.1", 6380),
      ("127.0.0.1", 6381),
  ]
  client = ShardedKVClient(shards, virtual_nodes=150)
  client.set("user:1001", "Alice")
  value = client.get("user:1001")

原理:
  - 使用一致性哈希环将 key 映射到节点
  - 每个物理节点有 150 个虚拟节点（默认），保证均匀分布
  - 节点增删时只影响相邻节点，避免全量数据迁移
"""

import hashlib
import bisect
from kv_client import KVClient


class ConsistentHash:
    """一致性哈希环"""

    def __init__(self, nodes, virtual_nodes=150):
        """
        Args:
            nodes: 物理节点列表，如 [("host1", 6379), ("host2", 6380)]
            virtual_nodes: 每个物理节点的虚拟节点数（默认 150）
        """
        self.virtual_nodes = virtual_nodes
        self.ring = {}       # hash_pos → node_index
        self.sorted_keys = []  # 排序后的哈希位置列表
        self.nodes = nodes

        self._build_ring()

    def _hash(self, key):
        """MD5 哈希，返回 32 位整数"""
        h = hashlib.md5(str(key).encode()).hexdigest()
        return int(h, 16) & 0x7FFFFFFF

    def _build_ring(self):
        """构建哈希环"""
        self.ring.clear()
        for node_idx in range(len(self.nodes)):
            for v in range(self.virtual_nodes):
                vnode_key = f"node_{node_idx}_vnode_{v}"
                pos = self._hash(vnode_key)
                self.ring[pos] = node_idx
        self.sorted_keys = sorted(self.ring.keys())

    def get_node(self, key):
        """根据 key 获取目标节点索引"""
        if not self.ring:
            return None
        h = self._hash(key)
        # 二分查找第一个 >= h 的位置
        idx = bisect.bisect_left(self.sorted_keys, h)
        if idx == len(self.sorted_keys):
            idx = 0  # 环形，回到开头
        pos = self.sorted_keys[idx]
        return self.ring[pos]

    def add_node(self, host, port):
        """添加节点（需要重新构建环）"""
        self.nodes.append((host, port))
        self._build_ring()

    def remove_node(self, host, port):
        """移除节点（需要重新构建环）"""
        self.nodes = [n for n in self.nodes if n != (host, port)]
        self._build_ring()


class ShardedKVClient:
    """分片 KV 客户端 — 自动将请求路由到正确的节点"""

    def __init__(self, shard_nodes, virtual_nodes=150):
        """
        Args:
            shard_nodes: [(host, port), ...] 分片节点列表
            virtual_nodes: 虚拟节点数量
        """
        self.ch = ConsistentHash(shard_nodes, virtual_nodes)
        self.connections = {}  # node_idx → KVClient

    def _get_connection(self, node_idx):
        """获取或创建节点连接"""
        if node_idx not in self.connections:
            host, port = self.ch.nodes[node_idx]
            client = KVClient(host, port)
            client.connect()
            self.connections[node_idx] = client
        return self.connections[node_idx]

    def close(self):
        """关闭所有连接"""
        for client in self.connections.values():
            client.close()
        self.connections.clear()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # ===== 路由命令 =====

    def _route(self, key):
        """根据 key 路由到目标节点"""
        node_idx = self.ch.get_node(key)
        return self._get_connection(node_idx)

    def get(self, key):
        return self._route(key).get(key)

    def set(self, key, value):
        return self._route(key).set(key, value)

    def delete(self, key):
        return self._route(key).delete(key)

    def exists(self, key):
        return self._route(key).exists(key)

    # ===== 聚合命令（需要广播到所有节点） =====

    def dbsize(self):
        """所有节点的 key 总数"""
        total = 0
        for node_idx in range(len(self.ch.nodes)):
            client = self._get_connection(node_idx)
            result = client.dbsize()
            if isinstance(result, int):
                total += result
        return total

    def keys(self, pattern="*"):
        """所有节点的 key 列表（合并）"""
        all_keys = []
        for node_idx in range(len(self.ch.nodes)):
            client = self._get_connection(node_idx)
            result = client.keys(pattern)
            if isinstance(result, list):
                all_keys.extend(result)
        return all_keys

    def flushdb(self):
        """清空所有节点"""
        for node_idx in range(len(self.ch.nodes)):
            client = self._get_connection(node_idx)
            client.flushdb()

    def scan_all(self):
        """扫描所有节点的 key"""
        all_keys = []
        for node_idx in range(len(self.ch.nodes)):
            client = self._get_connection(node_idx)
            all_keys.extend(client.scan_all())
        return all_keys

    def get_distribution(self):
        """获取数据分布统计（每个节点的 key 数量）"""
        dist = {}
        for node_idx in range(len(self.ch.nodes)):
            host, port = self.ch.nodes[node_idx]
            client = self._get_connection(node_idx)
            result = client.dbsize()
            dist[f"{host}:{port}"] = result if isinstance(result, int) else 0
        return dist


# ===== 便捷函数 =====

def start_shard_cluster(base_port=6379, num_shards=3, data_dir_template="./data_shard_{}"):
    """启动分片集群（用于测试）

    需要在不同终端中启动每个 kv_server 实例

    示例:
        # 终端 1: kv_server -p 6379 -d ./data_shard_0
        # 终端 2: kv_server -p 6380 -d ./data_shard_1
        # 终端 3: kv_server -p 6381 -d ./data_shard_2
    """
    import subprocess
    import os

    processes = []
    for i in range(num_shards):
        port = base_port + i
        data_dir = data_dir_template.format(i)
        os.makedirs(data_dir, exist_ok=True)

        cmd = f"kv_server -h 127.0.0.1 -p {port} -d {data_dir}"
        print(f"  Starting shard {i}: {cmd}")
        proc = subprocess.Popen(cmd, shell=True)
        processes.append(proc)

    return processes


if __name__ == "__main__":
    # 简单自测：单节点兼容
    import sys
    import time

    # 单节点分片测试
    shards = [("127.0.0.1", 6379)]
    print(f"ShardedKVClient Test — {len(shards)} shard(s)")
    print("  (Make sure kv_server is running on port 6379)\n")

    try:
        with ShardedKVClient(shards) as c:
            print(f"  PING (via shard): {c._get_connection(0).ping()}")

            # 写入测试数据
            for i in range(100):
                c.set(f"user:{i}", f"value_{i}")

            print(f"  DBSIZE: {c.dbsize()}")
            print(f"  GET user:50: {c.get('user:50')}")
            print(f"  GET user:99: {c.get('user:99')}")

            dist = c.get_distribution()
            print(f"  Distribution: {dist}")

            c.flushdb()
            print(f"  After FLUSHDB: {c.dbsize()}")

    except ConnectionRefusedError:
        print("  ERROR: Cannot connect to kv_server on port 6379")
        print("  Start it with: kv_server -h 127.0.0.1 -p 6379")
    except Exception as e:
        print(f"  ERROR: {e}")