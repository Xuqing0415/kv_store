# 性能调优指南

## 基准测试概览

kv_store 在标准硬件上的性能参考数据（30,000 条记录，每条 256 字节值，Release 编译）：

| 操作 | 吞吐量 | 说明 |
|------|--------|------|
| 顺序写入 | 50,000-150,000 ops/s | 受 MemTable 大小和压缩算法影响 |
| 随机读取 | 200,000-500,000 ops/s | 得益于 Bloom Filter + LRU 缓存 |
| 范围扫描 | 1,000,000+ ops/s | 顺序 I/O，SSTable 数据块前缀压缩 |
| 删除 | 100,000-300,000 ops/s | Tombstone 写入，性能接近写入 |

> 注：以上为单机（非 Raft 集群）基准数据。Raft 集群模式下，写入吞吐量约为单机的 1/3（需要多数派确认），读取吞吐量不变。

## 核心参数调优

### 1. MemTable 大小 (`MEMTABLE_SIZE_LIMIT`)

控制多久触发一次 MemTable 刷盘。默认 256KB，适合快速测试触发刷盘。

| 值 | 效果 | 适用场景 |
|----|------|---------|
| 256KB (默认) | 频繁刷盘，SSTable 文件多 | 开发测试，快速验证 |
| 1MB | 平衡刷盘频率和内存占用 | 中等吞吐 |
| 4MB | 写放大降低，批量写入提升 | 高吞吐写入 |
| 16MB | 大量写缓冲，但崩溃恢复 WAL 重放时间更长 | 批量导入 |

**建议**：写入密集型场景使用 4MB，读取密集型使用 1MB。

```bash
cmake .. -DMEMTABLE_SIZE_LIMIT=4194304
```

### 2. SSTable 块大小 (`SSTABLE_BLOCK_SIZE`)

影响单次磁盘读取的粒度。默认 4KB。

| 值 | 效果 | 适用场景 |
|----|------|---------|
| 4KB (默认) | 内存友好，缓存命中率高 | 点查询为主 |
| 16KB | 平衡点查和扫描 | 混合负载 |
| 64KB | 扫描快，但点查浪费内存 | 范围扫描为主 |

```bash
cmake .. -DSSTABLE_BLOCK_SIZE=16384
```

### 3. Bloom Filter 位/键 (`KV_BLOOM_BITS_PER_KEY`)

控制 Bloom Filter 的假阳性率。默认 10 bits/key。

| 值 | 假阳性率 | 内存开销 |
|----|---------|---------|
| 10 (默认) | ~1% | 1.25 bytes/key |
| 14 | ~0.1% | 1.75 bytes/key |
| 20 | ~0.01% | 2.5 bytes/key |

```bash
cmake .. -DKV_BLOOM_BITS_PER_KEY=14
```

### 4. 压缩算法

| 算法 | 压缩率 | 压缩速度 | 解压速度 | 适用场景 |
|------|--------|---------|---------|---------|
| Zstd (默认) | ~40% | 中等 | 快 | 磁盘空间敏感 |
| LZ4 | ~60% | 极快 | 极快 | 性能优先 |
| 无压缩 | 100% | 无开销 | 无开销 | 内存充足，SSD 直读 |

```bash
cmake .. -DKV_COMPRESSION=ZSTD   # 或 LZ4, NONE
```

### 5. Compaction 触发阈值

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `L0_FILE_LIMIT` | 4 | Level 0 文件数超过此值触发 Compaction |
| `L1_SIZE_LIMIT` | 10MB | Level 1 总大小超过此值触发 Compaction |
| 层级放大因子 | 10x | 每层大小是上一层的 10 倍 |

## 参数扫描工具

`scripts/param_sweep.py` 自动测试不同参数组合的性能：

```bash
# 快速模式（4 组参数）
python scripts/param_sweep.py --quick

# 完整模式（8 组参数）
python scripts/param_sweep.py

# 每组重复 3 次取平均
python scripts/param_sweep.py --repeat 3 --csv results.csv
```

输出 CSV 包含 `write_ops_per_sec`、`read_ops_per_sec`、`verify_errors` 等列，可直接用于分析。

## Raft 集群性能

### 写放大

Raft 模式下，每个写操作需要：
1. Leader 写入本地 Raft 日志
2. 通过网络复制到 Follower 的 Raft 日志
3. 多数派确认后提交
4. 应用到状态机（kv_store）

因此单次写入的网络往返次数为 1 次（AppendEntries RPC），写入延迟约为网络 RTT 的 1-2 倍。

### 优化建议

- **批量写入**：将多个 SET 合并为一个 pipeline 批量提交
- **Snapshot 频率**：日志条目数超过 10,000 条时触发快照，减少日志重放时间
- **网络拓扑**：节点间延迟越低越好，建议同机房部署

## 系统层调优

### Linux 内核参数

```bash
# 降低脏页回写阈值，减少 I/O 尖峰
sudo sysctl -w vm.dirty_background_ratio=5
sudo sysctl -w vm.dirty_ratio=20

# 提高文件描述符限制
ulimit -n 65536
```

### 磁盘

- **推荐**：NVMe SSD 或 SATA SSD
- **文件系统**：ext4（noatime 挂载选项）或 XFS
- **I/O 调度器**：none (NVMe) 或 mq-deadline (SATA SSD)

### 内存

- 建议至少 2GB 可用内存（含 OS 开销）
- APU 平台参考 [APU 调优指南](APU_TUNING.md)

## 编译优化

```bash
# Release 编译（推荐）
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS="-O3 -march=native"

# APU 平台：启用 O_DIRECT 绕过页缓存
cmake .. -DCMAKE_BUILD_TYPE=Release -DUSE_O_DIRECT=ON
```