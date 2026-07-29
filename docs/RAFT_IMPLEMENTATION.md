# Raft 共识协议实现文档

## 协议概述

Raft 是一种用于管理复制日志的共识算法。它将共识问题分解为三个相对独立的子问题：**Leader 选举**、**日志复制**和**安全性**。

本实现严格遵循 Raft 论文（In Search of an Understandable Consensus Algorithm），并在此基础上增加了**快照（Snapshot）**和**日志压缩（Log Compaction）**机制。

## 核心数据结构

### Raft 节点状态

```c
typedef struct raft {
    // 持久化状态（所有节点）
    uint64_t current_term;          // 当前任期
    char     voted_for[ID_LEN];     // 本任期投票给谁
    raft_log_entry_t* log;          // 日志数组
    size_t   log_count;             // 日志条目数

    // 易失状态（所有节点）
    uint64_t commit_index;          // 已提交的最高日志索引
    uint64_t last_applied;          // 已应用到状态机的最高索引

    // Leader 状态
    uint64_t* next_index;           // 下一个要发送给每个 peer 的日志索引
    uint64_t* match_index;          // 每个 peer 已复制的最高日志索引

    // 快照
    uint64_t snapshot_index;        // 快照最后包含的索引
    uint64_t snapshot_term;         // 快照最后包含的 term
} raft_t;
```

### 日志条目

```c
typedef struct {
    uint64_t term;      // 条目所属任期
    uint64_t index;     // 条目索引
    uint8_t  type;      // 操作类型（PUT/DELETE）
    size_t   key_len;   // 键长度
    char*    key;       // 键
    size_t   value_len; // 值长度
    char*    value;     // 值
} raft_log_entry_t;
```

## Leader 选举

### 状态机

```
                    ┌──────────────┐
       启动时        │              │
    ┌──────────────▶│   Follower   │◀──────────────┐
    │               │              │               │
    │               └──────┬───────┘               │
    │                      │                       │
    │          选举超时，发起选举                     │ 发现更高 term
    │                      │                       │
    │                      ▼                       │
    │               ┌──────────────┐               │
    │               │  Candidate   │               │
    │               │              │───────────────┘
    │               └──────┬───────┘
    │                      │
    │          获得多数票      │
    │                      │
    │                      ▼
    │               ┌──────────────┐
    └───────────────│   Leader     │
       发现更高 term  │              │
                    └──────────────┘
```

### 超时机制

- **选举超时**：随机 150-300ms，避免分裂投票
- **心跳间隔**：50ms，Leader 定期发送 AppendEntries 维持权威
- **选举计时器**：每次收到 AppendEntries 或投票请求时重置

### 投票规则

1. 候选人的日志至少和投票者一样新（比较 lastLogTerm 和 lastLogIndex）
2. 每个任期内最多投一票
3. 如果候选人 term 比当前 term 小，拒绝投票

## 日志复制

### AppendEntries RPC

```
Leader                            Follower
  │                                  │
  │  AppendEntries(term, prevLogIndex, │
  │    prevLogTerm, entries[],        │
  │    leaderCommit)                  │
  │─────────────────────────────────▶│
  │                                  │
  │  Response(term, success,         │
  │    lastLogIndex, need_snapshot)  │
  │◀─────────────────────────────────│
```

### 复制流程

1. **客户端写入**：Leader 将命令追加到本地日志
2. **并行复制**：向所有 Follower 发送 AppendEntries
3. **多数派确认**：收到多数派成功响应后，增加 commit_index
4. **应用到状态机**：将 committed 条目应用到 kv_store
5. **响应客户端**：返回操作结果

### 日志一致性保证

- **日志匹配特性**：如果两个日志在相同索引有相同 term，则之前所有条目都相同
- **冲突检测**：AppendEntries 检查 prevLogIndex 和 prevLogTerm
- **冲突解决**：发生冲突时，Leader 递减 next_index 并重试，直到找到一致点

## 快照与日志压缩

### 触发条件

日志条目数超过 10,000 条时自动触发快照，或手动调用 `raft_snapshot_create()`。

### 快照流程

```
Leader                            Follower
  │                                  │
  │  1. 创建快照（kv_store 备份）       │
  │  2. 截断日志（删除 snapshot_index   │
  │     之前的条目）                    │
  │                                  │
  │  InstallSnapshot(term,           │
  │    lastIncludedIndex,            │
  │    lastIncludedTerm,             │
  │    offset, data[], done)         │
  │─────────────────────────────────▶│
  │                                  │
  │                                  │  3. 保存快照到临时文件
  │                                  │  4. 应用快照到状态机
  │                                  │  5. 截断日志
  │                                  │
  │  Response(term)                  │
  │◀─────────────────────────────────│
```

### 快照分块传输

- **块大小**：512KB/块
- **传输方式**：逐块发送，最后一块标记 `done=true`
- **断点续传**：通过 `offset` 字段支持
- **临时文件**：接收时先写入临时文件，完成后再 rename

### 快照恢复

启动时检测快照文件，如果存在则：
1. 加载快照到状态机
2. 设置 `snapshot_index` 和 `snapshot_term`
3. 从快照索引之后开始重放日志

## 日志持久化

### 文件格式

```
┌────────────────────────────────────┐
│  Magic: "RAFT" (4 bytes)           │
│  Version: 3 (4 bytes)              │
│  snapshot_index (8 bytes)          │
│  snapshot_term (8 bytes)           │
│  snapshot_path_len (4 bytes)       │
│  snapshot_path (variable)          │
│  snapshot_size_bytes (8 bytes)     │
│  log_size_bytes (8 bytes)          │
│  current_term (8 bytes)            │
│  voted_for_len (4 bytes)           │
│  voted_for (variable)              │
│  log_count (8 bytes)               │
│  log entries...                    │
│    - term (8 bytes)                │
│    - index (8 bytes)               │
│    - type (1 byte)                 │
│    - key_len (8 bytes)             │
│    - key (variable)                │
│    - value_len (8 bytes)           │
│    - value (variable)              │
└────────────────────────────────────┘
```

### 持久化时机

- **currentTerm 和 votedFor**：每次变化时立即持久化
- **日志条目**：每次追加后立即持久化（Leader 和 Follower 都是）
- **快照元数据**：快照创建后持久化

## WAL 与 Raft 日志集成

在 Raft 模式下，kv_store 不再使用独立的 WAL：

- `kv_open_raft()` 创建无 WAL 的存储引擎
- `kv_put_internal()` / `kv_delete_internal()` 跳过 WAL 写入
- 崩溃恢复由 Raft 日志重放完成
- 消除了双写问题，写放大从 2x 降低到 1x

## 安全性保证

| 属性 | 保证方式 |
|------|---------|
| 选举安全 | 每个任期最多一个 Leader |
| 日志匹配 | AppendEntries 的 prevLogIndex/prevLogTerm 检查 |
| Leader 完整性 | Leader 的日志包含所有已提交条目 |
| 状态机安全 | 只有 committed 条目被 apply |

## 性能特征

| 指标 | 数值 |
|------|------|
| 选举超时 | 150-300ms |
| 心跳间隔 | 50ms |
| 日志条目最大数量 | 10,000（触发快照前） |
| 快照块大小 | 512KB |
| RPC 最大消息大小 | 1MB |

## 测试覆盖

- **单元测试**：`test_raft` — 选举、日志复制、持久化
- **快照测试**：`test_snapshot.py` — 10 个场景（大规模写入、节点恢复、崩溃恢复）
- **故障注入**：`test_fault_injection.py` — Leader 宕机、网络分区、数据一致性
- **混沌测试**：`test_chaos_100k.py` — 10 万条写入 + 随机 kill 节点
- **线性一致性**：`test_jepsen.py` — WGL 算法校验