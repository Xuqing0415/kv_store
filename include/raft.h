#ifndef RAFT_H
#define RAFT_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ================================================================
 * Raft 共识算法核心模块
 *
 * 实现 Raft 论文 (Ongaro 2014) 的核心协议：
 *   - Leader Election (随机超时 + RequestVote)
 *   - Log Replication (AppendEntries + 心跳)
 *   - 日志持久化 + 状态机 apply
 *
 * 集群规模：3 节点（可扩展至 5 节点）
 * ================================================================ */

/* --- 节点角色 --- */
typedef enum {
    RAFT_FOLLOWER  = 0,
    RAFT_CANDIDATE = 1,
    RAFT_LEADER    = 2,
} raft_role_t;

/* --- 单条日志条目 --- */
typedef struct {
    uint64_t term;        /* 创建该条目时的 term */
    uint64_t index;       /* 日志索引（从 1 开始） */
    uint8_t  type;        /* 0=PUT, 1=DELETE */
    char*    key;         /* 键 */
    size_t   key_len;
    char*    value;       /* 值（DELETE 时为空） */
    size_t   value_len;
} raft_entry_t;

/* --- Raft 节点配置 --- */
#define RAFT_NODE_ID_LEN    32
#define RAFT_MAX_NODES       5
#define RAFT_ELECTION_TIMEOUT_MIN_MS  150
#define RAFT_ELECTION_TIMEOUT_MAX_MS  300
#define RAFT_HEARTBEAT_INTERVAL_MS     50
#define RAFT_RPC_TIMEOUT_MS           200

typedef struct {
    char id[RAFT_NODE_ID_LEN];    /* 节点 ID，如 "node1" */
    char host[64];                 /* 主机地址 */
    int  port;                     /* Raft RPC 端口 */
} raft_peer_t;

typedef struct {
    char      node_id[RAFT_NODE_ID_LEN];  /* 本节点 ID */
    int       listen_port;                 /* 本节点监听端口 */
    int       num_peers;                   /* 集群节点数 */
    raft_peer_t peers[RAFT_MAX_NODES];    /* 所有节点（含自身） */
    char      data_dir[256];              /* 持久化目录 */
} raft_config_t;

/* --- 状态机回调 --- */
/* 将已提交的日志条目应用到业务状态机 */
typedef int (*raft_apply_cb)(void* state, raft_entry_t* entry);

/* --- Raft 句柄（不透明） --- */
typedef struct raft raft_t;

/* --- 核心 API --- */

/* 创建 Raft 节点 */
raft_t* raft_create(raft_config_t* cfg, void* state_machine, raft_apply_cb apply);

/* 销毁 Raft 节点 */
void raft_destroy(raft_t* r);

/* 启动 Raft 主循环（在独立线程中运行） */
int raft_start(raft_t* r);

/* 停止 Raft 节点 */
void raft_stop(raft_t* r);

/* 等待 Raft 线程退出 */
void raft_join(raft_t* r);

/* --- 客户端 API --- */

/* 向 Raft 集群提交写操作（阻塞直到 committed） */
int raft_propose(raft_t* r, uint8_t type, const char* key, size_t key_len,
                 const char* value, size_t value_len);

/* 查询当前是否为 Leader */
int raft_is_leader(raft_t* r);

/* 获取当前 Leader 的 peer 信息（用于客户端重定向） */
const raft_peer_t* raft_get_leader_peer(raft_t* r);

/* 获取当前节点角色名称 */
const char* raft_role_str(raft_role_t role);

/* 获取节点状态摘要（供调试/metrics） */
void raft_status(raft_t* r, uint64_t* out_term, raft_role_t* out_role,
                 uint64_t* out_commit_index, uint64_t* out_last_applied);

#ifdef __cplusplus
}
#endif
#endif /* RAFT_H */