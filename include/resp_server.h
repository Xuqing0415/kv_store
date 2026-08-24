#ifndef RESP_SERVER_H
#define RESP_SERVER_H

#include "kv_store.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Redis RESP 协议服务器配置 */
#define RESP_DEFAULT_PORT    6379
#define RESP_MAX_CLIENTS     32
#define RESP_MAX_CMD_LEN     (1024 * 1024)  /* 1MB max command */
#define RESP_IO_BUF_SIZE     (64 * 1024)     /* 64KB I/O buffer */
#define RESP_REPLY_BUF_SIZE  (1024 * 1024)   /* 1MB reply buffer */

/* 健康检查回调：用于 RESP HEALTH 命令获取 Raft 状态
 * 返回写入 buf 的字节数，或 -1 表示失败 */
typedef int (*resp_health_cb)(void* ctx, char* buf, size_t buf_size);

/* 服务器句柄 */
typedef struct resp_server resp_server_t;

/* Raft 写提议函数指针（由 raft_node.c 注入 raft_propose，避免 resp_server
 * 在单机模式（kv_server）下链接 raft.c 产生未解析符号）。
 * 返回 0 表示提议已提交成功，非 0 表示失败。 */
typedef int (*resp_raft_propose_fn)(void* raft, uint8_t type,
                                    const char* key, size_t key_len,
                                    const char* value, size_t value_len);

/* 设置 Raft 句柄与写提议函数，启用集群写复制模式。
 * 启用后 SET/DEL 经提议函数复制；raft 为 NULL 表示单机模式（直接写本地）。 */
void resp_server_set_raft(resp_server_t* server, void* raft, resp_raft_propose_fn propose_fn);

/* 启动 RESP 服务器，返回 0 成功，-1 失败 */
int resp_server_start(resp_server_t** out_server, const char* host, int port, kv_store_t* db);

/* 设置健康检查回调（用于 Raft 模式下返回集群状态） */
void resp_server_set_health_cb(resp_server_t* server, resp_health_cb cb, void* ctx);

/* 运行服务器主循环（阻塞，直到收到停止信号） */
void resp_server_run(resp_server_t* server);

/* 停止服务器 */
void resp_server_stop(resp_server_t* server);

/* 销毁服务器资源 */
void resp_server_free(resp_server_t* server);

#ifdef __cplusplus
}
#endif

#endif /* RESP_SERVER_H */