#ifndef METRICS_SERVER_H
#define METRICS_SERVER_H

#include "kv_store.h"

/* 前向声明，避免循环依赖 */
struct raft;
typedef struct raft raft_t;

#ifdef __cplusplus
extern "C" {
#endif

#define METRICS_DEFAULT_PORT 9090

typedef struct metrics_server metrics_server_t;

/* 启动 metrics HTTP 服务器，返回 0 成功，-1 失败
 * raft 参数可选（传 NULL 则只暴露 KV 指标） */
int metrics_server_start(metrics_server_t** out_server, const char* host, int port,
                         kv_store_t* db, raft_t* raft);

/* 运行服务器主循环（阻塞，直到收到停止信号） */
void metrics_server_run(metrics_server_t* server);

/* 停止服务器 */
void metrics_server_stop(metrics_server_t* server);

/* 销毁服务器资源 */
void metrics_server_free(metrics_server_t* server);

#ifdef __cplusplus
}
#endif
#endif