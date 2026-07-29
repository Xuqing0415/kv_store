#include "raft.h"
#include "resp_server.h"
#include "metrics_server.h"
#include "kv_store.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#include <process.h>
#else
#include <signal.h>
#include <pthread.h>
#endif

/* ================================================================
 * Raft 状态机回调：将 committed 日志应用到 kv_store
 * ================================================================ */
static int raft_apply_to_kv(void* state, raft_entry_t* entry) {
    kv_store_t* db = (kv_store_t*)state;
    if (!db || !entry) return -1;

    if (entry->type == 0) { /* PUT */
        return kv_put(db, entry->key, entry->key_len, entry->value, entry->value_len);
    } else if (entry->type == 1) { /* DELETE */
        return kv_delete(db, entry->key, entry->key_len);
    }
    return -1;
}

/* ================================================================
 * 全局变量（信号处理）
 * ================================================================ */
static raft_t*          g_raft = NULL;
static resp_server_t*   g_resp = NULL;
static metrics_server_t* g_metrics = NULL;
static kv_store_t*      g_db = NULL;

#ifdef _WIN32
static HANDLE g_metrics_thread = NULL;
static HANDLE g_resp_thread = NULL;

static BOOL WINAPI signal_handler(DWORD ctrl_type) {
    (void)ctrl_type;
    printf("\n[INFO] Shutting down...\n");
    if (g_resp) resp_server_stop(g_resp);
    if (g_metrics) metrics_server_stop(g_metrics);
    if (g_raft) raft_stop(g_raft);
    return TRUE;
}

static DWORD WINAPI metrics_thread_func(LPVOID arg) {
    metrics_server_run((metrics_server_t*)arg);
    return 0;
}

static DWORD WINAPI resp_thread_func(LPVOID arg) {
    resp_server_run((resp_server_t*)arg);
    return 0;
}
#else
static pthread_t g_metrics_thread;
static pthread_t g_resp_thread;
static int g_metrics_started = 0;
static int g_resp_started = 0;

static void signal_handler(int sig) {
    (void)sig;
    printf("\n[INFO] Shutting down...\n");
    if (g_resp) resp_server_stop(g_resp);
    if (g_metrics) metrics_server_stop(g_metrics);
    if (g_raft) raft_stop(g_raft);
}

static void* metrics_thread_func(void* arg) {
    metrics_server_run((metrics_server_t*)arg);
    return NULL;
}

static void* resp_thread_func(void* arg) {
    resp_server_run((resp_server_t*)arg);
    return NULL;
}
#endif

/* ================================================================
 * 使用说明
 * ================================================================ */
static void print_usage(const char* prog) {
    printf("Usage: %s [options]\n", prog);
    printf("Raft Cluster Node — 3-node KV cluster with Raft consensus\n\n");
    printf("Options:\n");
    printf("  --id <name>      Node ID (e.g. node1, node2, node3)\n");
    printf("  --raft-port <p>   Raft RPC port (default: 8001)\n");
    printf("  --resp-port <p>   Redis RESP port (default: 6379)\n");
    printf("  --metrics-port <p> Prometheus metrics port (default: 9090, 0=disable)\n");
    printf("  --data-dir <dir>  Data directory (default: ./data)\n");
    printf("  --peer <spec>     Peer spec: node_id:host:port (repeatable)\n");
    printf("  --help            Show this help\n\n");
    printf("Example (3-node cluster):\n");
    printf("  # Node 1\n");
    printf("  %s --id node1 --raft-port 8001 --resp-port 6379 \\\n", prog);
    printf("     --peer node1:127.0.0.1:8001 --peer node2:127.0.0.1:8002 \\\n");
    printf("     --peer node3:127.0.0.1:8003 --data-dir ./cluster/node1\n\n");
    printf("  # Node 2\n");
    printf("  %s --id node2 --raft-port 8002 --resp-port 6380 \\\n", prog);
    printf("     --peer node1:127.0.0.1:8001 --peer node2:127.0.0.1:8002 \\\n");
    printf("     --peer node3:127.0.0.1:8003 --data-dir ./cluster/node2\n\n");
    printf("  # Node 3\n");
    printf("  %s --id node3 --raft-port 8003 --resp-port 6381 \\\n", prog);
    printf("     --peer node1:127.0.0.1:8001 --peer node2:127.0.0.1:8002 \\\n");
    printf("     --peer node3:127.0.0.1:8003 --data-dir ./cluster/node3\n");
}

/* ================================================================
 * 主函数
 * ================================================================ */
int main(int argc, char* argv[]) {
    /* 默认配置 */
    const char* node_id = "node1";
    int raft_port = 8001;
    int resp_port = 6379;
    int metrics_port = 9090;
    const char* data_dir = "./data";
    raft_peer_t peers[RAFT_MAX_NODES];
    int num_peers = 0;
    memset(peers, 0, sizeof(peers));

    /* 解析命令行参数 */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--id") == 0 && i + 1 < argc) {
            node_id = argv[++i];
        } else if (strcmp(argv[i], "--raft-port") == 0 && i + 1 < argc) {
            raft_port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--resp-port") == 0 && i + 1 < argc) {
            resp_port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--metrics-port") == 0 && i + 1 < argc) {
            metrics_port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (strcmp(argv[i], "--peer") == 0 && i + 1 < argc) {
            if (num_peers >= RAFT_MAX_NODES) {
                fprintf(stderr, "Too many peers (max %d)\n", RAFT_MAX_NODES);
                return 1;
            }
            char spec[256];
            strncpy(spec, argv[++i], sizeof(spec) - 1);
            spec[sizeof(spec) - 1] = '\0';

            /* 解析 node_id:host:port */
            char* id_part = strtok(spec, ":");
            char* host_part = strtok(NULL, ":");
            char* port_part = strtok(NULL, ":");

            if (!id_part || !host_part || !port_part) {
                fprintf(stderr, "Invalid peer spec: %s (expected node_id:host:port)\n", argv[i]);
                return 1;
            }

            snprintf(peers[num_peers].id, sizeof(peers[num_peers].id), "%s", id_part);
            snprintf(peers[num_peers].host, sizeof(peers[num_peers].host), "%s", host_part);
            peers[num_peers].port = atoi(port_part);
            num_peers++;
        } else if (strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            print_usage(argv[0]);
            return 1;
        }
    }

    if (num_peers < 2) {
        fprintf(stderr, "ERROR: At least 2 peers required (including self)\n");
        print_usage(argv[0]);
        return 1;
    }

    /* 验证自身在 peer 列表中 */
    int self_found = 0;
    for (int i = 0; i < num_peers; i++) {
        if (strcmp(peers[i].id, node_id) == 0) {
            self_found = 1;
            break;
        }
    }
    if (!self_found) {
        fprintf(stderr, "ERROR: Node '%s' not found in peer list\n", node_id);
        return 1;
    }

    printf("========================================\n");
    printf("  KV Store Raft Cluster Node\n");
    printf("========================================\n");
    printf("Node ID:       %s\n", node_id);
    printf("Raft Port:     %d\n", raft_port);
    printf("RESP Port:     %d\n", resp_port);
    printf("Metrics Port:  %d\n", metrics_port);
    printf("Data Dir:      %s\n", data_dir);
    printf("Peers (%d):\n", num_peers);
    for (int i = 0; i < num_peers; i++) {
        printf("  %s -> %s:%d\n", peers[i].id, peers[i].host, peers[i].port);
    }
    printf("\n");

    /* 打开 kv_store */
    g_db = kv_open(data_dir);
    if (!g_db) {
        fprintf(stderr, "Failed to open database at %s\n", data_dir);
        return 1;
    }
    printf("[INFO] KV Store opened at %s\n", data_dir);

    /* 创建 Raft 配置 */
    raft_config_t raft_cfg;
    memset(&raft_cfg, 0, sizeof(raft_cfg));
    snprintf(raft_cfg.node_id, sizeof(raft_cfg.node_id), "%s", node_id);
    raft_cfg.listen_port = raft_port;
    raft_cfg.num_peers = num_peers;
    memcpy(raft_cfg.peers, peers, (size_t)num_peers * sizeof(raft_peer_t));
    snprintf(raft_cfg.data_dir, sizeof(raft_cfg.data_dir), "%s", data_dir);

    /* 创建 Raft 节点 */
    g_raft = raft_create(&raft_cfg, g_db, raft_apply_to_kv);
    if (!g_raft) {
        fprintf(stderr, "Failed to create Raft node\n");
        kv_close(g_db);
        return 1;
    }
    printf("[INFO] Raft node created\n");

    /* 启动 Raft */
    if (raft_start(g_raft) != 0) {
        fprintf(stderr, "Failed to start Raft\n");
        raft_destroy(g_raft);
        kv_close(g_db);
        return 1;
    }
    printf("[INFO] Raft event loop started\n");

    /* 等待 Raft 选举完成（最多 5 秒） */
    printf("[INFO] Waiting for leader election...\n");
    int wait_count = 0;
    while (wait_count < 50) {
        uint64_t term;
        raft_role_t role;
        uint64_t commit_idx, last_applied;
        raft_status(g_raft, &term, &role, &commit_idx, &last_applied);

        if (role == RAFT_LEADER) {
            printf("[INFO] This node is LEADER (term=%llu)\n", (unsigned long long)term);
            break;
        }

        const raft_peer_t* leader = raft_get_leader_peer(g_raft);
        if (leader && leader->port > 0) {
            printf("[INFO] Leader is %s (%s:%d), I am %s\n",
                   leader->id, leader->host, leader->port,
                   raft_role_str(role));
        }

#ifdef _WIN32
        Sleep(100);
#else
        usleep(100000);
#endif
        wait_count++;
    }

    /* 启动 RESP 服务器（后台线程） */
    if (resp_server_start(&g_resp, "0.0.0.0", resp_port, g_db) != 0) {
        fprintf(stderr, "Failed to start RESP server on port %d\n", resp_port);
    } else {
#ifdef _WIN32
        g_resp_thread = CreateThread(NULL, 0, resp_thread_func, g_resp, 0, NULL);
#else
        if (pthread_create(&g_resp_thread, NULL, resp_thread_func, g_resp) == 0) {
            g_resp_started = 1;
        }
#endif
        printf("[INFO] RESP server listening on 0.0.0.0:%d\n", resp_port);
    }

    /* 启动 Metrics 服务器（后台线程） */
    if (metrics_port > 0) {
        if (metrics_server_start(&g_metrics, "0.0.0.0", metrics_port, g_db) != 0) {
            fprintf(stderr, "WARNING: Failed to start metrics server on port %d\n", metrics_port);
        } else {
#ifdef _WIN32
            g_metrics_thread = CreateThread(NULL, 0, metrics_thread_func, g_metrics, 0, NULL);
#else
            if (pthread_create(&g_metrics_thread, NULL, metrics_thread_func, g_metrics) == 0) {
                g_metrics_started = 1;
            }
#endif
            printf("[INFO] Metrics server listening on 0.0.0.0:%d\n", metrics_port);
        }
    }

    /* 注册信号处理 */
#ifdef _WIN32
    SetConsoleCtrlHandler(signal_handler, TRUE);
#else
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
#endif

    printf("\n========================================\n");
    printf("  Cluster node ready!\n");
    printf("  RESP:   redis-cli -h 127.0.0.1 -p %d\n", resp_port);
    if (metrics_port > 0) {
        printf("  Metrics: http://127.0.0.1:%d/metrics\n", metrics_port);
    }
    printf("  Press Ctrl+C to stop.\n");
    printf("========================================\n\n");

    /* 主线程等待（直到信号停止） */
    while (g_raft) {
        raft_status(g_raft, NULL, NULL, NULL, NULL);
#ifdef _WIN32
        Sleep(1000);
#else
        sleep(1);
#endif
    }

    /* 清理 */
    printf("[INFO] Cleaning up...\n");

    if (g_metrics) metrics_server_stop(g_metrics);
    if (g_resp) resp_server_stop(g_resp);
    if (g_raft) { raft_stop(g_raft); raft_join(g_raft); }

#ifdef _WIN32
    if (g_metrics_thread) {
        WaitForSingleObject(g_metrics_thread, 5000);
        CloseHandle(g_metrics_thread);
    }
    if (g_resp_thread) {
        WaitForSingleObject(g_resp_thread, 5000);
        CloseHandle(g_resp_thread);
    }
#else
    if (g_metrics_started) {
        pthread_join(g_metrics_thread, NULL);
    }
    if (g_resp_started) {
        pthread_join(g_resp_thread, NULL);
    }
#endif

    if (g_metrics) metrics_server_free(g_metrics);
    if (g_resp) resp_server_free(g_resp);
    if (g_raft) raft_destroy(g_raft);
    if (g_db) kv_close(g_db);

    printf("[INFO] Node stopped.\n");
    return 0;
}