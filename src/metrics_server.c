#include "metrics_server.h"
#include "raft.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #define close_socket(s) closesocket(s)
    #define socklen_t int
    #define SHUT_RDWR SD_BOTH
#else
    #include <sys/socket.h>
    #include <sys/select.h>
    #include <netinet/in.h>
    #include <netinet/tcp.h>
    #include <arpa/inet.h>
    #include <unistd.h>
    #include <fcntl.h>
    #include <signal.h>
    #define close_socket(s) close(s)
    #define SOCKET int
    #define INVALID_SOCKET (-1)
    #define SOCKET_ERROR (-1)
#endif

/* HTTP 请求缓冲区大小 */
#define HTTP_BUF_SIZE (16 * 1024)

/* 指标响应缓冲区大小 */
#define METRICS_BUF_SIZE (16 * 1024)

struct metrics_server {
    SOCKET listen_fd;
    kv_store_t* db;
    raft_t*     raft;
    int running;
    char* host;
    int port;
    time_t start_time;
#ifdef _WIN32
    int wsock_initialized;
#endif
};

/* 尝试从 socket 读取 HTTP 请求行，直到读到 \r\n\r\n 或缓冲区满 */
static int http_read_request(SOCKET fd, char* buf, size_t* buf_len, size_t buf_capacity) {
    *buf_len = 0;
    while (*buf_len < buf_capacity - 1) {
        int n = recv(fd, buf + *buf_len, (int)(buf_capacity - 1 - *buf_len), 0);
        if (n <= 0) {
            return -1; /* 连接关闭或错误 */
        }
        *buf_len += n;
        buf[*buf_len] = '\0';

        /* 检查是否收到完整的请求头（以 \r\n\r\n 结束） */
        if (strstr(buf, "\r\n\r\n") != NULL) {
            return 0;
        }
    }
    return -1; /* 请求太大 */
}

/* 解析 HTTP 请求行，提取方法（GET/POST）和路径 */
static int http_parse_request(const char* buf, size_t buf_len,
                               char* method, size_t method_cap,
                               char* path, size_t path_cap) {
    (void)buf_len;
    /* 解析第一行: METHOD SP PATH SP HTTP/1.x\r\n */
    const char* line_end = strstr(buf, "\r\n");
    if (!line_end) return -1;

    size_t line_len = (size_t)(line_end - buf);
    if (line_len > 4096) return -1; /* 安全限制 */

    char line[4096];
    size_t copy_len = line_len < sizeof(line) - 1 ? line_len : sizeof(line) - 1;
    memcpy(line, buf, copy_len);
    line[copy_len] = '\0';

    /* 提取 method */
    const char* sp1 = strchr(line, ' ');
    if (!sp1) return -1;
    size_t method_len = (size_t)(sp1 - line);
    if (method_len >= method_cap) return -1;
    memcpy(method, line, method_len);
    method[method_len] = '\0';

    /* 提取 path */
    const char* sp2 = strchr(sp1 + 1, ' ');
    if (sp2) {
        size_t path_len = (size_t)(sp2 - (sp1 + 1));
        if (path_len >= path_cap) return -1;
        memcpy(path, sp1 + 1, path_len);
        path[path_len] = '\0';
    } else {
        /* 没有第二段空格，path 就是到行尾 */
        size_t path_len = line_len - (size_t)(sp1 + 1 - line);
        if (path_len >= path_cap) return -1;
        memcpy(path, sp1 + 1, path_len);
        path[path_len] = '\0';
    }

    return 0;
}

/* 构建 Prometheus 文本格式的指标响应 */
static int metrics_build_response(metrics_server_t* server, time_t start_time,
                                   char* buf, size_t buf_capacity, size_t* out_len) {
    kv_metrics_snapshot_t snap;
    kv_metrics_snapshot(server->db, &snap);

    time_t now = time(NULL);
    long long uptime = (long long)(now - start_time);

    /* 获取 Raft 状态（如果可用） */
    uint64_t raft_term = 0;
    int raft_role = -1;  /* -1 = unknown */
    uint64_t raft_commit = 0;
    uint64_t raft_applied = 0;
    size_t raft_log_count = 0;
    int raft_is_leader = 0;
    uint64_t raft_snapshot_idx = 0;

    if (server->raft) {
        raft_role_t role;
        raft_status(server->raft, &raft_term, &role, &raft_commit, &raft_applied, &raft_log_count, NULL);
        raft_role = (int)role;
        raft_is_leader = (role == RAFT_LEADER) ? 1 : 0;

        /* 获取快照信息 */
        raft_snapshot_info_t snap_info;
        if (raft_get_snapshot_info(server->raft, &snap_info) == 0) {
            raft_snapshot_idx = snap_info.last_included_index;
        }
    }

    /* 使用 snprintf 构建完整响应 */
    int written = snprintf(buf, buf_capacity,
        /* 响应头 */
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/plain; version=0.0.4\r\n"
        "\r\n"
        /* Prometheus metrics */
        "# HELP kv_uptime_seconds Server uptime in seconds\n"
        "# TYPE kv_uptime_seconds gauge\n"
        "kv_uptime_seconds %lld\n"
        "\n"
        "# HELP kv_puts_total Total number of PUT operations\n"
        "# TYPE kv_puts_total counter\n"
        "kv_puts_total %lld\n"
        "\n"
        "# HELP kv_gets_total Total number of GET operations (hits)\n"
        "# TYPE kv_gets_total counter\n"
        "kv_gets_total %lld\n"
        "\n"
        "# HELP kv_get_misses_total Total number of GET misses\n"
        "# TYPE kv_get_misses_total counter\n"
        "kv_get_misses_total %lld\n"
        "\n"
        "# HELP kv_deletes_total Total number of DELETE operations\n"
        "# TYPE kv_deletes_total counter\n"
        "kv_deletes_total %lld\n"
        "\n"
        "# HELP kv_scans_total Total number of SCAN operations\n"
        "# TYPE kv_scans_total counter\n"
        "kv_scans_total %lld\n"
        "\n"
        "# HELP kv_compactions_total Total number of compaction runs\n"
        "# TYPE kv_compactions_total counter\n"
        "kv_compactions_total %lld\n"
        "\n"
        /* Raft metrics */
        "# HELP raft_state Current Raft state (0=Follower, 1=Candidate, 2=Leader, -1=unknown)\n"
        "# TYPE raft_state gauge\n"
        "raft_state %d\n"
        "\n"
        "# HELP raft_term Current Raft term\n"
        "# TYPE raft_term gauge\n"
        "raft_term %llu\n"
        "\n"
        "# HELP raft_commit_index Raft commit index\n"
        "# TYPE raft_commit_index gauge\n"
        "raft_commit_index %llu\n"
        "\n"
        "# HELP raft_last_applied Raft last applied index\n"
        "# TYPE raft_last_applied gauge\n"
        "raft_last_applied %llu\n"
        "\n"
        "# HELP raft_is_leader 1 if this node is the leader\n"
        "# TYPE raft_is_leader gauge\n"
        "raft_is_leader %d\n"
        "\n"
        "# HELP raft_log_count Number of committed log entries\n"
        "# TYPE raft_log_count gauge\n"
        "raft_log_count %zu\n"
        "\n"
        "# HELP raft_snapshot_index Last included index in snapshot (0=no snapshot)\n"
        "# TYPE raft_snapshot_index gauge\n"
        "raft_snapshot_index %llu\n",
        uptime,
        snap.puts_total,
        snap.gets_total,
        snap.get_misses_total,
        snap.deletes_total,
        snap.scans_total,
        snap.compactions_total,
        raft_role,
        (unsigned long long)raft_term,
        (unsigned long long)raft_commit,
        (unsigned long long)raft_applied,
        raft_is_leader,
        raft_log_count,
        (unsigned long long)raft_snapshot_idx
    );

    if (written < 0 || (size_t)written >= buf_capacity) {
        return -1;
    }

    *out_len = (size_t)written;
    return 0;
}

/* 构建 404 响应 */
static int metrics_build_404(char* buf, size_t buf_capacity, size_t* out_len) {
    int written = snprintf(buf, buf_capacity,
        "HTTP/1.1 404 Not Found\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: 13\r\n"
        "\r\n"
        "404 Not Found"
    );

    if (written < 0 || (size_t)written >= buf_capacity) {
        return -1;
    }

    *out_len = (size_t)written;
    return 0;
}

/* 构建 /health JSON 响应 */
static int metrics_build_health(metrics_server_t* server, char* buf, size_t buf_capacity, size_t* out_len) {
    kv_stats_t stats;
    kv_get_stats(server->db, &stats);

    /* 获取 Raft 状态 */
    const char* role_str = "standalone";
    uint64_t raft_term = 0;
    uint64_t raft_commit = 0;
    uint64_t raft_applied = 0;
    uint64_t raft_snapshot_idx = 0;

    if (server->raft) {
        raft_role_t role;
        size_t log_count;
        raft_status(server->raft, &raft_term, &role, &raft_commit, &raft_applied, &log_count, NULL);
        switch (role) {
            case RAFT_LEADER:    role_str = "leader";    break;
            case RAFT_CANDIDATE: role_str = "candidate"; break;
            case RAFT_FOLLOWER:  role_str = "follower";  break;
            default:             role_str = "unknown";   break;
        }

        raft_snapshot_info_t snap_info;
        if (raft_get_snapshot_info(server->raft, &snap_info) == 0) {
            raft_snapshot_idx = snap_info.last_included_index;
        }
    }

    char body[1024];
    int body_len = snprintf(body, sizeof(body),
        "{"
        "\"status\":\"ok\","
        "\"role\":\"%s\","
        "\"raft_term\":%llu,"
        "\"raft_commit_index\":%llu,"
        "\"raft_applied_index\":%llu,"
        "\"last_snapshot_index\":%llu,"
        "\"memtable_size\":%zu,"
        "\"sstable_count\":%zu,"
        "\"total_keys\":%zu,"
        "\"wal_enabled\":%d"
        "}",
        role_str,
        (unsigned long long)raft_term,
        (unsigned long long)raft_commit,
        (unsigned long long)raft_applied,
        (unsigned long long)raft_snapshot_idx,
        stats.memtable_size,
        stats.sstable_count,
        stats.total_keys,
        stats.wal_enabled
    );

    int written = snprintf(buf, buf_capacity,
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %d\r\n"
        "\r\n"
        "%s",
        body_len, body
    );

    if (written < 0 || (size_t)written >= buf_capacity) {
        return -1;
    }

    *out_len = (size_t)written;
    return 0;
}

/* 处理单个 HTTP 连接 */
static void metrics_handle_connection(metrics_server_t* server, SOCKET client_fd) {
    char buf[HTTP_BUF_SIZE];
    size_t buf_len = 0;

    /* 读取 HTTP 请求 */
    if (http_read_request(client_fd, buf, &buf_len, sizeof(buf)) != 0) {
        close_socket(client_fd);
        return;
    }

    char method[16] = {0};
    char path[256] = {0};

    if (http_parse_request(buf, buf_len, method, sizeof(method), path, sizeof(path)) != 0) {
        close_socket(client_fd);
        return;
    }

    char resp[METRICS_BUF_SIZE];
    size_t resp_len = 0;

    /* 处理 /metrics 和 /health */
    if (strcmp(method, "GET") == 0 && strcmp(path, "/metrics") == 0) {
        if (metrics_build_response(server, server->start_time, resp, sizeof(resp), &resp_len) != 0) {
            close_socket(client_fd);
            return;
        }
    } else if (strcmp(method, "GET") == 0 && strcmp(path, "/health") == 0) {
        if (metrics_build_health(server, resp, sizeof(resp), &resp_len) != 0) {
            close_socket(client_fd);
            return;
        }
    } else {
        if (metrics_build_404(resp, sizeof(resp), &resp_len) != 0) {
            close_socket(client_fd);
            return;
        }
    }

    /* 发送响应 */
    send(client_fd, resp, (int)resp_len, 0);
    close_socket(client_fd);
}

/* ================================================================
 * 公共 API
 * ================================================================ */

int metrics_server_start(metrics_server_t** out_server, const char* host, int port,
                         kv_store_t* db, raft_t* raft) {
    if (!out_server || !db) return -1;

    metrics_server_t* server = kv_malloc(sizeof(metrics_server_t));
    if (!server) return -1;
    memset(server, 0, sizeof(metrics_server_t));

    server->db = db;
    server->raft = raft;
    server->running = 0;
    server->host = kv_strdup(host ? host : "127.0.0.1");
    server->port = port;
    server->start_time = time(NULL);

#ifdef _WIN32
    WSADATA wsa_data;
    if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) {
        kv_free(server->host);
        kv_free(server);
        return -1;
    }
    server->wsock_initialized = 1;
#endif

    server->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server->listen_fd == INVALID_SOCKET) {
        printf("[METRICS] Failed to create socket\n");
        metrics_server_free(server);
        return -1;
    }

    /* 设置 SO_REUSEADDR */
    int opt = 1;
    setsockopt(server->listen_fd, SOL_SOCKET, SO_REUSEADDR,
               (const char*)&opt, sizeof(opt));

    /* 设置 TCP_NODELAY */
    setsockopt(server->listen_fd, IPPROTO_TCP, TCP_NODELAY,
               (const char*)&opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((unsigned short)port);
    addr.sin_addr.s_addr = inet_addr(server->host);

    if (bind(server->listen_fd, (struct sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
        printf("[METRICS] Failed to bind %s:%d\n", server->host, server->port);
        metrics_server_free(server);
        return -1;
    }

    if (listen(server->listen_fd, SOMAXCONN) == SOCKET_ERROR) {
        printf("[METRICS] Failed to listen\n");
        metrics_server_free(server);
        return -1;
    }

    printf("[METRICS] Server listening on %s:%d\n", server->host, server->port);
    *out_server = server;
    return 0;
}

void metrics_server_run(metrics_server_t* server) {
    if (!server) return;

    server->running = 1;
    printf("[METRICS] Server started, accepting connections...\n");

    while (server->running) {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(server->listen_fd, &read_fds);

        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        int ret = select((int)server->listen_fd + 1, &read_fds, NULL, NULL, &tv);
        if (ret < 0) {
            if (
#ifdef _WIN32
                WSAGetLastError() == WSAEINTR
#else
                errno == EINTR
#endif
            ) {
                continue;
            }
            break;
        }

        if (ret == 0) continue;

        /* 接受新连接 */
        if (FD_ISSET(server->listen_fd, &read_fds)) {
            struct sockaddr_in client_addr;
            socklen_t addr_len = sizeof(client_addr);
            SOCKET client_fd = accept(server->listen_fd,
                                       (struct sockaddr*)&client_addr, &addr_len);
            if (client_fd != INVALID_SOCKET) {
                /* 设置 TCP_NODELAY */
                int opt = 1;
                setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY,
                           (const char*)&opt, sizeof(opt));

                /* 处理单个连接（阻塞读取，但 select 已保证可读） */
                metrics_handle_connection(server, client_fd);
            }
        }
    }
}

void metrics_server_stop(metrics_server_t* server) {
    if (!server) return;
    server->running = 0;
}

void metrics_server_free(metrics_server_t* server) {
    if (!server) return;

    if (server->listen_fd != INVALID_SOCKET) {
        close_socket(server->listen_fd);
    }

    kv_free(server->host);

#ifdef _WIN32
    if (server->wsock_initialized) {
        WSACleanup();
    }
#endif

    kv_free(server);
}