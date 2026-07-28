#include "resp_server.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
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

/* ================================================================
 * RESP 协议解析
 * ================================================================ */

typedef enum {
    RESP_STRING  = '+',  /* Simple String */
    RESP_ERROR   = '-',  /* Error */
    RESP_INTEGER = ':',  /* Integer */
    RESP_BULK    = '$',  /* Bulk String */
    RESP_ARRAY   = '*',  /* Array */
} resp_type_t;

typedef struct resp_value {
    resp_type_t type;
    /* 对于 bulk string / error / simple string */
    char* str;
    size_t len;
    /* 对于 integer */
    long long integer;
    /* 对于 array */
    struct resp_value** elements;
    size_t count;
} resp_value_t;

static resp_value_t* resp_value_new(resp_type_t type) {
    resp_value_t* v = kv_malloc(sizeof(resp_value_t));
    if (!v) return NULL;
    memset(v, 0, sizeof(resp_value_t));
    v->type = type;
    return v;
}

static void resp_value_free(resp_value_t* v) {
    if (!v) return;
    if (v->str) kv_free(v->str);
    if (v->elements) {
        for (size_t i = 0; i < v->count; i++) {
            resp_value_free(v->elements[i]);
        }
        kv_free(v->elements);
    }
    kv_free(v);
}

/* 读取一行直到 \r\n，返回行内容（不含 \r\n），需要调用者释放 */
static char* resp_read_line(SOCKET fd, char* buf, size_t* buf_pos, size_t* buf_len) {
    (void)fd;
    /* 在缓冲区中查找 \r\n */
    for (size_t i = *buf_pos; i + 1 < *buf_len; i++) {
        if (buf[i] == '\r' && buf[i + 1] == '\n') {
            size_t line_len = i - *buf_pos;
            char* line = kv_malloc(line_len + 1);
            if (!line) return NULL;
            memcpy(line, buf + *buf_pos, line_len);
            line[line_len] = '\0';
            *buf_pos = i + 2;
            return line;
        }
    }
    return NULL; /* 行不完整，需要更多数据 */
}

/* 从 socket 读取更多数据到缓冲区 */
static int resp_fill_buffer(SOCKET fd, char* buf, size_t* buf_len, size_t buf_capacity) {
    if (*buf_len >= buf_capacity) return -1;
    int n = recv(fd, buf + *buf_len, (int)(buf_capacity - *buf_len), 0);
    if (n <= 0) return -1;
    *buf_len += n;
    return 0;
}

/* 解析 RESP 协议 */
static resp_value_t* resp_parse(SOCKET fd, char* buf, size_t* buf_pos, size_t* buf_len) {
    if (*buf_pos >= *buf_len) {
        if (resp_fill_buffer(fd, buf, buf_len, RESP_MAX_CMD_LEN) != 0) return NULL;
    }

    if (*buf_pos >= *buf_len) return NULL;

    char type = buf[*buf_pos];
    (*buf_pos)++;

    char* line = resp_read_line(fd, buf, buf_pos, buf_len);
    if (!line) return NULL;

    resp_value_t* v = NULL;

    switch (type) {
    case '+': { /* Simple String */
        v = resp_value_new(RESP_STRING);
        if (v) {
            v->str = line;
            v->len = strlen(line);
        } else {
            kv_free(line);
        }
        return v;
    }
    case '-': { /* Error */
        v = resp_value_new(RESP_ERROR);
        if (v) {
            v->str = line;
            v->len = strlen(line);
        } else {
            kv_free(line);
        }
        return v;
    }
    case ':': { /* Integer */
        v = resp_value_new(RESP_INTEGER);
        if (v) {
            v->integer = strtoll(line, NULL, 10);
        }
        kv_free(line);
        return v;
    }
    case '$': { /* Bulk String */
        long long bulk_len = strtoll(line, NULL, 10);
        kv_free(line);

        if (bulk_len < 0) {
            /* Null bulk string */
            v = resp_value_new(RESP_BULK);
            if (v) {
                v->str = NULL;
                v->len = 0;
            }
            return v;
        }

        if (bulk_len > RESP_MAX_CMD_LEN) return NULL;

        v = resp_value_new(RESP_BULK);
        if (!v) return NULL;

        /* 确保缓冲区有足够数据: bulk_len + \r\n */
        while (*buf_len - *buf_pos < (size_t)bulk_len + 2) {
            if (resp_fill_buffer(fd, buf, buf_len, RESP_MAX_CMD_LEN) != 0) {
                resp_value_free(v);
                return NULL;
            }
        }

        v->str = kv_malloc((size_t)bulk_len + 1);
        if (!v->str) {
            resp_value_free(v);
            return NULL;
        }
        memcpy(v->str, buf + *buf_pos, (size_t)bulk_len);
        v->str[bulk_len] = '\0';
        v->len = (size_t)bulk_len;
        *buf_pos += (size_t)bulk_len;

        /* 读取 \r\n */
        line = resp_read_line(fd, buf, buf_pos, buf_len);
        if (line) kv_free(line);

        return v;
    }
    case '*': { /* Array */
        long long arr_count = strtoll(line, NULL, 10);
        kv_free(line);

        if (arr_count < 0 || arr_count > 256) return NULL;

        v = resp_value_new(RESP_ARRAY);
        if (!v) return NULL;

        v->count = (size_t)arr_count;
        if (v->count == 0) return v;

        v->elements = kv_calloc(v->count, sizeof(resp_value_t*));
        if (!v->elements) {
            resp_value_free(v);
            return NULL;
        }

        for (size_t i = 0; i < v->count; i++) {
            v->elements[i] = resp_parse(fd, buf, buf_pos, buf_len);
            if (!v->elements[i]) {
                resp_value_free(v);
                return NULL;
            }
        }

        return v;
    }
    default:
        kv_free(line);
        return NULL;
    }
}

/* ================================================================
 * RESP 响应构建
 * ================================================================ */

typedef struct {
    char* buf;
    size_t len;
    size_t capacity;
} resp_reply_t;

static resp_reply_t* resp_reply_new(void) {
    resp_reply_t* r = kv_malloc(sizeof(resp_reply_t));
    if (!r) return NULL;
    r->capacity = RESP_REPLY_BUF_SIZE;
    r->buf = kv_malloc(r->capacity);
    if (!r->buf) {
        kv_free(r);
        return NULL;
    }
    r->len = 0;
    return r;
}

static void resp_reply_free(resp_reply_t* r) {
    if (!r) return;
    kv_free(r->buf);
    kv_free(r);
}

static int resp_reply_append(resp_reply_t* r, const char* data, size_t len) {
    while (r->len + len > r->capacity) {
        size_t new_cap = r->capacity * 2;
        char* new_buf = kv_realloc(r->buf, new_cap);
        if (!new_buf) return -1;
        r->buf = new_buf;
        r->capacity = new_cap;
    }
    memcpy(r->buf + r->len, data, len);
    r->len += len;
    return 0;
}

#define REPLY_APPEND_STRING(r, s) resp_reply_append((r), (s), strlen(s))

/* 构建各类 RESP 响应 */
static int resp_make_simple_string(resp_reply_t* r, const char* str) {
    REPLY_APPEND_STRING(r, "+");
    REPLY_APPEND_STRING(r, str);
    REPLY_APPEND_STRING(r, "\r\n");
    return 0;
}

static int resp_make_error(resp_reply_t* r, const char* msg) {
    REPLY_APPEND_STRING(r, "-ERR ");
    REPLY_APPEND_STRING(r, msg);
    REPLY_APPEND_STRING(r, "\r\n");
    return 0;
}

static int resp_make_integer(resp_reply_t* r, long long val) {
    REPLY_APPEND_STRING(r, ":");
    char num[32];
    snprintf(num, sizeof(num), "%lld", val);
    REPLY_APPEND_STRING(r, num);
    REPLY_APPEND_STRING(r, "\r\n");
    return 0;
}

static int resp_make_null(resp_reply_t* r) {
    REPLY_APPEND_STRING(r, "$-1\r\n");
    return 0;
}

static int resp_make_bulk_string(resp_reply_t* r, const char* str, size_t len) {
    if (!str) return resp_make_null(r);
    REPLY_APPEND_STRING(r, "$");
    char num[32];
    snprintf(num, sizeof(num), "%zu", len);
    REPLY_APPEND_STRING(r, num);
    REPLY_APPEND_STRING(r, "\r\n");
    resp_reply_append(r, str, len);
    REPLY_APPEND_STRING(r, "\r\n");
    return 0;
}

static int resp_make_ok(resp_reply_t* r) {
    return resp_make_simple_string(r, "OK");
}

static int resp_make_pong(resp_reply_t* r) {
    return resp_make_simple_string(r, "PONG");
}

/* ================================================================
 * 命令处理
 * ================================================================ */

/* 将字符串转为小写 */
static void str_tolower(char* s) {
    for (; *s; s++) *s = (char)tolower((unsigned char)*s);
}

static int cmd_ping(resp_reply_t* r, resp_value_t* cmd) {
    if (cmd->count > 1 && cmd->elements[1]->type == RESP_BULK && cmd->elements[1]->str) {
        return resp_make_bulk_string(r, cmd->elements[1]->str, cmd->elements[1]->len);
    }
    return resp_make_pong(r);
}

static int cmd_set(kv_store_t* db, resp_reply_t* r, resp_value_t* cmd) {
    if (cmd->count < 3) {
        return resp_make_error(r, "wrong number of arguments for 'SET'");
    }
    if (cmd->elements[1]->type != RESP_BULK || !cmd->elements[1]->str) {
        return resp_make_error(r, "invalid key");
    }
    if (cmd->elements[2]->type != RESP_BULK || !cmd->elements[2]->str) {
        return resp_make_error(r, "invalid value");
    }

    int ret = kv_put(db,
        cmd->elements[1]->str, cmd->elements[1]->len,
        cmd->elements[2]->str, cmd->elements[2]->len);
    if (ret != 0) {
        return resp_make_error(r, "write failed");
    }
    return resp_make_ok(r);
}

static int cmd_get(kv_store_t* db, resp_reply_t* r, resp_value_t* cmd) {
    if (cmd->count < 2) {
        return resp_make_error(r, "wrong number of arguments for 'GET'");
    }
    if (cmd->elements[1]->type != RESP_BULK || !cmd->elements[1]->str) {
        return resp_make_error(r, "invalid key");
    }

    char* val = NULL;
    size_t vlen = 0;
    int ret = kv_get(db, cmd->elements[1]->str, cmd->elements[1]->len, &val, &vlen);
    if (ret != 0) {
        return resp_make_null(r);
    }

    int rv = resp_make_bulk_string(r, val, vlen);
    kv_free(val);
    return rv;
}

static int cmd_del(kv_store_t* db, resp_reply_t* r, resp_value_t* cmd) {
    if (cmd->count < 2) {
        return resp_make_error(r, "wrong number of arguments for 'DEL'");
    }

    int deleted = 0;
    for (size_t i = 1; i < cmd->count; i++) {
        if (cmd->elements[i]->type == RESP_BULK && cmd->elements[i]->str) {
            if (kv_delete(db, cmd->elements[i]->str, cmd->elements[i]->len) == 0) {
                deleted++;
            }
        }
    }
    return resp_make_integer(r, deleted);
}

static int cmd_exists(kv_store_t* db, resp_reply_t* r, resp_value_t* cmd) {
    if (cmd->count < 2) {
        return resp_make_error(r, "wrong number of arguments for 'EXISTS'");
    }

    int exists = 0;
    for (size_t i = 1; i < cmd->count; i++) {
        if (cmd->elements[i]->type == RESP_BULK && cmd->elements[i]->str) {
            char* val = NULL;
            size_t vlen = 0;
            if (kv_get(db, cmd->elements[i]->str, cmd->elements[i]->len, &val, &vlen) == 0) {
                exists++;
                kv_free(val);
            }
        }
    }
    return resp_make_integer(r, exists);
}

static int cmd_dbsize(kv_store_t* db, resp_reply_t* r, resp_value_t* cmd) {
    (void)cmd;
    /* 通过 scan 统计 key 数量 */
    kv_iter_t* iter = kv_scan(db, NULL, 0, NULL, 0);
    if (!iter) return resp_make_integer(r, 0);

    long long count = 0;
    char* k = NULL; size_t kl = 0;
    char* v = NULL; size_t vl = 0;
    while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
        count++;
        kv_free(k);
        kv_free(v);
    }
    kv_iter_free(iter);
    return resp_make_integer(r, count);
}

static int cmd_flushdb(kv_store_t* db, resp_reply_t* r, resp_value_t* cmd) {
    (void)cmd;
    /* 扫描所有 key 并删除 */
    kv_iter_t* iter = kv_scan(db, NULL, 0, NULL, 0);
    if (iter) {
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            kv_delete(db, k, kl);
            kv_free(k);
            kv_free(v);
        }
        kv_iter_free(iter);
    }
    return resp_make_ok(r);
}

static int cmd_info(kv_store_t* db, resp_reply_t* r, resp_value_t* cmd) {
    (void)cmd;
    (void)db;
    char info[1024];
    snprintf(info, sizeof(info),
        "# Server\r\n"
        "kv_store_version:1.0.0\r\n"
        "os:Windows\r\n"
        "redis_version:6.0.0  /* compatible */\r\n"
        "\r\n"
        "# Keyspace\r\n");
    /* 简要信息 */
    resp_make_bulk_string(r, info, strlen(info));
    return 0;
}

static int cmd_quit(resp_reply_t* r, resp_value_t* cmd) {
    (void)cmd;
    return resp_make_ok(r);
}

static int cmd_save(kv_store_t* db, resp_reply_t* r, resp_value_t* cmd) {
    (void)cmd;
    if (kv_sync(db) != 0) {
        return resp_make_error(r, "save failed");
    }
    return resp_make_ok(r);
}

static int cmd_bgsave(kv_store_t* db, resp_reply_t* r, resp_value_t* cmd) {
    /* 同步执行（简化实现） */
    return cmd_save(db, r, cmd);
}

/* ================================================================
 * 客户端连接管理
 * ================================================================ */

typedef struct resp_client {
    SOCKET fd;
    char read_buf[RESP_IO_BUF_SIZE];
    size_t buf_pos;
    size_t buf_len;
    int active;
} resp_client_t;

struct resp_server {
    SOCKET listen_fd;
    kv_store_t* db;
    resp_client_t clients[RESP_MAX_CLIENTS];
    int running;
    char* host;
    int port;
#ifdef _WIN32
    int wsock_initialized;
#endif
};

/* 处理客户端请求 */
static void resp_process_client(resp_server_t* server, resp_client_t* client) {
    /* 读取更多数据 */
    if (client->buf_len < RESP_IO_BUF_SIZE) {
        int n = recv(client->fd, client->read_buf + client->buf_len,
                     (int)(RESP_IO_BUF_SIZE - client->buf_len), 0);
        if (n <= 0) {
            client->active = 0;
            return;
        }
        client->buf_len += n;
    }

    /* 尝试解析命令 */
    while (client->buf_pos < client->buf_len) {
        size_t saved_pos = client->buf_pos;
        size_t saved_len = client->buf_len;

        resp_value_t* cmd = resp_parse(client->fd, client->read_buf,
                                        &client->buf_pos, &client->buf_len);
        if (!cmd) {
            /* 解析失败或数据不完整 */
            client->buf_pos = saved_pos;
            client->buf_len = saved_len;
            /* 紧凑缓冲区 */
            if (client->buf_pos > 0 && client->buf_len > client->buf_pos) {
                memmove(client->read_buf, client->read_buf + client->buf_pos,
                        client->buf_len - client->buf_pos);
                client->buf_len -= client->buf_pos;
                client->buf_pos = 0;
            } else if (client->buf_pos >= client->buf_len) {
                client->buf_pos = 0;
                client->buf_len = 0;
            }
            return;
        }

        /* 紧凑缓冲区 */
        if (client->buf_pos > 0 && client->buf_len > client->buf_pos) {
            memmove(client->read_buf, client->read_buf + client->buf_pos,
                    client->buf_len - client->buf_pos);
            client->buf_len -= client->buf_pos;
            client->buf_pos = 0;
        } else if (client->buf_pos >= client->buf_len) {
            client->buf_pos = 0;
            client->buf_len = 0;
        }

        if (cmd->type != RESP_ARRAY || cmd->count == 0) {
            resp_value_free(cmd);
            continue;
        }

        /* 获取命令名 */
        if (cmd->elements[0]->type != RESP_BULK || !cmd->elements[0]->str) {
            resp_value_free(cmd);
            continue;
        }

        char* cmd_name = cmd->elements[0]->str;
        str_tolower(cmd_name);

        resp_reply_t* reply = resp_reply_new();
        if (!reply) {
            resp_value_free(cmd);
            continue;
        }

        /* 路由命令 */
        if (strcmp(cmd_name, "ping") == 0) {
            cmd_ping(reply, cmd);
        } else if (strcmp(cmd_name, "set") == 0) {
            cmd_set(server->db, reply, cmd);
        } else if (strcmp(cmd_name, "get") == 0) {
            cmd_get(server->db, reply, cmd);
        } else if (strcmp(cmd_name, "del") == 0) {
            cmd_del(server->db, reply, cmd);
        } else if (strcmp(cmd_name, "exists") == 0) {
            cmd_exists(server->db, reply, cmd);
        } else if (strcmp(cmd_name, "dbsize") == 0) {
            cmd_dbsize(server->db, reply, cmd);
        } else if (strcmp(cmd_name, "flushdb") == 0) {
            cmd_flushdb(server->db, reply, cmd);
        } else if (strcmp(cmd_name, "info") == 0) {
            cmd_info(server->db, reply, cmd);
        } else if (strcmp(cmd_name, "quit") == 0) {
            cmd_quit(reply, cmd);
        } else if (strcmp(cmd_name, "save") == 0) {
            cmd_save(server->db, reply, cmd);
        } else if (strcmp(cmd_name, "bgsave") == 0) {
            cmd_bgsave(server->db, reply, cmd);
        } else if (strcmp(cmd_name, "command") == 0) {
            resp_make_ok(reply);  /* 兼容 redis-cli */
        } else {
            char err[128];
            snprintf(err, sizeof(err), "unknown command '%s'", cmd_name);
            resp_make_error(reply, err);
        }

        /* 发送响应 */
        if (reply->len > 0) {
            send(client->fd, reply->buf, (int)reply->len, 0);
        }

        resp_reply_free(reply);
        resp_value_free(cmd);

        /* QUIT 命令关闭连接 */
        if (strcmp(cmd_name, "quit") == 0) {
            client->active = 0;
            return;
        }
    }
}

/* ================================================================
 * 服务器主循环
 * ================================================================ */

int resp_server_start(resp_server_t** out_server, const char* host, int port, kv_store_t* db) {
    if (!out_server || !db) return -1;

    resp_server_t* server = kv_malloc(sizeof(resp_server_t));
    if (!server) return -1;
    memset(server, 0, sizeof(resp_server_t));

    server->db = db;
    server->running = 0;
    server->host = kv_strdup(host ? host : "127.0.0.1");
    server->port = port;

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
        printf("[RESP] Failed to create socket\n");
        resp_server_free(server);
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
        printf("[RESP] Failed to bind %s:%d\n", server->host, server->port);
        resp_server_free(server);
        return -1;
    }

    if (listen(server->listen_fd, SOMAXCONN) == SOCKET_ERROR) {
        printf("[RESP] Failed to listen\n");
        resp_server_free(server);
        return -1;
    }

    /* 初始化客户端 */
    for (int i = 0; i < RESP_MAX_CLIENTS; i++) {
        server->clients[i].fd = INVALID_SOCKET;
        server->clients[i].active = 0;
    }

    printf("[RESP] Server listening on %s:%d\n", server->host, server->port);
    *out_server = server;
    return 0;
}

void resp_server_run(resp_server_t* server) {
    if (!server) return;

    server->running = 1;
    printf("[RESP] Server started, accepting connections...\n");

    while (server->running) {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(server->listen_fd, &read_fds);

        SOCKET max_fd = server->listen_fd;

        for (int i = 0; i < RESP_MAX_CLIENTS; i++) {
            if (server->clients[i].active) {
                FD_SET(server->clients[i].fd, &read_fds);
                if (server->clients[i].fd > max_fd) {
                    max_fd = server->clients[i].fd;
                }
            }
        }

        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        int ret = select((int)max_fd + 1, &read_fds, NULL, NULL, &tv);
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

                int added = 0;
                for (int i = 0; i < RESP_MAX_CLIENTS; i++) {
                    if (!server->clients[i].active) {
                        server->clients[i].fd = client_fd;
                        server->clients[i].buf_pos = 0;
                        server->clients[i].buf_len = 0;
                        server->clients[i].active = 1;
                        added = 1;
                        printf("[RESP] Client connected: %s\n",
                               inet_ntoa(client_addr.sin_addr));
                        break;
                    }
                }
                if (!added) {
                    printf("[RESP] Max clients reached, rejecting connection\n");
                    close_socket(client_fd);
                }
            }
        }

        /* 处理客户端数据 */
        for (int i = 0; i < RESP_MAX_CLIENTS; i++) {
            if (server->clients[i].active && FD_ISSET(server->clients[i].fd, &read_fds)) {
                resp_process_client(server, &server->clients[i]);
                if (!server->clients[i].active) {
                    printf("[RESP] Client disconnected\n");
                    close_socket(server->clients[i].fd);
                    server->clients[i].fd = INVALID_SOCKET;
                }
            }
        }
    }
}

void resp_server_stop(resp_server_t* server) {
    if (!server) return;
    server->running = 0;
}

void resp_server_free(resp_server_t* server) {
    if (!server) return;

    /* 关闭所有客户端连接 */
    for (int i = 0; i < RESP_MAX_CLIENTS; i++) {
        if (server->clients[i].active) {
            close_socket(server->clients[i].fd);
            server->clients[i].fd = INVALID_SOCKET;
            server->clients[i].active = 0;
        }
    }

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