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
    #include <windows.h>
    #define close_socket(s)  closesocket(s)
    #define socklen_t        int
    #define SHUT_RDWR        SD_BOTH
    typedef HANDLE           thread_t;
    typedef CRITICAL_SECTION mutex_t;
    #define MUTEX_INIT(m)    InitializeCriticalSection(m)
    #define MUTEX_LOCK(m)    EnterCriticalSection(m)
    #define MUTEX_UNLOCK(m)  LeaveCriticalSection(m)
    #define MUTEX_DESTROY(m) DeleteCriticalSection(m)
    static void thread_sleep_ms(int ms) { Sleep((DWORD)ms); }
    static uint64_t thread_time_ms(void) { return (uint64_t)GetTickCount64(); }
    static int thread_create(thread_t* t, DWORD WINAPI (*fn)(LPVOID), void* arg) {
        *t = CreateThread(NULL, 0, fn, arg, 0, NULL);
        return *t ? 0 : -1;
    }
    static void thread_join(thread_t t) { WaitForSingleObject(t, INFINITE); CloseHandle(t); }
#else
    #include <sys/socket.h>
    #include <sys/select.h>
    #include <netinet/in.h>
    #include <netinet/tcp.h>
    #include <arpa/inet.h>
    #include <unistd.h>
    #include <fcntl.h>
    #include <signal.h>
    #include <pthread.h>
    #define close_socket(s)  close(s)
    #define SOCKET           int
    #define INVALID_SOCKET   (-1)
    #define SOCKET_ERROR     (-1)
    typedef pthread_t        thread_t;
    typedef pthread_mutex_t  mutex_t;
    #define MUTEX_INIT(m)    pthread_mutex_init(m, NULL)
    #define MUTEX_LOCK(m)    pthread_mutex_lock(m)
    #define MUTEX_UNLOCK(m)  pthread_mutex_unlock(m)
    #define MUTEX_DESTROY(m) pthread_mutex_destroy(m)
    static void thread_sleep_ms(int ms) { usleep(ms * 1000); }
    static uint64_t thread_time_ms(void) {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
    }
    static int thread_create(thread_t* t, void* (*fn)(void*), void* arg) {
        return pthread_create(t, NULL, fn, arg);
    }
    static void thread_join(thread_t t) { pthread_join(t, NULL); }
#endif

/* ================================================================
 * 二进制 RPC 消息协议
 * ================================================================ */
#define RAFT_RPC_VOTE_REQ      0
#define RAFT_RPC_VOTE_RESP     1
#define RAFT_RPC_APPEND_REQ    2
#define RAFT_RPC_APPEND_RESP   3
#define RAFT_RPC_PROPOSE_REQ   4
#define RAFT_RPC_PROPOSE_RESP  5
#define RAFT_RPC_REDIRECT      6

#define RAFT_MAX_MSG_SIZE      (1024 * 1024)  /* 1MB */
#define RAFT_MAX_LOG_ENTRIES   10000

/* --- 简单网络字节序辅助 --- */
static inline void write_u64(uint8_t* buf, uint64_t v) {
    buf[0] = (uint8_t)(v >> 56); buf[1] = (uint8_t)(v >> 48);
    buf[2] = (uint8_t)(v >> 40); buf[3] = (uint8_t)(v >> 32);
    buf[4] = (uint8_t)(v >> 24); buf[5] = (uint8_t)(v >> 16);
    buf[6] = (uint8_t)(v >> 8);  buf[7] = (uint8_t)(v);
}
static inline uint64_t read_u64(const uint8_t* buf) {
    return ((uint64_t)buf[0] << 56) | ((uint64_t)buf[1] << 48) |
           ((uint64_t)buf[2] << 40) | ((uint64_t)buf[3] << 32) |
           ((uint64_t)buf[4] << 24) | ((uint64_t)buf[5] << 16) |
           ((uint64_t)buf[6] << 8)  | (uint64_t)buf[7];
}
static inline void write_u32(uint8_t* buf, uint32_t v) {
    buf[0] = (uint8_t)(v >> 24); buf[1] = (uint8_t)(v >> 16);
    buf[2] = (uint8_t)(v >> 8);  buf[3] = (uint8_t)(v);
}
static inline uint32_t read_u32(const uint8_t* buf) {
    return ((uint32_t)buf[0] << 24) | ((uint32_t)buf[1] << 16) |
           ((uint32_t)buf[2] << 8)  | (uint32_t)buf[3];
}

/* ================================================================
 * Raft 日志持久化
 * ================================================================ */
#define RAFT_LOG_MAX_ENTRIES  10000

typedef struct {
    uint64_t term;
    uint64_t index;
    uint8_t  type;
    size_t   key_len;
    char*    key;
    size_t   value_len;
    char*    value;
} raft_log_entry_t;

/* --- 日志持久化到文件 --- */
static int raft_log_save(raft_t* r);
static int raft_log_load(raft_t* r);

/* ================================================================
 * Raft 节点结构
 * ================================================================ */
struct raft {
    /* 配置 */
    raft_config_t cfg;

    /* 持久化状态（所有节点） */
    uint64_t current_term;
    char     voted_for[RAFT_NODE_ID_LEN];
    raft_log_entry_t* log;        /* 日志数组 */
    size_t   log_count;
    size_t   log_capacity;

    /* 易失状态（所有节点） */
    uint64_t commit_index;
    uint64_t last_applied;

    /* Leader 状态 */
    uint64_t* next_index;
    uint64_t* match_index;

    /* 运行时状态 */
    raft_role_t role;
    uint64_t    election_timeout_ms;
    uint64_t    last_heartbeat_ms;   /* 上次收到心跳/AppendEntries 的时间 */
    uint64_t    last_election_ms;    /* 上次发起选举的时间（Leader 用于心跳间隔） */
    int         votes_received;
    int         running;

    /* 网络 */
    SOCKET      listen_fd;

    /* 状态机 */
    void*       state_machine;
    raft_apply_cb apply_fn;

    /* 线程 */
    thread_t    thread;
    mutex_t     mutex;

    /* 当前 Leader peer（用于重定向） */
    raft_peer_t leader_peer;
    int         leader_known;

    /* 条件变量：propose 等待 */
    mutex_t     propose_mutex;
    int         propose_done;
    int         propose_result;
};

/* --- 辅助：查找 peer 索引 --- */
static int raft_find_peer(raft_t* r, const char* node_id) {
    for (int i = 0; i < r->cfg.num_peers; i++) {
        if (strcmp(r->cfg.peers[i].id, node_id) == 0) return i;
    }
    return -1;
}

/* --- 辅助：获取自身 peer 索引 --- */
static int raft_self_index(raft_t* r) {
    return raft_find_peer(r, r->cfg.node_id);
}

/* ================================================================
 * 日志持久化
 * ================================================================ */

static int raft_log_save(raft_t* r) {
    char path[512];
    snprintf(path, sizeof(path), "%s/raft_log", r->cfg.data_dir);

    FILE* f = fopen(path, "wb");
    if (!f) return -1;

    /* 写入 current_term */
    fwrite(&r->current_term, 8, 1, f);

    /* 写入 voted_for */
    uint32_t voted_len = (uint32_t)strlen(r->voted_for);
    fwrite(&voted_len, 4, 1, f);
    fwrite(r->voted_for, 1, voted_len, f);

    /* 写入日志条目数 */
    uint64_t count = (uint64_t)r->log_count;
    fwrite(&count, 8, 1, f);

    /* 写入日志条目 */
    for (size_t i = 0; i < r->log_count; i++) {
        raft_log_entry_t* e = &r->log[i];
        fwrite(&e->term, 8, 1, f);
        fwrite(&e->index, 8, 1, f);
        fwrite(&e->type, 1, 1, f);

        uint64_t klen = (uint64_t)e->key_len;
        fwrite(&klen, 8, 1, f);
        fwrite(e->key, 1, e->key_len, f);

        uint64_t vlen = (uint64_t)e->value_len;
        fwrite(&vlen, 8, 1, f);
        if (e->value_len > 0) fwrite(e->value, 1, e->value_len, f);
    }

    fclose(f);
    return 0;
}

static int raft_log_load(raft_t* r) {
    char path[512];
    snprintf(path, sizeof(path), "%s/raft_log", r->cfg.data_dir);

    FILE* f = fopen(path, "rb");
    if (!f) {
        /* 首次启动，无日志 */
        r->current_term = 0;
        memset(r->voted_for, 0, sizeof(r->voted_for));
        r->log_count = 0;
        r->log_capacity = 1024;
        r->log = kv_malloc(r->log_capacity * sizeof(raft_log_entry_t));
        return 0;
    }

    /* 读取 current_term */
    if (fread(&r->current_term, 8, 1, f) != 1) { fclose(f); return -1; }

    /* 读取 voted_for */
    uint32_t voted_len = 0;
    if (fread(&voted_len, 4, 1, f) != 1) { fclose(f); return -1; }
    if (voted_len >= RAFT_NODE_ID_LEN) { fclose(f); return -1; }
    fread(r->voted_for, 1, voted_len, f);
    r->voted_for[voted_len] = '\0';

    /* 读取日志条目 */
    uint64_t count = 0;
    if (fread(&count, 8, 1, f) != 1) { fclose(f); return -1; }
    r->log_capacity = (count > 1024) ? count + 1024 : 1024;
    r->log = kv_malloc(r->log_capacity * sizeof(raft_log_entry_t));
    r->log_count = (size_t)count;

    for (size_t i = 0; i < r->log_count; i++) {
        raft_log_entry_t* e = &r->log[i];
        memset(e, 0, sizeof(*e));

        if (fread(&e->term, 8, 1, f) != 1) break;
        if (fread(&e->index, 8, 1, f) != 1) break;
        if (fread(&e->type, 1, 1, f) != 1) break;

        uint64_t klen = 0;
        if (fread(&klen, 8, 1, f) != 1) break;
        e->key_len = (size_t)klen;
        e->key = kv_malloc(e->key_len);
        fread(e->key, 1, e->key_len, f);

        uint64_t vlen = 0;
        if (fread(&vlen, 8, 1, f) != 1) break;
        e->value_len = (size_t)vlen;
        if (vlen > 0) {
            e->value = kv_malloc(e->value_len);
            fread(e->value, 1, e->value_len, f);
        }
    }

    fclose(f);

    /* 初始化 commit_index 和 last_applied */
    r->commit_index = r->log_count > 0 ? r->log[r->log_count - 1].index : 0;
    r->last_applied = r->commit_index;

    printf("[RAFT] Loaded log: term=%llu, entries=%zu\n",
           (unsigned long long)r->current_term, r->log_count);
    return 0;
}

/* ================================================================
 * RPC 消息序列化 / 反序列化
 * ================================================================ */

/* RequestVote 请求 */
static int rpc_send_vote_request(SOCKET fd, uint64_t term, const char* candidate_id,
                                  uint64_t last_log_idx, uint64_t last_log_term) {
    uint8_t buf[2048];
    size_t off = 0;

    buf[off++] = RAFT_RPC_VOTE_REQ;
    write_u64(buf + off, term); off += 8;
    uint32_t id_len = (uint32_t)strlen(candidate_id);
    write_u32(buf + off, id_len); off += 4;
    memcpy(buf + off, candidate_id, id_len); off += id_len;
    write_u64(buf + off, last_log_idx); off += 8;
    write_u64(buf + off, last_log_term); off += 8;

    /* 发送长度前缀 + 数据 */
    uint32_t total = (uint32_t)off;
    write_u32(buf + 2048 - 4, total);
    send(fd, (const char*)buf + 2048 - 4, 4, 0);
    send(fd, (const char*)buf, (int)off, 0);
    return 0;
}

/* RequestVote 响应 */
static int rpc_send_vote_response(SOCKET fd, uint64_t term, int granted) {
    uint8_t buf[16];
    buf[0] = RAFT_RPC_VOTE_RESP;
    write_u64(buf + 1, term);
    buf[9] = (uint8_t)(granted ? 1 : 0);
    uint32_t total = 10;
    write_u32(buf + 10, total);
    send(fd, (const char*)buf + 10, 4, 0);
    send(fd, (const char*)buf, 10, 0);
    return 0;
}

/* AppendEntries 请求 */
static int rpc_send_append_request(SOCKET fd, uint64_t term, const char* leader_id,
                                    uint64_t prev_idx, uint64_t prev_term,
                                    raft_log_entry_t* entries, int n_entries,
                                    uint64_t leader_commit) {
    uint8_t* buf = kv_malloc(RAFT_MAX_MSG_SIZE);
    if (!buf) return -1;
    size_t off = 0;

    buf[off++] = RAFT_RPC_APPEND_REQ;
    write_u64(buf + off, term); off += 8;
    uint32_t id_len = (uint32_t)strlen(leader_id);
    write_u32(buf + off, id_len); off += 4;
    memcpy(buf + off, leader_id, id_len); off += id_len;
    write_u64(buf + off, prev_idx); off += 8;
    write_u64(buf + off, prev_term); off += 8;
    write_u64(buf + off, (uint64_t)n_entries); off += 8;
    write_u64(buf + off, leader_commit); off += 8;

    for (int i = 0; i < n_entries; i++) {
        raft_log_entry_t* e = &entries[i];
        write_u64(buf + off, e->term); off += 8;
        write_u64(buf + off, e->index); off += 8;
        buf[off++] = e->type;
        write_u64(buf + off, (uint64_t)e->key_len); off += 8;
        memcpy(buf + off, e->key, e->key_len); off += e->key_len;
        write_u64(buf + off, (uint64_t)e->value_len); off += 8;
        if (e->value_len > 0) {
            memcpy(buf + off, e->value, e->value_len);
            off += e->value_len;
        }
    }

    /* 发送长度前缀 */
    uint32_t total = (uint32_t)off;
    send(fd, (const char*)&total, 4, 0);
    send(fd, (const char*)buf, (int)off, 0);
    kv_free(buf);
    return 0;
}

/* AppendEntries 响应 */
static int rpc_send_append_response(SOCKET fd, uint64_t term, int success, uint64_t last_idx) {
    uint8_t buf[32];
    buf[0] = RAFT_RPC_APPEND_RESP;
    write_u64(buf + 1, term);
    buf[9] = (uint8_t)(success ? 1 : 0);
    write_u64(buf + 10, last_idx);
    uint32_t total = 18;
    write_u32(buf + 18, total);
    send(fd, (const char*)buf + 18, 4, 0);
    send(fd, (const char*)buf, 18, 0);
    return 0;
}

/* Propose 响应 */
static int rpc_send_propose_response(SOCKET fd, uint64_t term, int success,
                                      const char* leader_id, const char* leader_host, int leader_port) {
    uint8_t buf[1024];
    buf[0] = RAFT_RPC_PROPOSE_RESP;
    write_u64(buf + 1, term);
    buf[9] = (uint8_t)(success ? 1 : 0);
    uint32_t id_len = leader_id ? (uint32_t)strlen(leader_id) : 0;
    write_u32(buf + 10, id_len);
    if (id_len > 0) memcpy(buf + 14, leader_id, id_len);
    size_t off = 14 + id_len;
    uint32_t host_len = leader_host ? (uint32_t)strlen(leader_host) : 0;
    write_u32(buf + off, host_len); off += 4;
    if (host_len > 0) memcpy(buf + off, leader_host, host_len);
    off += host_len;
    write_u32(buf + off, (uint32_t)leader_port); off += 4;

    uint32_t total = (uint32_t)off;
    send(fd, (const char*)&total, 4, 0);
    send(fd, (const char*)buf, (int)off, 0);
    return 0;
}

/* ================================================================
 * 网络发送辅助
 * ================================================================ */

static SOCKET raft_connect_peer(raft_peer_t* peer) {
    SOCKET fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == INVALID_SOCKET) return INVALID_SOCKET;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((unsigned short)peer->port);
    addr.sin_addr.s_addr = inet_addr(peer->host);

    /* 设置超时 */
    int timeout = RAFT_RPC_TIMEOUT_MS;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (const char*)&timeout, sizeof(timeout));

    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
        close_socket(fd);
        return INVALID_SOCKET;
    }
    return fd;
}

/* ================================================================
 * 核心 Raft 算法
 * ================================================================ */

static void raft_become_follower(raft_t* r, uint64_t term) {
    r->role = RAFT_FOLLOWER;
    r->current_term = term;
    memset(r->voted_for, 0, sizeof(r->voted_for));
    printf("[RAFT] %s -> FOLLOWER (term=%llu)\n", r->cfg.node_id,
           (unsigned long long)term);
}

static void raft_become_candidate(raft_t* r) {
    r->role = RAFT_CANDIDATE;
    r->current_term++;
    r->votes_received = 1; /* vote for self */
    snprintf(r->voted_for, sizeof(r->voted_for), "%s", r->cfg.node_id);
    r->election_timeout_ms = RAFT_ELECTION_TIMEOUT_MIN_MS +
        (rand() % (RAFT_ELECTION_TIMEOUT_MAX_MS - RAFT_ELECTION_TIMEOUT_MIN_MS));
    printf("[RAFT] %s -> CANDIDATE (term=%llu, timeout=%llums)\n",
           r->cfg.node_id, (unsigned long long)r->current_term,
           (unsigned long long)r->election_timeout_ms);
}

static void raft_become_leader(raft_t* r) {
    r->role = RAFT_LEADER;
    int self_idx = raft_self_index(r);
    printf("[RAFT] %s -> LEADER (term=%llu)\n", r->cfg.node_id,
           (unsigned long long)r->current_term);

    /* 初始化 leader 状态 */
    uint64_t last_log_idx = r->log_count > 0 ? r->log[r->log_count - 1].index : 0;
    for (int i = 0; i < r->cfg.num_peers; i++) {
        r->next_index[i] = last_log_idx + 1;
        r->match_index[i] = 0;
    }
    r->match_index[self_idx] = last_log_idx;

    /* 更新 leader peer 信息 */
    snprintf(r->leader_peer.id, sizeof(r->leader_peer.id), "%s", r->cfg.node_id);
    r->leader_peer = r->cfg.peers[self_idx];
    r->leader_known = 1;

    /* 立即发送心跳 */
    r->last_election_ms = 0; /* 触发立即发送 */
}

/* 检查自身日志是否至少和 candidate 一样新 */
static int raft_log_is_up_to_date(raft_t* r, uint64_t last_idx, uint64_t last_term) {
    if (r->log_count == 0) return 1; /* no log, accept any */
    uint64_t my_last_term = r->log[r->log_count - 1].term;
    uint64_t my_last_idx = r->log[r->log_count - 1].index;
    if (last_term != my_last_term) return last_term > my_last_term;
    return last_idx >= my_last_idx;
}

/* 将已提交的日志应用到状态机 */
static void raft_apply_committed(raft_t* r) {
    while (r->last_applied < r->commit_index) {
        uint64_t idx = r->last_applied + 1;

        /* 查找日志条目 */
        raft_log_entry_t* e = NULL;
        for (size_t i = 0; i < r->log_count; i++) {
            if (r->log[i].index == idx) {
                e = &r->log[i];
                break;
            }
        }

        if (e && r->apply_fn) {
            raft_entry_t entry;
            entry.term = e->term;
            entry.index = e->index;
            entry.type = e->type;
            entry.key = e->key;
            entry.key_len = e->key_len;
            entry.value = e->value;
            entry.value_len = e->value_len;
            r->apply_fn(r->state_machine, &entry);
        }

        r->last_applied = idx;
    }
}

/* 处理 RequestVote */
static void raft_handle_vote_request(raft_t* r, SOCKET fd, uint8_t* data, size_t data_len) {
    if (data_len < 21) return;
    size_t off = 0;
    uint64_t term = read_u64(data + off); off += 8;
    uint32_t id_len = read_u32(data + off); off += 4;
    if (off + id_len > data_len) return;
    char candidate_id[RAFT_NODE_ID_LEN];
    memcpy(candidate_id, data + off, id_len);
    candidate_id[id_len] = '\0';
    off += id_len;
    if (off + 16 > data_len) return;
    uint64_t last_log_idx = read_u64(data + off); off += 8;
    uint64_t last_log_term = read_u64(data + off); off += 8;

    MUTEX_LOCK(&r->mutex);

    int grant = 0;
    if (term < r->current_term) {
        /* 拒绝：term 太旧 */
    } else {
        if (term > r->current_term) {
            raft_become_follower(r, term);
        }
        /* 可以投票：尚未投票给其他人，且 candidate 日志足够新 */
        if ((r->voted_for[0] == '\0' || strcmp(r->voted_for, candidate_id) == 0) &&
            raft_log_is_up_to_date(r, last_log_idx, last_log_term)) {
            grant = 1;
            snprintf(r->voted_for, sizeof(r->voted_for), "%s", candidate_id);
            r->last_heartbeat_ms = thread_time_ms(); /* 重置选举计时器 */
        }
    }

    MUTEX_UNLOCK(&r->mutex);

    rpc_send_vote_response(fd, r->current_term, grant);
    if (grant) {
        raft_log_save(r);
        printf("[RAFT] Voted for %s (term=%llu)\n", candidate_id, (unsigned long long)term);
    }
}

/* 处理 RequestVote 响应 */
static void raft_handle_vote_response(raft_t* r, uint8_t* data, size_t data_len) {
    if (data_len < 10) return;
    uint64_t term = read_u64(data + 1);
    int granted = data[9] != 0;

    MUTEX_LOCK(&r->mutex);

    if (r->role != RAFT_CANDIDATE) {
        MUTEX_UNLOCK(&r->mutex);
        return;
    }

    if (term > r->current_term) {
        raft_become_follower(r, term);
        raft_log_save(r);
        MUTEX_UNLOCK(&r->mutex);
        return;
    }

    if (granted) {
        r->votes_received++;
        printf("[RAFT] Got vote: %d/%d\n", r->votes_received, r->cfg.num_peers);
        if (r->votes_received > r->cfg.num_peers / 2) {
            raft_become_leader(r);
        }
    }

    MUTEX_UNLOCK(&r->mutex);
}

/* 处理 AppendEntries */
static void raft_handle_append_request(raft_t* r, SOCKET fd, uint8_t* data, size_t data_len) {
    size_t off = 0;
    if (off + 1 > data_len) return;
    off++; /* skip type */
    if (off + 8 > data_len) return;
    uint64_t term = read_u64(data + off); off += 8;
    uint32_t id_len = read_u32(data + off); off += 4;
    if (off + id_len > data_len) return;
    char leader_id[RAFT_NODE_ID_LEN];
    memcpy(leader_id, data + off, id_len);
    leader_id[id_len] = '\0';
    off += id_len;
    if (off + 32 > data_len) return;
    uint64_t prev_idx = read_u64(data + off); off += 8;
    uint64_t prev_term = read_u64(data + off); off += 8;
    uint64_t n_entries = read_u64(data + off); off += 8;
    uint64_t leader_commit = read_u64(data + off); off += 8;

    MUTEX_LOCK(&r->mutex);

    int success = 0;
    uint64_t last_idx = prev_idx;

    /* 1. 如果 term < currentTerm，拒绝 */
    if (term < r->current_term) {
        /* 获取 last log index */
        last_idx = r->log_count > 0 ? r->log[r->log_count - 1].index : 0;
        MUTEX_UNLOCK(&r->mutex);
        rpc_send_append_response(fd, r->current_term, 0, last_idx);
        return;
    }

    /* 2. 如果是有效 leader，更新状态 */
    if (term > r->current_term) {
        raft_become_follower(r, term);
    } else {
        /* 如果收到同一 term 的不同 leader 的 AppendEntries，接受他 */
        if (r->role == RAFT_CANDIDATE) {
            raft_become_follower(r, term);
        }
    }

    r->last_heartbeat_ms = thread_time_ms();
    r->role = RAFT_FOLLOWER;

    /* 更新 leader 信息 */
    {
        int idx = raft_find_peer(r, leader_id);
        if (idx >= 0) {
            r->leader_peer = r->cfg.peers[idx];
            r->leader_known = 1;
        }
    }

    /* 3. 检查 prevLogIndex/prevLogTerm */
    if (prev_idx > 0) {
        int found = 0;
        for (size_t i = 0; i < r->log_count; i++) {
            if (r->log[i].index == prev_idx) {
                if (r->log[i].term == prev_term) {
                    found = 1;
                }
                break;
            }
        }
        if (!found && prev_idx != 0) {
            /* 冲突：删除冲突及之后的条目 */
            size_t new_count = 0;
            for (size_t i = 0; i < r->log_count; i++) {
                if (r->log[i].index < prev_idx) {
                    new_count = i + 1;
                }
            }
            /* 释放被删除的条目 */
            for (size_t i = new_count; i < r->log_count; i++) {
                kv_free(r->log[i].key);
                kv_free(r->log[i].value);
            }
            r->log_count = new_count;
            last_idx = r->log_count > 0 ? r->log[r->log_count - 1].index : 0;
            MUTEX_UNLOCK(&r->mutex);
            raft_log_save(r);
            rpc_send_append_response(fd, r->current_term, 0, last_idx);
            return;
        }
    }

    /* 4. 追加新条目 */
    for (uint64_t i = 0; i < n_entries; i++) {
        if (off + 25 > data_len) break;
        uint64_t e_term = read_u64(data + off); off += 8;
        uint64_t e_index = read_u64(data + off); off += 8;
        uint8_t e_type = data[off++];
        uint64_t key_len = read_u64(data + off); off += 8;
        if (off + key_len > data_len) break;
        char* key = kv_malloc((size_t)key_len);
        memcpy(key, data + off, (size_t)key_len);
        off += (size_t)key_len;
        uint64_t val_len = read_u64(data + off); off += 8;
        char* val = NULL;
        if (val_len > 0) {
            if (off + val_len > data_len) { kv_free(key); break; }
            val = kv_malloc((size_t)val_len);
            memcpy(val, data + off, (size_t)val_len);
            off += (size_t)val_len;
        }

        /* 检查是否与现有日志冲突 */
        int found_existing = 0;
        for (size_t j = 0; j < r->log_count; j++) {
            if (r->log[j].index == e_index) {
                if (r->log[j].term != e_term) {
                    /* 冲突：删除此条目及之后 */
                    for (size_t k = j; k < r->log_count; k++) {
                        kv_free(r->log[k].key);
                        kv_free(r->log[k].value);
                    }
                    r->log_count = j;
                } else {
                    /* 已存在且匹配，跳过 */
                    kv_free(key);
                    kv_free(val);
                    key = NULL;
                }
                found_existing = 1;
                break;
            }
        }
        (void)found_existing;

        if (key) {
            /* 扩容 */
            if (r->log_count >= r->log_capacity) {
                r->log_capacity *= 2;
                r->log = kv_realloc(r->log, r->log_capacity * sizeof(raft_log_entry_t));
            }
            raft_log_entry_t* ne = &r->log[r->log_count++];
            ne->term = e_term;
            ne->index = e_index;
            ne->type = e_type;
            ne->key = key;
            ne->key_len = (size_t)key_len;
            ne->value = val;
            ne->value_len = val ? (size_t)val_len : 0;
        }

        last_idx = e_index;
    }

    /* 5. 更新 commitIndex */
    if (leader_commit > r->commit_index) {
        uint64_t last_log = r->log_count > 0 ? r->log[r->log_count - 1].index : 0;
        r->commit_index = leader_commit < last_log ? leader_commit : last_log;
    }

    success = 1;
    MUTEX_UNLOCK(&r->mutex);

    raft_log_save(r);
    rpc_send_append_response(fd, r->current_term, success, last_idx);

    /* apply committed */
    MUTEX_LOCK(&r->mutex);
    raft_apply_committed(r);
    MUTEX_UNLOCK(&r->mutex);
}

/* 处理 AppendEntries 响应 */
static void raft_handle_append_response(raft_t* r, uint8_t* data, size_t data_len) {
    if (data_len < 18) return;
    uint64_t term = read_u64(data + 1);
    int success = data[9] != 0;
    uint64_t last_idx = read_u64(data + 10);

    MUTEX_LOCK(&r->mutex);

    if (r->role != RAFT_LEADER) {
        MUTEX_UNLOCK(&r->mutex);
        return;
    }

    if (term > r->current_term) {
        raft_become_follower(r, term);
        raft_log_save(r);
        MUTEX_UNLOCK(&r->mutex);
        return;
    }

    /* 这里需要知道是哪个 peer 的响应……简化处理：
     * 单次 AppendEntries 发送给所有 peer，我们通过 last_idx 更新 match_index。
     * 实际生产代码需要更好的 peer 追踪机制。 */
    /* 对于演示目的，遍历所有 peer 更新 match_index */
    for (int i = 0; i < r->cfg.num_peers; i++) {
        if (i == raft_self_index(r)) continue;
        if (success) {
            if (last_idx > r->match_index[i]) {
                r->match_index[i] = last_idx;
                r->next_index[i] = last_idx + 1;
            }
        } else {
            if (r->next_index[i] > 1) {
                r->next_index[i]--;
            }
        }
    }

    /* 更新 commitIndex */
    uint64_t last_log_idx = r->log_count > 0 ? r->log[r->log_count - 1].index : 0;
    for (uint64_t n = r->commit_index + 1; n <= last_log_idx; n++) {
        /* 查找该索引的 term */
        uint64_t n_term = 0;
        for (size_t j = 0; j < r->log_count; j++) {
            if (r->log[j].index == n) { n_term = r->log[j].term; break; }
        }
        if (n_term != r->current_term) continue;

        int count = 1; /* 自己的票 */
        for (int i = 0; i < r->cfg.num_peers; i++) {
            if (i == raft_self_index(r)) continue;
            if (r->match_index[i] >= n) count++;
        }
        if (count > r->cfg.num_peers / 2) {
            r->commit_index = n;
        }
    }

    MUTEX_UNLOCK(&r->mutex);

    /* apply committed */
    MUTEX_LOCK(&r->mutex);
    raft_apply_committed(r);
    MUTEX_UNLOCK(&r->mutex);
}

/* Leader 发送心跳 / 日志复制 */
static void raft_leader_send_heartbeat(raft_t* r) {
    int self_idx = raft_self_index(r);
    uint64_t prev_idx, prev_term;

    for (int i = 0; i < r->cfg.num_peers; i++) {
        if (i == self_idx) continue;

        /* 准备发送的日志条目 */
        uint64_t next = r->next_index[i];
        prev_idx = next - 1;
        prev_term = 0;

        if (prev_idx > 0) {
            for (size_t j = 0; j < r->log_count; j++) {
                if (r->log[j].index == prev_idx) {
                    prev_term = r->log[j].term;
                    break;
                }
            }
        }

        /* 收集要发送的日志 */
        raft_log_entry_t send_entries[128];
        int n_send = 0;
        uint64_t send_idx = next;
        for (size_t j = 0; j < r->log_count && n_send < 128; j++) {
            if (r->log[j].index >= send_idx) {
                send_entries[n_send++] = r->log[j];
                send_idx++;
            }
        }

        SOCKET fd = raft_connect_peer(&r->cfg.peers[i]);
        if (fd == INVALID_SOCKET) {
            continue;
        }

        rpc_send_append_request(fd, r->current_term, r->cfg.node_id,
                                 prev_idx, prev_term, send_entries, n_send,
                                 r->commit_index);

        /* 读取响应 */
        uint32_t resp_len = 0;
        int ret = recv(fd, (char*)&resp_len, 4, 0);
        if (ret == 4) {
            uint8_t resp_buf[256];
            int total = recv(fd, (char*)resp_buf, (int)resp_len, 0);
            if (total == (int)resp_len) {
                raft_handle_append_response(r, resp_buf, resp_len);
            }
        }
        close_socket(fd);
    }
}

/* Candidate 发起选举 */
static void raft_candidate_start_election(raft_t* r) {
    r->votes_received = 1; /* vote for self */
    r->election_timeout_ms = RAFT_ELECTION_TIMEOUT_MIN_MS +
        (rand() % (RAFT_ELECTION_TIMEOUT_MAX_MS - RAFT_ELECTION_TIMEOUT_MIN_MS));
    r->last_heartbeat_ms = thread_time_ms(); /* 重置计时器 */

    uint64_t last_log_idx = r->log_count > 0 ? r->log[r->log_count - 1].index : 0;
    uint64_t last_log_term = r->log_count > 0 ? r->log[r->log_count - 1].term : 0;

    int self_idx = raft_self_index(r);
    for (int i = 0; i < r->cfg.num_peers; i++) {
        if (i == self_idx) continue;

        SOCKET fd = raft_connect_peer(&r->cfg.peers[i]);
        if (fd == INVALID_SOCKET) continue;

        rpc_send_vote_request(fd, r->current_term, r->cfg.node_id,
                               last_log_idx, last_log_term);

        /* 读取响应 */
        uint32_t resp_len = 0;
        int ret = recv(fd, (char*)&resp_len, 4, 0);
        if (ret == 4) {
            uint8_t resp_buf[64];
            int total = recv(fd, (char*)resp_buf, (int)resp_len, 0);
            if (total == (int)resp_len) {
                raft_handle_vote_response(r, resp_buf, resp_len);
            }
        }
        close_socket(fd);
    }

    raft_log_save(r);
}

/* 处理客户端 Propose 请求 */
static void raft_handle_propose_request(raft_t* r, SOCKET fd, uint8_t* data, size_t data_len) {
    size_t off = 0;
    if (off + 1 > data_len) return;
    uint8_t cmd_type = data[off++];
    if (off + 8 > data_len) return;
    uint64_t key_len = read_u64(data + off); off += 8;
    if (off + key_len > data_len) return;
    char* key = kv_malloc((size_t)key_len + 1);
    memcpy(key, data + off, (size_t)key_len);
    key[key_len] = '\0';
    off += (size_t)key_len;
    if (off + 8 > data_len) { kv_free(key); return; }
    uint64_t val_len = read_u64(data + off); off += 8;
    char* val = NULL;
    if (val_len > 0) {
        if (off + val_len > data_len) { kv_free(key); return; }
        val = kv_malloc((size_t)val_len);
        memcpy(val, data + off, (size_t)val_len);
        off += (size_t)val_len;
    }

    MUTEX_LOCK(&r->mutex);

    /* 只有 Leader 可以处理 propose */
    if (r->role != RAFT_LEADER) {
        if (r->leader_known) {
            rpc_send_propose_response(fd, r->current_term, 0,
                                       r->leader_peer.id, r->leader_peer.host,
                                       r->leader_peer.port);
        } else {
            rpc_send_propose_response(fd, r->current_term, 0, NULL, NULL, 0);
        }
        kv_free(key);
        kv_free(val);
        MUTEX_UNLOCK(&r->mutex);
        return;
    }

    /* 扩容日志 */
    if (r->log_count >= r->log_capacity) {
        r->log_capacity *= 2;
        r->log = kv_realloc(r->log, r->log_capacity * sizeof(raft_log_entry_t));
    }

    uint64_t new_idx = r->log_count > 0 ? r->log[r->log_count - 1].index + 1 : 1;
    raft_log_entry_t* ne = &r->log[r->log_count++];
    ne->term = r->current_term;
    ne->index = new_idx;
    ne->type = cmd_type;
    ne->key = key;
    ne->key_len = (size_t)key_len;
    ne->value = val;
    ne->value_len = val ? (size_t)val_len : 0;

    /* 立即尝试复制 */
    r->match_index[raft_self_index(r)] = new_idx;

    MUTEX_UNLOCK(&r->mutex);
    raft_log_save(r);

    /* 发送 AppendEntries 给所有 follower */
    raft_leader_send_heartbeat(r);

    /* 等待提交 */
    int max_wait = 100; /* 最多等待 100ms */
    while (max_wait-- > 0) {
        MUTEX_LOCK(&r->mutex);
        if (r->commit_index >= new_idx) {
            raft_apply_committed(r);
            MUTEX_UNLOCK(&r->mutex);
            rpc_send_propose_response(fd, r->current_term, 1, NULL, NULL, 0);
            return;
        }
        MUTEX_UNLOCK(&r->mutex);
        thread_sleep_ms(1);
    }

    rpc_send_propose_response(fd, r->current_term, 0, NULL, NULL, 0);
}

/* ================================================================
 * 主事件循环
 * ================================================================ */

#ifdef _WIN32
static DWORD WINAPI raft_event_loop(LPVOID arg) {
#else
static void* raft_event_loop(void* arg) {
#endif
    raft_t* r = (raft_t*)arg;
    r->running = 1;

    printf("[RAFT] %s event loop started\n", r->cfg.node_id);

    while (r->running) {
        /* 使用 select 等待连接或超时 */
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(r->listen_fd, &read_fds);

        struct timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = 50000; /* 50ms 轮询 */

        int ret = select((int)r->listen_fd + 1, &read_fds, NULL, NULL, &tv);
        if (ret < 0) {
            if (
#ifdef _WIN32
                WSAGetLastError() == WSAEINTR
#else
                errno == EINTR
#endif
            ) continue;
            break;
        }

        uint64_t now = thread_time_ms();

        MUTEX_LOCK(&r->mutex);

        /* --- 选举超时检查 --- */
        if (r->role != RAFT_LEADER) {
            if (now - r->last_heartbeat_ms >= r->election_timeout_ms) {
                raft_become_candidate(r);
                raft_log_save(r);
            }
        }

        /* --- Leader 心跳 --- */
        if (r->role == RAFT_LEADER) {
            if (now - r->last_election_ms >= RAFT_HEARTBEAT_INTERVAL_MS) {
                r->last_election_ms = now;
                MUTEX_UNLOCK(&r->mutex);
                raft_leader_send_heartbeat(r);
                MUTEX_LOCK(&r->mutex);
            }
        }

        /* --- Candidate 发起选举 --- */
        if (r->role == RAFT_CANDIDATE) {
            /* 选举在超时触发时已经发起 */
            if (now - r->last_heartbeat_ms >= r->election_timeout_ms) {
                MUTEX_UNLOCK(&r->mutex);
                raft_candidate_start_election(r);
                MUTEX_LOCK(&r->mutex);
            }
        }

        MUTEX_UNLOCK(&r->mutex);

        /* --- 接受新连接 --- */
        if (FD_ISSET(r->listen_fd, &read_fds)) {
            struct sockaddr_in client_addr;
            socklen_t addr_len = sizeof(client_addr);
            SOCKET client_fd = accept(r->listen_fd, (struct sockaddr*)&client_addr, &addr_len);
            if (client_fd != INVALID_SOCKET) {
                /* 读取消息 */
                uint32_t msg_len = 0;
                int n = recv(client_fd, (char*)&msg_len, 4, 0);
                if (n == 4 && msg_len > 0 && msg_len < RAFT_MAX_MSG_SIZE) {
                    uint8_t* msg = kv_malloc(msg_len);
                    int total = recv(client_fd, (char*)msg, (int)msg_len, 0);
                    if (total == (int)msg_len && msg_len > 0) {
                        switch (msg[0]) {
                        case RAFT_RPC_VOTE_REQ:
                            raft_handle_vote_request(r, client_fd, msg, msg_len);
                            break;
                        case RAFT_RPC_APPEND_REQ:
                            raft_handle_append_request(r, client_fd, msg, msg_len);
                            break;
                        case RAFT_RPC_PROPOSE_REQ:
                            raft_handle_propose_request(r, client_fd, msg, msg_len);
                            break;
                        default:
                            break;
                        }
                    }
                    kv_free(msg);
                }
                close_socket(client_fd);
            }
        }
    }

    printf("[RAFT] %s event loop stopped\n", r->cfg.node_id);
    return 0;
}

/* ================================================================
 * 公共 API
 * ================================================================ */

raft_t* raft_create(raft_config_t* cfg, void* state_machine, raft_apply_cb apply) {
    if (!cfg || cfg->num_peers < 2 || cfg->num_peers > RAFT_MAX_NODES) return NULL;

    raft_t* r = kv_malloc(sizeof(raft_t));
    if (!r) return NULL;
    memset(r, 0, sizeof(raft_t));

    memcpy(&r->cfg, cfg, sizeof(raft_config_t));
    r->state_machine = state_machine;
    r->apply_fn = apply;
    r->role = RAFT_FOLLOWER;
    r->current_term = 0;
    r->commit_index = 0;
    r->last_applied = 0;
    r->votes_received = 0;
    r->running = 0;
    r->leader_known = 0;

    r->election_timeout_ms = RAFT_ELECTION_TIMEOUT_MIN_MS +
        (rand() % (RAFT_ELECTION_TIMEOUT_MAX_MS - RAFT_ELECTION_TIMEOUT_MIN_MS));
    r->last_heartbeat_ms = thread_time_ms();
    r->last_election_ms = 0;

    memset(r->voted_for, 0, sizeof(r->voted_for));
    memset(&r->leader_peer, 0, sizeof(r->leader_peer));

    /* 分配 leader 状态数组 */
    r->next_index = kv_malloc((size_t)cfg->num_peers * sizeof(uint64_t));
    r->match_index = kv_malloc((size_t)cfg->num_peers * sizeof(uint64_t));
    memset(r->next_index, 0, (size_t)cfg->num_peers * sizeof(uint64_t));
    memset(r->match_index, 0, (size_t)cfg->num_peers * sizeof(uint64_t));

    /* 加载持久化日志 */
    if (raft_log_load(r) != 0) {
        kv_free(r->next_index);
        kv_free(r->match_index);
        kv_free(r);
        return NULL;
    }

    MUTEX_INIT(&r->mutex);
    MUTEX_INIT(&r->propose_mutex);

    r->listen_fd = INVALID_SOCKET;

    return r;
}

void raft_destroy(raft_t* r) {
    if (!r) return;
    raft_stop(r);
    raft_join(r);

    if (r->listen_fd != INVALID_SOCKET) close_socket(r->listen_fd);
    for (size_t i = 0; i < r->log_count; i++) {
        kv_free(r->log[i].key);
        kv_free(r->log[i].value);
    }
    kv_free(r->log);
    kv_free(r->next_index);
    kv_free(r->match_index);
    MUTEX_DESTROY(&r->mutex);
    MUTEX_DESTROY(&r->propose_mutex);
    kv_free(r);
}

int raft_start(raft_t* r) {
    if (!r) return -1;

#ifdef _WIN32
    WSADATA wsa_data;
    WSAStartup(MAKEWORD(2, 2), &wsa_data);
#endif

    r->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (r->listen_fd == INVALID_SOCKET) return -1;

    int opt = 1;
    setsockopt(r->listen_fd, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((unsigned short)r->cfg.listen_port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(r->listen_fd, (struct sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
        printf("[RAFT] Failed to bind port %d\n", r->cfg.listen_port);
        close_socket(r->listen_fd);
        r->listen_fd = INVALID_SOCKET;
        return -1;
    }

    if (listen(r->listen_fd, SOMAXCONN) == SOCKET_ERROR) {
        close_socket(r->listen_fd);
        r->listen_fd = INVALID_SOCKET;
        return -1;
    }

    printf("[RAFT] %s listening on port %d\n", r->cfg.node_id, r->cfg.listen_port);

    if (thread_create(&r->thread, raft_event_loop, r) != 0) {
        close_socket(r->listen_fd);
        r->listen_fd = INVALID_SOCKET;
        return -1;
    }

    return 0;
}

void raft_stop(raft_t* r) {
    if (!r) return;
    r->running = 0;
}

void raft_join(raft_t* r) {
    if (!r) return;
    if (r->listen_fd != INVALID_SOCKET) {
        thread_join(r->thread);
    }
}

int raft_propose(raft_t* r, uint8_t type, const char* key, size_t key_len,
                 const char* value, size_t value_len) {
    if (!r || !key || key_len == 0) return -1;

    MUTEX_LOCK(&r->mutex);

    if (r->role != RAFT_LEADER) {
        if (r->leader_known) {
            /* 转发到 Leader */
            SOCKET fd = raft_connect_peer(&r->leader_peer);
            if (fd == INVALID_SOCKET) {
                MUTEX_UNLOCK(&r->mutex);
                return -1;
            }

            uint8_t buf[4096];
            size_t off = 0;
            buf[off++] = RAFT_RPC_PROPOSE_REQ;
            buf[off++] = type;
            write_u64(buf + off, (uint64_t)key_len); off += 8;
            memcpy(buf + off, key, key_len); off += key_len;
            write_u64(buf + off, (uint64_t)value_len); off += 8;
            if (value_len > 0) memcpy(buf + off, value, value_len);
            off += value_len;

            uint32_t total = (uint32_t)off;
            send(fd, (const char*)&total, 4, 0);
            send(fd, (const char*)buf, (int)off, 0);

            /* 读取响应 */
            uint32_t resp_len = 0;
            int ret = recv(fd, (char*)&resp_len, 4, 0);
            int result = -1;
            if (ret == 4 && resp_len > 0) {
                uint8_t* resp = kv_malloc(resp_len);
                int total_bytes = recv(fd, (char*)resp, (int)resp_len, 0);
                if (total_bytes == (int)resp_len && resp_len >= 10) {
                    /* 检查是否成功 */
                    int success = resp[9] != 0;
                    if (success) result = 0;
                    else {
                        /* 如果响应中有新的 leader 信息，更新 */
                        uint32_t lid_len = read_u32(resp + 10);
                        if (lid_len > 0 && lid_len < RAFT_NODE_ID_LEN && 14 + lid_len <= resp_len) {
                            memcpy(r->leader_peer.id, resp + 14, lid_len);
                            r->leader_peer.id[lid_len] = '\0';
                            uint32_t host_off = (uint32_t)(14 + lid_len);
                            if (host_off + 4 <= resp_len) {
                                uint32_t host_len = read_u32(resp + host_off);
                                if (host_len > 0 && host_len < 64 && host_off + 4 + host_len + 4 <= resp_len) {
                                    memcpy(r->leader_peer.host, resp + host_off + 4, host_len);
                                    r->leader_peer.host[host_len] = '\0';
                                    r->leader_peer.port = (int)read_u32(resp + host_off + 4 + host_len);
                                    r->leader_known = 1;
                                }
                            }
                        }
                    }
                }
                kv_free(resp);
            }
            close_socket(fd);
            MUTEX_UNLOCK(&r->mutex);
            return result;
        }
        MUTEX_UNLOCK(&r->mutex);
        return -1;
    }

    MUTEX_UNLOCK(&r->mutex);

    /* 我们是 Leader，直接在本地处理 */
    /* 通过 propose RPC 自处理（简化：直接通过内部 API 处理） */
    MUTEX_LOCK(&r->mutex);
    if (r->log_count >= r->log_capacity) {
        r->log_capacity *= 2;
        r->log = kv_realloc(r->log, r->log_capacity * sizeof(raft_log_entry_t));
    }

    uint64_t new_idx = r->log_count > 0 ? r->log[r->log_count - 1].index + 1 : 1;
    raft_log_entry_t* ne = &r->log[r->log_count++];
    ne->term = r->current_term;
    ne->index = new_idx;
    ne->type = type;
    ne->key = kv_malloc(key_len);
    memcpy(ne->key, key, key_len);
    ne->key_len = key_len;
    ne->value = value_len > 0 ? kv_malloc(value_len) : NULL;
    if (ne->value) memcpy(ne->value, value, value_len);
    ne->value_len = value_len;

    r->match_index[raft_self_index(r)] = new_idx;
    MUTEX_UNLOCK(&r->mutex);
    raft_log_save(r);

    /* 复制到 followers */
    raft_leader_send_heartbeat(r);

    return 0;
}

int raft_is_leader(raft_t* r) {
    if (!r) return 0;
    MUTEX_LOCK(&r->mutex);
    int is_leader = (r->role == RAFT_LEADER);
    MUTEX_UNLOCK(&r->mutex);
    return is_leader;
}

const raft_peer_t* raft_get_leader_peer(raft_t* r) {
    if (!r) return NULL;
    MUTEX_LOCK(&r->mutex);
    if (r->leader_known) {
        MUTEX_UNLOCK(&r->mutex);
        return &r->leader_peer;
    }
    MUTEX_UNLOCK(&r->mutex);
    return NULL;
}

const char* raft_role_str(raft_role_t role) {
    switch (role) {
    case RAFT_FOLLOWER:  return "Follower";
    case RAFT_CANDIDATE: return "Candidate";
    case RAFT_LEADER:    return "Leader";
    default: return "Unknown";
    }
}

void raft_status(raft_t* r, uint64_t* out_term, raft_role_t* out_role,
                 uint64_t* out_commit_index, uint64_t* out_last_applied) {
    if (!r) return;
    MUTEX_LOCK(&r->mutex);
    if (out_term) *out_term = r->current_term;
    if (out_role) *out_role = r->role;
    if (out_commit_index) *out_commit_index = r->commit_index;
    if (out_last_applied) *out_last_applied = r->last_applied;
    MUTEX_UNLOCK(&r->mutex);
}