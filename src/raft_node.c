#include "raft.h"
#include "resp_server.h"
#include "metrics_server.h"
#include "kv_store.h"
#include "manifest.h"
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
#include <dirent.h>
#include <sys/stat.h>
#endif

/* ================================================================
 * Raft 状态机回调：将 committed 日志应用到 kv_store
 * ================================================================ */
static int raft_apply_to_kv(void* state, raft_entry_t* entry) {
    kv_store_t* db = (kv_store_t*)state;
    if (!db || !entry) return -1;

    if (entry->type == 0) { /* PUT */
        return kv_put_internal(db, entry->key, entry->key_len, entry->value, entry->value_len);
    } else if (entry->type == 1) { /* DELETE */
        return kv_delete_internal(db, entry->key, entry->key_len);
    }
    return -1;
}

/* ================================================================
 * Raft 快照回调：将 kv_store 序列化到快照文件
 * ================================================================ */
static int raft_snapshot_to_kv(void* state, const char* file_path,
                                uint64_t last_included_index, uint64_t last_included_term) {
    kv_store_t* db = (kv_store_t*)state;
    if (!db || !file_path) return -1;

    /* 使用 kv_backup 创建完整备份 */
    char tmp_dir[512];
    snprintf(tmp_dir, sizeof(tmp_dir), "%s.tmp", file_path);

    if (kv_backup(db, tmp_dir) != 0) {
        printf("[SNAPSHOT] ERROR: kv_backup failed\n");
        return -1;
    }

    /* 统计文件数量与总大小（用于 snapshot.meta） */
    char search_path[1024];
    snprintf(search_path, sizeof(search_path), "%s/*.sst", tmp_dir);

    uint32_t file_count = 0;
    uint64_t total_size = 0;

#ifdef _WIN32
    WIN32_FIND_DATA pre_find_data;
    HANDLE pre_h_find = FindFirstFile(search_path, &pre_find_data);
    if (pre_h_find != INVALID_HANDLE_VALUE) {
        do {
            file_count++;
            total_size += ((uint64_t)pre_find_data.nFileSizeHigh << 32) | pre_find_data.nFileSizeLow;
        } while (FindNextFile(pre_h_find, &pre_find_data));
        FindClose(pre_h_find);
    }
#endif

    /* 创建快照文件 */
    FILE* f = fopen(file_path, "wb");
    if (!f) {
        printf("[SNAPSHOT] ERROR: Cannot create snapshot file %s\n", file_path);
        /* 清理临时目录 */
        {
            char cmd[2048];
#ifdef _WIN32
            snprintf(cmd, sizeof(cmd), "rmdir /s /q \"%s\" 2>nul", tmp_dir);
            system(cmd);
#endif
        }
        return -1;
    }

    /* 写入头部：last_included_index(8) + last_included_term(8) */
    if (fwrite(&last_included_index, 8, 1, f) != 1 ||
        fwrite(&last_included_term, 8, 1, f) != 1) {
        printf("[SNAPSHOT] ERROR: Failed to write header\n");
        fclose(f);
        remove(file_path);
        {
            char cmd[2048];
#ifdef _WIN32
            snprintf(cmd, sizeof(cmd), "rmdir /s /q \"%s\" 2>nul", tmp_dir);
            system(cmd);
#endif
        }
        return -1;
    }

    /* 打包 MANIFEST 文件到快照中 */
    {
        char manifest_src[1024];
        snprintf(manifest_src, sizeof(manifest_src), "%s/" MANIFEST_FILE_NAME, tmp_dir);
        FILE* mf = fopen(manifest_src, "rb");
        if (mf) {
            fseek(mf, 0, SEEK_END);
            long mf_size = ftell(mf);
            fseek(mf, 0, SEEK_SET);

            uint32_t name_len = (uint32_t)strlen(MANIFEST_FILE_NAME);
            fwrite(&name_len, 4, 1, f);
            fwrite(MANIFEST_FILE_NAME, 1, name_len, f);

            uint64_t file_sz = (uint64_t)mf_size;
            fwrite(&file_sz, 8, 1, f);

            uint8_t* buf = kv_malloc((size_t)mf_size);
            if (buf) {
                if (fread(buf, 1, (size_t)mf_size, mf) == (size_t)mf_size) {
                    fwrite(buf, 1, (size_t)mf_size, f);
                }
                kv_free(buf);
            }
            fclose(mf);
        }
    }

    /* 遍历备份目录中的所有 SSTable 文件，写入快照 */
#ifdef _WIN32
    WIN32_FIND_DATA find_data;
    HANDLE h_find = FindFirstFile(search_path, &find_data);
    if (h_find != INVALID_HANDLE_VALUE) {
        do {
            char sst_path[2048];
            snprintf(sst_path, sizeof(sst_path), "%s/%s", tmp_dir, find_data.cFileName);
            FILE* sf = fopen(sst_path, "rb");
            if (!sf) continue;

            fseek(sf, 0, SEEK_END);
            long sst_size = ftell(sf);
            fseek(sf, 0, SEEK_SET);

            /* 写入文件名长度 + 文件名 */
            uint32_t name_len = (uint32_t)strlen(find_data.cFileName);
            if (fwrite(&name_len, 4, 1, f) != 1 ||
                fwrite(find_data.cFileName, 1, name_len, f) != name_len) {
                printf("[SNAPSHOT] ERROR: Failed to write file entry header\n");
                fclose(sf);
                fclose(f);
                remove(file_path);
                FindClose(h_find);
                {
                    char cmd[2048];
#ifdef _WIN32
                    snprintf(cmd, sizeof(cmd), "rmdir /s /q \"%s\" 2>nul", tmp_dir);
                    system(cmd);
#endif
                }
                return -1;
            }

            /* 写入文件大小 + 文件内容 */
            uint64_t file_sz = (uint64_t)sst_size;
            if (fwrite(&file_sz, 8, 1, f) != 1) {
                printf("[SNAPSHOT] ERROR: Failed to write file size\n");
                fclose(sf);
                fclose(f);
                remove(file_path);
                FindClose(h_find);
                {
                    char cmd[2048];
#ifdef _WIN32
                    snprintf(cmd, sizeof(cmd), "rmdir /s /q \"%s\" 2>nul", tmp_dir);
                    system(cmd);
#endif
                }
                return -1;
            }

            uint8_t* buf = kv_malloc((size_t)sst_size);
            if (!buf) {
                printf("[SNAPSHOT] ERROR: Out of memory\n");
                fclose(sf);
                fclose(f);
                remove(file_path);
                FindClose(h_find);
                {
                    char cmd[2048];
#ifdef _WIN32
                    snprintf(cmd, sizeof(cmd), "rmdir /s /q \"%s\" 2>nul", tmp_dir);
                    system(cmd);
#endif
                }
                return -1;
            }

            if (fread(buf, 1, (size_t)sst_size, sf) != (size_t)sst_size) {
                printf("[SNAPSHOT] ERROR: Failed to read SST file %s\n", find_data.cFileName);
                kv_free(buf);
                fclose(sf);
                fclose(f);
                remove(file_path);
                FindClose(h_find);
                {
                    char cmd[2048];
#ifdef _WIN32
                    snprintf(cmd, sizeof(cmd), "rmdir /s /q \"%s\" 2>nul", tmp_dir);
                    system(cmd);
#endif
                }
                return -1;
            }

            fwrite(buf, 1, (size_t)sst_size, f);
            kv_free(buf);
            fclose(sf);
        } while (FindNextFile(h_find, &find_data));
        FindClose(h_find);
    }
#else
    /* Linux: 使用 opendir/readdir 遍历 SSTable 文件 */
    {
        DIR* dir = opendir(tmp_dir);
        if (dir) {
            struct dirent* entry;
            while ((entry = readdir(dir)) != NULL) {
                const char* name = entry->d_name;
                size_t name_len = strlen(name);
                /* 只处理 .sst 文件 */
                if (name_len < 4 || strcmp(name + name_len - 4, ".sst") != 0) continue;

                char sst_path[2048];
                snprintf(sst_path, sizeof(sst_path), "%s/%s", tmp_dir, name);
                struct stat st;
                if (stat(sst_path, &st) != 0 || !S_ISREG(st.st_mode)) continue;

                FILE* sf = fopen(sst_path, "rb");
                if (!sf) continue;

                /* 写入文件名长度 + 文件名 */
                uint32_t nlen = (uint32_t)name_len;
                if (fwrite(&nlen, 4, 1, f) != 1 ||
                    fwrite(name, 1, name_len, f) != name_len) {
                    printf("[SNAPSHOT] ERROR: Failed to write file entry header\n");
                    fclose(sf);
                    fclose(f);
                    remove(file_path);
                    closedir(dir);
                    { char cmd[2048]; snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", tmp_dir); system(cmd); }
                    return -1;
                }

                /* 写入文件大小 + 文件内容 */
                uint64_t file_sz = (uint64_t)st.st_size;
                if (fwrite(&file_sz, 8, 1, f) != 1) {
                    printf("[SNAPSHOT] ERROR: Failed to write file size\n");
                    fclose(sf);
                    fclose(f);
                    remove(file_path);
                    closedir(dir);
                    { char cmd[2048]; snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", tmp_dir); system(cmd); }
                    return -1;
                }

                uint8_t* buf = kv_malloc((size_t)st.st_size);
                if (!buf) {
                    printf("[SNAPSHOT] ERROR: Out of memory\n");
                    fclose(sf);
                    fclose(f);
                    remove(file_path);
                    closedir(dir);
                    { char cmd[2048]; snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", tmp_dir); system(cmd); }
                    return -1;
                }

                if (fread(buf, 1, (size_t)st.st_size, sf) != (size_t)st.st_size) {
                    printf("[SNAPSHOT] ERROR: Failed to read SST file %s\n", name);
                    kv_free(buf);
                    fclose(sf);
                    fclose(f);
                    remove(file_path);
                    closedir(dir);
                    { char cmd[2048]; snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", tmp_dir); system(cmd); }
                    return -1;
                }

                fwrite(buf, 1, (size_t)st.st_size, f);
                kv_free(buf);
                fclose(sf);
                file_count++;
                total_size += (uint64_t)st.st_size;
            }
            closedir(dir);
        }
    }
#endif

    /* 写入结束标记 */
    uint32_t end_marker = 0;
    fwrite(&end_marker, 4, 1, f);

    fclose(f);

    /* 创建 snapshot.meta 文件 */
    {
        char meta_path[1024];
        snprintf(meta_path, sizeof(meta_path), "%s.meta", file_path);
        FILE* mf = fopen(meta_path, "w");
        if (mf) {
            fprintf(mf, "{\"last_included_index\": %llu, \"last_included_term\": %llu, \"file_count\": %u, \"total_size\": %llu}\n",
                    (unsigned long long)last_included_index,
                    (unsigned long long)last_included_term,
                    (unsigned int)file_count,
                    (unsigned long long)total_size);
            fclose(mf);
            printf("[SNAPSHOT] Metadata written to %s\n", meta_path);
        } else {
            printf("[SNAPSHOT] WARNING: Failed to create snapshot.meta\n");
        }
    }

    /* 清理临时备份目录 */
    {
        char cmd[2048];
#ifdef _WIN32
        snprintf(cmd, sizeof(cmd), "rmdir /s /q \"%s\" 2>nul", tmp_dir);
        system(cmd);
#else
        snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", tmp_dir);
        system(cmd);
#endif
    }

    printf("[SNAPSHOT] Created snapshot at %s (index=%llu, term=%llu, files=%u, size=%llu)\n",
           file_path, (unsigned long long)last_included_index,
           (unsigned long long)last_included_term,
           (unsigned int)file_count, (unsigned long long)total_size);
    return 0;
}

/* ================================================================
 * Raft 快照恢复回调：从快照文件恢复 kv_store
 * ================================================================ */
static int raft_restore_from_kv(void* state, const char* file_path) {
    kv_store_t* db = (kv_store_t*)state;
    if (!db || !file_path) return -1;

    FILE* f = fopen(file_path, "rb");
    if (!f) {
        printf("[SNAPSHOT] ERROR: Cannot open snapshot file %s\n", file_path);
        return -1;
    }

    /* 跳过头部 */
    fseek(f, 16, SEEK_SET);

    /* 读取所有文件（SSTable + MANIFEST）并恢复到临时目录 */
    char tmp_dir[512];
    snprintf(tmp_dir, sizeof(tmp_dir), "%s.restore", file_path);

#ifdef _WIN32
    CreateDirectory(tmp_dir, NULL);
#else
    mkdir(tmp_dir, 0755);
#endif

    while (1) {
        uint32_t name_len;
        if (fread(&name_len, 4, 1, f) != 1 || name_len == 0) break;

        char fname[256];
        if (fread(fname, 1, name_len, f) != name_len) break;
        fname[name_len] = '\0';

        uint64_t file_sz;
        if (fread(&file_sz, 8, 1, f) != 1) break;

        char out_path[2048];
        snprintf(out_path, sizeof(out_path), "%s/%s", tmp_dir, fname);

        uint8_t* buf = kv_malloc((size_t)file_sz);
        if (!buf) {
            printf("[SNAPSHOT] ERROR: Out of memory during restore\n");
            fclose(f);
            {
                char cmd[2048];
#ifdef _WIN32
                snprintf(cmd, sizeof(cmd), "rmdir /s /q \"%s\" 2>nul", tmp_dir);
                system(cmd);
#else
                snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", tmp_dir);
                system(cmd);
#endif
            }
            return -1;
        }

        if (fread(buf, 1, (size_t)file_sz, f) != (size_t)file_sz) {
            printf("[SNAPSHOT] ERROR: Failed to read file %s from snapshot\n", fname);
            kv_free(buf);
            fclose(f);
            {
                char cmd[2048];
#ifdef _WIN32
                snprintf(cmd, sizeof(cmd), "rmdir /s /q \"%s\" 2>nul", tmp_dir);
                system(cmd);
#else
                snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", tmp_dir);
                system(cmd);
#endif
            }
            return -1;
        }

        FILE* wf = fopen(out_path, "wb");
        if (wf) {
            if (fwrite(buf, 1, (size_t)file_sz, wf) != (size_t)file_sz) {
                printf("[SNAPSHOT] ERROR: Failed to write %s to temp dir\n", fname);
                kv_free(buf);
                fclose(wf);
                fclose(f);
                {
                    char cmd[2048];
#ifdef _WIN32
                    snprintf(cmd, sizeof(cmd), "rmdir /s /q \"%s\" 2>nul", tmp_dir);
                    system(cmd);
#else
                    snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", tmp_dir);
                    system(cmd);
#endif
                }
                return -1;
            }
            fclose(wf);
        }
        kv_free(buf);
    }
    fclose(f);

    /* 使用 kv_import_snapshot_files 高效导入：直接复制 SSTable 文件到数据目录 */
    if (kv_import_snapshot_files(db, tmp_dir) != 0) {
        printf("[SNAPSHOT] ERROR: Failed to import snapshot from %s\n", file_path);
        /* 清理临时目录 */
        {
            char cmd[2048];
#ifdef _WIN32
            snprintf(cmd, sizeof(cmd), "rmdir /s /q \"%s\" 2>nul", tmp_dir);
            system(cmd);
#else
            snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", tmp_dir);
            system(cmd);
#endif
        }
        return -1;
    }

    /* 清理临时目录 */
    {
        char cmd[2048];
#ifdef _WIN32
        snprintf(cmd, sizeof(cmd), "rmdir /s /q \"%s\" 2>nul", tmp_dir);
        system(cmd);
#else
        snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", tmp_dir);
        system(cmd);
#endif
    }

    printf("[SNAPSHOT] Restored snapshot from %s\n", file_path);
    return 0;
}

/* ================================================================
 * 全局变量（信号处理）
 * ================================================================ */
static raft_t*          g_raft = NULL;
static resp_server_t*   g_resp = NULL;
static metrics_server_t* g_metrics = NULL;
static kv_store_t*      g_db = NULL;
static volatile int     g_running = 1;  /* 主循环控制标志 */

/* RESP HEALTH 命令回调：提供 Raft 状态信息 */
static int resp_health_raft_cb(void* ctx, char* buf, size_t buf_size) {
    (void)ctx;
    if (!g_raft || !buf || buf_size < 256) return -1;

    uint64_t term;
    raft_role_t role;
    uint64_t commit_idx, last_applied;
    size_t log_count;
    raft_status(g_raft, &term, &role, &commit_idx, &last_applied, &log_count, NULL);

    const char* role_str = "unknown";
    switch (role) {
        case RAFT_LEADER:    role_str = "leader";    break;
        case RAFT_CANDIDATE: role_str = "candidate"; break;
        case RAFT_FOLLOWER:  role_str = "follower";  break;
        default:             break;
    }

    raft_snapshot_info_t snap_info;
    uint64_t snap_idx = 0;
    if (raft_get_snapshot_info(g_raft, &snap_info) == 0) {
        snap_idx = snap_info.last_included_index;
    }

    return snprintf(buf, buf_size,
        "role:%s\r\n"
        "raft_term:%llu\r\n"
        "raft_commit_index:%llu\r\n"
        "raft_applied_index:%llu\r\n"
        "raft_log_count:%zu\r\n"
        "last_snapshot_index:%llu\r\n",
        role_str,
        (unsigned long long)term,
        (unsigned long long)commit_idx,
        (unsigned long long)last_applied,
        log_count,
        (unsigned long long)snap_idx
    );
}

#ifdef _WIN32
static HANDLE g_metrics_thread = NULL;
static HANDLE g_resp_thread = NULL;

static BOOL WINAPI signal_handler(DWORD ctrl_type) {
    (void)ctrl_type;
    printf("\n[INFO] Shutting down...\n");
    g_running = 0;
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
    g_running = 0;
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
    printf("  --version         Show version info\n");
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

static void print_version(void) {
    printf("kv_raft (KV Store Raft Cluster Node)\n");
    printf("  Version: 1.0.0\n");
    printf("  Git Commit: " GIT_COMMIT_HASH "\n");
    printf("  Build Time: " BUILD_TIMESTAMP "\n");
    printf("  Compiler: " __VERSION__ "\n");
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
        } else if (strcmp(argv[i], "--version") == 0) {
            print_version();
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

    /* 打开 kv_store（Raft 模式：不创建 WAL） */
    g_db = kv_open_raft(data_dir);
    if (!g_db) {
        fprintf(stderr, "Failed to open database at %s\n", data_dir);
        return 1;
    }
    printf("[INFO] KV Store opened in Raft mode at %s\n", data_dir);

    /* 创建 Raft 配置 */
    raft_config_t raft_cfg;
    memset(&raft_cfg, 0, sizeof(raft_cfg));
    snprintf(raft_cfg.node_id, sizeof(raft_cfg.node_id), "%s", node_id);
    raft_cfg.listen_port = raft_port;
    raft_cfg.num_peers = num_peers;
    memcpy(raft_cfg.peers, peers, (size_t)num_peers * sizeof(raft_peer_t));
    snprintf(raft_cfg.data_dir, sizeof(raft_cfg.data_dir), "%s", data_dir);
    raft_cfg.snapshot_fn = raft_snapshot_to_kv;
    raft_cfg.restore_fn = raft_restore_from_kv;

    /* 创建 Raft 节点 */
    g_raft = raft_create(&raft_cfg, g_db, raft_apply_to_kv);
    if (!g_raft) {
        fprintf(stderr, "Failed to create Raft node\n");
        kv_close(g_db);
        return 1;
    }
    printf("[INFO] Raft node created\n");

    /* 重启恢复：如果存在快照但数据目录中没有 MANIFEST，先从快照恢复状态机
     * 这处理的是新节点加入或数据目录损坏后重启的场景 */
    {
        raft_snapshot_info_t snap_info;
        if (raft_get_snapshot_info(g_raft, &snap_info) == 0 && snap_info.last_included_index > 0) {
            char manifest_path[512];
            snprintf(manifest_path, sizeof(manifest_path), "%s/" MANIFEST_FILE_NAME, data_dir);
            FILE* mf = fopen(manifest_path, "rb");
            if (mf) {
                fclose(mf);
                printf("[INFO] MANIFEST exists, skipping snapshot restore\n");
            } else {
                printf("[INFO] No MANIFEST found, restoring from snapshot (index=%llu)...\n",
                       (unsigned long long)snap_info.last_included_index);
                if (raft_snapshot_restore(g_raft, snap_info.file_path) != 0) {
                    fprintf(stderr, "WARNING: Failed to restore from snapshot %s\n",
                            snap_info.file_path);
                }
            }
        }
    }

    /* 崩溃恢复：重放已提交但未应用的 Raft 日志到 KV 引擎 */
    {
        uint64_t term, commit_idx, last_applied;
        raft_role_t role;
        size_t log_count;
        raft_status(g_raft, &term, &role, &commit_idx, &last_applied, &log_count, NULL);
        printf("[INFO] Raft state: term=%llu, role=%s, commit=%llu, applied=%llu, log_count=%zu\n",
               (unsigned long long)term, raft_role_str(role),
               (unsigned long long)commit_idx, (unsigned long long)last_applied, log_count);
        if (commit_idx > last_applied) {
            raft_replay_committed(g_raft);
        }
    }

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
        raft_status(g_raft, &term, &role, &commit_idx, &last_applied, NULL, NULL);

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
        /* 设置健康检查回调，使 HEALTH 命令返回 Raft 状态 */
        resp_server_set_health_cb(g_resp, resp_health_raft_cb, NULL);
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
        if (metrics_server_start(&g_metrics, "0.0.0.0", metrics_port, g_db, g_raft) != 0) {
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
    while (g_running) {
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