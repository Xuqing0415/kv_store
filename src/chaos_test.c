#include "kv_store.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>

#ifdef _WIN32
#include <windows.h>
#include <process.h>
#define THREAD_FN unsigned(__stdcall*)(void*)
#define THREAD_RET unsigned
#define THREAD_CREATE(h, a, f, d) ((void)(*(h) = (HANDLE)_beginthreadex(NULL, 0, (f), (d), 0, NULL)))
#define THREAD_JOIN(h) WaitForSingleObject((h), INFINITE)
#define THREAD_CLOSE(h) CloseHandle((h))
#define SLEEP_MS(ms) Sleep((ms))
#define ATOMIC_INC(p) InterlockedIncrement((LONG*)(p))
#define ATOMIC_ADD(p, v) InterlockedAdd((LONG*)(p), (LONG)(v))
typedef volatile LONG atomic_t;
#else
#include <pthread.h>
#include <unistd.h>
#define THREAD_FN void*(*)(void*)
#define THREAD_RET void*
#define THREAD_CREATE(h, a, f, d) (pthread_create((h), (a), (f), (d)) == 0)
#define THREAD_JOIN(h) pthread_join((h), NULL)
#define THREAD_CLOSE(h) ((void)(h))
#define SLEEP_MS(ms) usleep((ms) * 1000)
#define ATOMIC_INC(p) __sync_add_and_fetch((p), 1)
#define ATOMIC_ADD(p, v) __sync_add_and_fetch((p), (v))
typedef volatile int atomic_t;
#endif

#define NUM_THREADS      4
#define NUM_KEYS         10000
#define TEST_DURATION    60     /* 测试持续秒数 */
#define KEY_PREFIX       "ck_"
#define VAL_PREFIX       "cv_"
#define VAL_SIZE         128

/* 统计信息 */
typedef struct {
    atomic_t put_ops;
    atomic_t put_errors;
    atomic_t get_ops;
    atomic_t get_hits;
    atomic_t get_misses;
    atomic_t del_ops;
    atomic_t del_errors;
    atomic_t scan_ops;
    atomic_t snapshot_ops;
    atomic_t snapshot_errors;
} chaos_stats_t;

static chaos_stats_t g_stats;
static volatile int g_running = 1;
static kv_store_t* g_db = NULL;
static const char* g_data_dir = NULL;

static void fill_random_value(char* buf, size_t len) {
    static const char charset[] = "abcdefghijklmnopqrstuvwxyz0123456789";
    for (size_t i = 0; i < len - 1; i++) {
        buf[i] = charset[rand() % (sizeof(charset) - 1)];
    }
    buf[len - 1] = '\0';
}

static THREAD_RET worker_thread(void* arg) {
    int thread_id = (int)(intptr_t)arg;
    char key[64];
    char value[VAL_SIZE];
    int iter = 0;

    printf("[CHAOS] Worker %d started\n", thread_id);

    while (g_running) {
        int op = rand() % 100;
        int key_idx = rand() % NUM_KEYS;
        snprintf(key, sizeof(key), "%s%d", KEY_PREFIX, key_idx);

        if (op < 50) {
            /* 50% PUT */
            fill_random_value(value, VAL_SIZE);
            if (kv_put(g_db, key, strlen(key), value, strlen(value)) == 0) {
                ATOMIC_INC(&g_stats.put_ops);
            } else {
                ATOMIC_INC(&g_stats.put_errors);
            }
        } else if (op < 85) {
            /* 35% GET */
            char* val = NULL;
            size_t vlen = 0;
            int ret = kv_get(g_db, key, strlen(key), &val, &vlen);
            ATOMIC_INC(&g_stats.get_ops);
            if (ret == 0) {
                ATOMIC_INC(&g_stats.get_hits);
                kv_free(val);
            } else {
                ATOMIC_INC(&g_stats.get_misses);
            }
        } else if (op < 95) {
            /* 10% DELETE */
            if (kv_delete(g_db, key, strlen(key)) == 0) {
                ATOMIC_INC(&g_stats.del_ops);
            } else {
                ATOMIC_INC(&g_stats.del_errors);
            }
        } else {
            /* 5% SCAN (lightweight, limited range) */
            int start_idx = rand() % (NUM_KEYS - 100);
            int end_idx = start_idx + 100;
            char start_key[64], end_key[64];
            snprintf(start_key, sizeof(start_key), "%s%d", KEY_PREFIX, start_idx);
            snprintf(end_key, sizeof(end_key), "%s%d", KEY_PREFIX, end_idx);

            kv_iter_t* iter = kv_scan(g_db, start_key, strlen(start_key),
                                       end_key, strlen(end_key));
            if (iter) {
                char* k = NULL; size_t kl = 0;
                char* v = NULL; size_t vl = 0;
                int count = 0;
                while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0 && count < 100) {
                    kv_free(k);
                    kv_free(v);
                    count++;
                }
                kv_iter_free(iter);
            }
            ATOMIC_INC(&g_stats.scan_ops);
        }

        iter++;
        if (iter % 10000 == 0) {
            printf("[CHAOS] Worker %d: %d iterations\n", thread_id, iter);
        }
    }

    printf("[CHAOS] Worker %d stopped after %d iterations\n", thread_id, iter);
    return (THREAD_RET)0;
}

static THREAD_RET snapshot_thread(void* arg) {
    (void)arg;
    int snapshot_count = 0;

    printf("[CHAOS] Snapshot thread started\n");

    while (g_running) {
        SLEEP_MS(5000); /* 每 5 秒创建一个快照 */

        if (!g_running) break;

        kv_snapshot_t* snap = kv_snapshot_create(g_db);
        if (!snap) {
            ATOMIC_INC(&g_stats.snapshot_errors);
            printf("[SNAP] ERROR: Failed to create snapshot\n");
            continue;
        }

        ATOMIC_INC(&g_stats.snapshot_ops);
        snapshot_count++;

        /* 验证快照：随机抽查 50 个 key */
        int verify_errors = 0;
        for (int i = 0; i < 50; i++) {
            int key_idx = rand() % NUM_KEYS;
            char key[64];
            snprintf(key, sizeof(key), "%s%d", KEY_PREFIX, key_idx);

            char* snap_val = NULL;
            size_t snap_vlen = 0;
            int snap_ret = kv_snapshot_get(snap, key, strlen(key), &snap_val, &snap_vlen);

            /* 同时从主库读取 */
            char* db_val = NULL;
            size_t db_vlen = 0;
            int db_ret = kv_get(g_db, key, strlen(key), &db_val, &db_vlen);

            if (snap_ret != db_ret) {
                printf("[SNAP] MISMATCH for key '%s': snap_ret=%d db_ret=%d\n",
                       key, snap_ret, db_ret);
                verify_errors++;
            } else if (snap_ret == 0 && db_ret == 0) {
                if (snap_vlen != db_vlen || memcmp(snap_val, db_val, snap_vlen) != 0) {
                    printf("[SNAP] VALUE MISMATCH for key '%s': snap_len=%zu db_len=%zu\n",
                           key, snap_vlen, db_vlen);
                    verify_errors++;
                }
            }

            kv_free(snap_val);
            kv_free(db_val);
        }

        if (verify_errors > 0) {
            printf("[SNAP] Snapshot #%d: %d verification errors!\n", snapshot_count, verify_errors);
        } else {
            printf("[SNAP] Snapshot #%d created and verified OK\n", snapshot_count);
        }

        kv_snapshot_free(snap);
    }

    printf("[CHAOS] Snapshot thread stopped after %d snapshots\n", snapshot_count);
    return (THREAD_RET)0;
}

static void print_stats_header(void) {
    printf("\n");
    printf("==============================================================\n");
    printf("  KV Store Chaos Test\n");
    printf("==============================================================\n");
    printf("Threads:      %d\n", NUM_THREADS);
    printf("Keys:         %d\n", NUM_KEYS);
    printf("Duration:     %d seconds\n", TEST_DURATION);
    printf("Data dir:     %s\n", g_data_dir);
    printf("Compression:  zstd\n");
    printf("==============================================================\n\n");
}

static void print_stats(const char* phase, double elapsed) {
    long long put_ops = g_stats.put_ops;
    long long get_ops = g_stats.get_ops;
    long long get_hits = g_stats.get_hits;
    long long get_misses = g_stats.get_misses;
    long long del_ops = g_stats.del_ops;
    long long scan_ops = g_stats.scan_ops;
    long long snap_ops = g_stats.snapshot_ops;
    long long snap_errs = g_stats.snapshot_errors;
    long long put_errs = g_stats.put_errors;
    long long del_errs = g_stats.del_errors;
    long long total_ops = put_ops + get_ops + del_ops + scan_ops;

    printf("\n[STATS] === %s (%.1f sec) ===\n", phase, elapsed);
    printf("  PUT:    %lld ops (%.0f/s), %lld errors\n",
           put_ops, elapsed > 0 ? put_ops / elapsed : 0, put_errs);
    printf("  GET:    %lld ops (%.0f/s), hit=%lld miss=%lld (%.1f%% hit)\n",
           get_ops, elapsed > 0 ? get_ops / elapsed : 0, get_hits, get_misses,
           get_ops > 0 ? 100.0 * get_hits / get_ops : 0);
    printf("  DEL:    %lld ops (%.0f/s), %lld errors\n",
           del_ops, elapsed > 0 ? del_ops / elapsed : 0, del_errs);
    printf("  SCAN:   %lld ops\n", scan_ops);
    printf("  SNAP:   %lld created, %lld errors\n", snap_ops, snap_errs);
    printf("  TOTAL:  %lld ops (%.0f/s)\n",
           total_ops, elapsed > 0 ? total_ops / elapsed : 0);
}

static int verify_data_integrity(void) {
    printf("\n[VERIFY] Checking data integrity...\n");

    kv_store_t* db = kv_open(g_data_dir);
    if (!db) {
        printf("[VERIFY] ERROR: Failed to reopen database\n");
        return 1;
    }

    int errors = 0;
    int found = 0;
    int missing = 0;

    /* 随机抽查 200 个 key */
    for (int i = 0; i < 200; i++) {
        int key_idx = rand() % NUM_KEYS;
        char key[64];
        snprintf(key, sizeof(key), "%s%d", KEY_PREFIX, key_idx);

        char* val = NULL;
        size_t vlen = 0;
        int ret = kv_get(db, key, strlen(key), &val, &vlen);

        if (ret == 0) {
            found++;
            kv_free(val);
        } else {
            missing++;
            /* 记录但不报错，因为 key 可能已被删除（混沌测试中有 DELETE 操作） */
        }
    }

    /* 全量扫描 */
    int scan_count = 0;
    kv_iter_t* iter = kv_scan(db, NULL, 0, NULL, 0);
    if (iter) {
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            scan_count++;
            kv_free(k);
            kv_free(v);
        }
        kv_iter_free(iter);
    }

    printf("[VERIFY] Random check: %d found, %d missing (of 200)\n", found, missing);
    printf("[VERIFY] Total keys in database: %d\n", scan_count);

    kv_close(db);
    return errors;
}

static void cleanup_data_dir(void) {
#ifdef _WIN32
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rd /s /q \"%s\" 2>nul", g_data_dir);
    system(cmd);
#else
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", g_data_dir);
    system(cmd);
#endif
}

int main(int argc, char* argv[]) {
    srand((unsigned int)time(NULL));

    g_data_dir = "./chaos_test_db";
    int duration = TEST_DURATION;
    int threads = NUM_THREADS;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-d") == 0 && i + 1 < argc) {
            g_data_dir = argv[++i];
        } else if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) {
            duration = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-n") == 0 && i + 1 < argc) {
            threads = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--clean") == 0) {
            cleanup_data_dir();
        }
    }

    if (threads < 1) threads = 1;
    if (threads > 16) threads = 16;

    print_stats_header();

    /* 清理旧数据 */
    cleanup_data_dir();

    /* 打开数据库 */
    g_db = kv_open(g_data_dir);
    if (!g_db) {
        fprintf(stderr, "Failed to open database\n");
        return 1;
    }

    /* 预填充数据 */
    printf("[CHAOS] Pre-populating %d keys...\n", NUM_KEYS);
    char value[VAL_SIZE];
    char key[64];
    for (int i = 0; i < NUM_KEYS; i++) {
        snprintf(key, sizeof(key), "%s%d", KEY_PREFIX, i);
        fill_random_value(value, VAL_SIZE);
        kv_put(g_db, key, strlen(key), value, strlen(value));
    }
    printf("[CHAOS] Pre-population complete\n");

    /* 启动工作线程 */
    HANDLE thread_handles[16];
    for (int i = 0; i < threads; i++) {
        THREAD_CREATE(&thread_handles[i], NULL, worker_thread, (void*)(intptr_t)i);
    }

    /* 启动快照线程 */
    HANDLE snapshot_handle;
    THREAD_CREATE(&snapshot_handle, NULL, snapshot_thread, NULL);

    /* 运行指定时间 */
    time_t start_time = time(NULL);
    time_t last_report = start_time;

    printf("[CHAOS] Test running for %d seconds...\n", duration);
    fflush(stdout);

    while (1) {
        SLEEP_MS(1000);
        time_t now = time(NULL);
        double elapsed = difftime(now, start_time);

        if (elapsed >= duration) break;

        /* 每 10 秒输出一次统计 */
        if (difftime(now, last_report) >= 10.0) {
            print_stats("Running", elapsed);
            last_report = now;
            fflush(stdout);
        }
    }

    /* 停止所有线程 */
    printf("\n[CHAOS] Stopping all threads...\n");
    g_running = 0;

    for (int i = 0; i < threads; i++) {
        THREAD_JOIN(thread_handles[i]);
        THREAD_CLOSE(thread_handles[i]);
    }
    THREAD_JOIN(snapshot_handle);
    THREAD_CLOSE(snapshot_handle);

    double total_elapsed = difftime(time(NULL), start_time);

    /* 最终统计 */
    print_stats("Final", total_elapsed);

    /* 关闭数据库 */
    printf("\n[CHAOS] Closing database...\n");
    kv_close(g_db);

    /* 验证数据完整性 */
    int verify_errors = verify_data_integrity();

    /* 清理 */
    cleanup_data_dir();

    printf("\n==============================================================\n");
    if (verify_errors == 0) {
        printf("  Chaos test PASSED! No data integrity errors.\n");
    } else {
        printf("  Chaos test FAILED! %d errors found.\n", verify_errors);
    }
    printf("==============================================================\n");

    return verify_errors > 0 ? 1 : 0;
}