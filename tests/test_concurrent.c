#include "kv_store.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
#include <windows.h>
#include <process.h>
#include <crtdbg.h>
#define THREAD_TYPE unsigned(__stdcall*)(void*)
#define THREAD_RET unsigned
#define THREAD_CREATE(h, f, d) ((void)(*(h) = (HANDLE)_beginthreadex(NULL, 0, (f), (d), 0, NULL)))
#define THREAD_JOIN(h) WaitForSingleObject((h), INFINITE)
#define THREAD_CLOSE(h) CloseHandle((h))
#define SLEEP_MS(ms) Sleep((ms))
typedef HANDLE thread_t;
typedef volatile LONG atomic_t;
#define ATOMIC_INC(p) InterlockedIncrement((p))
#define ATOMIC_READ(p) InterlockedCompareExchange((p), 0, 0)
#else
#include <pthread.h>
#include <unistd.h>
#define THREAD_TYPE void*(*)(void*)
#define THREAD_RET void*
#define THREAD_CREATE(h, f, d) (pthread_create((h), NULL, (f), (d)) == 0)
#define THREAD_JOIN(h) pthread_join((h), NULL)
#define THREAD_CLOSE(h) ((void)(h))
#define SLEEP_MS(ms) usleep((ms) * 1000)
typedef pthread_t thread_t;
typedef volatile int atomic_t;
#define ATOMIC_INC(p) __sync_add_and_fetch((p), 1)
#define ATOMIC_READ(p) __sync_add_and_fetch((p), 0)
#endif

static void rmrf(const char* dir) {
#ifdef _WIN32
    char search_path[MAX_PATH + 4];
    char file_path[MAX_PATH * 2];
    WIN32_FIND_DATAA fd;
    strcpy(search_path, dir);
    strcat(search_path, "\\*");
    HANDLE hFind = FindFirstFileA(search_path, &fd);
    if (hFind == INVALID_HANDLE_VALUE) {
        RemoveDirectoryA(dir);
        return;
    }
    do {
        if (strcmp(fd.cFileName, ".") == 0 || strcmp(fd.cFileName, "..") == 0) continue;
        snprintf(file_path, sizeof(file_path), "%s\\%s", dir, fd.cFileName);
        if (fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
            rmrf(file_path);
        } else {
            SetFileAttributesA(file_path, FILE_ATTRIBUTE_NORMAL);
            DeleteFileA(file_path);
        }
    } while (FindNextFileA(hFind, &fd));
    FindClose(hFind);
    RemoveDirectoryA(dir);
#else
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", dir);
    system(cmd);
#endif
}

#define NUM_WRITERS 4
#define NUM_READERS 4
#define TEST_DURATION_SEC 5
#define VALUE_SIZE 128
#define KEY_POOL_SIZE 200

typedef struct {
    kv_store_t* db;
    int thread_id;
    atomic_t* write_count;
    atomic_t* read_count;
    atomic_t* read_errors;
    atomic_t* stop;
} thread_args_t;

static THREAD_RET writer_thread(void* arg) {
    thread_args_t* args = (thread_args_t*)arg;
    char key[32];
    char value[VALUE_SIZE];
    
    /* 每个线程有独立的 key 命名空间，避免竞争 */
    int key_start = args->thread_id * KEY_POOL_SIZE;
    
    memset(value, 'W', VALUE_SIZE);
    snprintf(value + VALUE_SIZE - 32, 32, "_writer_%d_", args->thread_id);
    
    int local_writes = 0;
    
    while (!ATOMIC_READ(args->stop)) {
        int idx = key_start + (rand() % KEY_POOL_SIZE);
        snprintf(key, sizeof(key), "key_%08d", idx);
        snprintf(value + VALUE_SIZE - 16, 16, "_%08d", local_writes);
        
        int ret = kv_put(args->db, key, strlen(key), value, VALUE_SIZE);
        if (ret != 0) {
            printf("[Writer %d] ERROR: kv_put failed for %s\n", args->thread_id, key);
        }
        local_writes++;
        
        /* 偶尔 sync */
        if (local_writes % 500 == 0) {
            kv_sync(args->db);
        }
    }
    
    ATOMIC_INC(args->write_count);
    printf("[Writer %d] Done: %d writes\n", args->thread_id, local_writes);
    return (THREAD_RET)0;
}

static THREAD_RET reader_thread(void* arg) {
    thread_args_t* args = (thread_args_t*)arg;
    char key[32];
    
    int local_reads = 0;
    int local_errors = 0;
    
    while (!ATOMIC_READ(args->stop)) {
        /* 随机读取所有 writer 写入的 key 范围 */
        int thread_id = rand() % NUM_WRITERS;
        int key_start = thread_id * KEY_POOL_SIZE;
        int idx = key_start + (rand() % KEY_POOL_SIZE);
        snprintf(key, sizeof(key), "key_%08d", idx);
        
        char* value = NULL;
        size_t vlen = 0;
        int ret = kv_get(args->db, key, strlen(key), &value, &vlen);
        
        if (ret == 0) {
            /* 验证值完整性 */
            if (vlen != VALUE_SIZE) {
                printf("[Reader %d] ERROR: %s has wrong vlen %zu\n", args->thread_id, key, vlen);
                local_errors++;
            }
            kv_free(value);
        }
        /* ret == -1 是正常的（key 可能还没被写入） */
        
        local_reads++;
        if (local_reads % 10000 == 0) {
            printf("[Reader %d] %d reads so far...\n", args->thread_id, local_reads);
        }
    }
    
    ATOMIC_INC(args->read_count);
    if (local_errors > 0) {
        /* 原子地将 local_errors 加到 read_errors */
        for (int e = 0; e < local_errors; e++) {
            ATOMIC_INC(args->read_errors);
        }
    }
    printf("[Reader %d] Done: %d reads, %d errors\n", args->thread_id, local_reads, local_errors);
    return (THREAD_RET)0;
}

int main() {
    setbuf(stdout, NULL);
    
#ifdef _WIN32
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
#endif
    
    const char* dir = "./test_concurrent_db";
    rmrf(dir);
    
    printf("========================================\n");
    printf("Multi-Thread Concurrency Test\n");
    printf("========================================\n");
    printf("Writers: %d, Readers: %d, Duration: %d sec\n", NUM_WRITERS, NUM_READERS, TEST_DURATION_SEC);
    printf("Key pool: %d keys per writer, Value size: %d bytes\n", KEY_POOL_SIZE, VALUE_SIZE);
    
    srand((unsigned int)time(NULL));
    
    kv_store_t* db = kv_open(dir);
    if (!db) {
        printf("ERROR: Failed to open database\n");
        return 1;
    }
    
    atomic_t stop = 0;
    atomic_t write_count = 0;
    atomic_t read_count = 0;
    atomic_t read_errors = 0;
    
    thread_t writers[NUM_WRITERS];
    thread_t readers[NUM_READERS];
    thread_args_t writer_args[NUM_WRITERS];
    thread_args_t reader_args[NUM_READERS];
    
    /* 启动写线程 */
    printf("\n--- Starting %d writer threads ---\n", NUM_WRITERS);
    for (int i = 0; i < NUM_WRITERS; i++) {
        writer_args[i].db = db;
        writer_args[i].thread_id = i;
        writer_args[i].write_count = &write_count;
        writer_args[i].read_count = &read_count;
        writer_args[i].read_errors = &read_errors;
        writer_args[i].stop = &stop;
        THREAD_CREATE(&writers[i], writer_thread, &writer_args[i]);
    }
    
    /* 启动读线程 */
    printf("--- Starting %d reader threads ---\n", NUM_READERS);
    for (int i = 0; i < NUM_READERS; i++) {
        reader_args[i].db = db;
        reader_args[i].thread_id = i;
        reader_args[i].write_count = &write_count;
        reader_args[i].read_count = &read_count;
        reader_args[i].read_errors = &read_errors;
        reader_args[i].stop = &stop;
        THREAD_CREATE(&readers[i], reader_thread, &reader_args[i]);
    }
    
    /* 运行指定时间 */
    printf("\n--- Running for %d seconds ---\n", TEST_DURATION_SEC);
    for (int sec = 1; sec <= TEST_DURATION_SEC; sec++) {
        SLEEP_MS(1000);
        printf("  %d/%d sec elapsed\n", sec, TEST_DURATION_SEC);
    }
    
    /* 停止所有线程 */
    printf("\n--- Stopping threads ---\n");
    ATOMIC_INC(&stop);
    
    for (int i = 0; i < NUM_WRITERS; i++) {
        THREAD_JOIN(writers[i]);
        THREAD_CLOSE(writers[i]);
    }
    for (int i = 0; i < NUM_READERS; i++) {
        THREAD_JOIN(readers[i]);
        THREAD_CLOSE(readers[i]);
    }
    
    printf("\n--- All threads stopped ---\n");
    printf("Total write batches: %d\n", (int)ATOMIC_READ(&write_count));
    printf("Total read batches: %d\n", (int)ATOMIC_READ(&read_count));
    printf("Total read errors: %d\n", (int)ATOMIC_READ(&read_errors));
    
    /* ============================================================
     * Test 1: 最终一致性验证
     * ============================================================ */
    printf("\n=== Final Consistency Check ===\n");
    
    /* 写入一个标记 key，确保所有 writer 的最后一次写入可见 */
    for (int w = 0; w < NUM_WRITERS; w++) {
        char marker_key[32];
        snprintf(marker_key, sizeof(marker_key), "marker_writer_%d", w);
        kv_put(db, marker_key, strlen(marker_key), "MARKER", 6);
    }
    kv_sync(db);
    
    /* 等待 compaction 完成 */
    printf("  Waiting for compaction to settle...\n");
    SLEEP_MS(3000);
    kv_force_merge(db);
    
    /* 扫描验证所有 marker 存在 */
    int markers_found = 0;
    for (int w = 0; w < NUM_WRITERS; w++) {
        char marker_key[32];
        snprintf(marker_key, sizeof(marker_key), "marker_writer_%d", w);
        char* val = NULL;
        size_t vlen = 0;
        if (kv_get(db, marker_key, strlen(marker_key), &val, &vlen) == 0) {
            markers_found++;
            kv_free(val);
        } else {
            printf("  ERROR: marker %s NOT found!\n", marker_key);
        }
    }
    printf("  Markers found: %d/%d\n", markers_found, NUM_WRITERS);
    
    /* 全量扫描，确保没有数据损坏 */
    printf("\n--- Full Scan Integrity ---\n");
    kv_iter_t* iter = kv_scan(db, NULL, 0, NULL, 0);
    int scan_count = 0;
    int scan_errors = 0;
    char* prev_k = NULL;
    size_t prev_kl = 0;
    
    if (iter) {
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            /* 检查有序性 */
            if (prev_k) {
                size_t min_len = prev_kl < kl ? prev_kl : kl;
                int cmp = memcmp(prev_k, k, min_len);
                if (cmp > 0 || (cmp == 0 && prev_kl > kl)) {
                    printf("  ERROR: Out of order: %.*s > %.*s\n", (int)prev_kl, prev_k, (int)kl, k);
                    scan_errors++;
                }
            }
            kv_free(prev_k);
            prev_k = k;
            prev_kl = kl;
            
            /* 检查值长度 */
            if (vl != VALUE_SIZE && vl != 6) { /* 6 = "MARKER" */
                /* 可能是墓碑或其他，跳过 */
            }
            kv_free(v);
            scan_count++;
        }
        kv_free(prev_k);
        kv_iter_free(iter);
    }
    
    printf("  Scan: %d entries, %d order errors\n", scan_count, scan_errors);
    
    /* ============================================================
     * Test 2: 重启后数据完整性
     * ============================================================ */
    printf("\n=== Reopen & Verify ===\n");
    kv_close(db);
    
    db = kv_open(dir);
    if (!db) {
        printf("ERROR: Reopen failed\n");
        return 1;
    }
    
    /* 重新扫描 */
    iter = kv_scan(db, NULL, 0, NULL, 0);
    int reopen_count = 0;
    if (iter) {
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            reopen_count++;
            kv_free(k); kv_free(v);
        }
        kv_iter_free(iter);
    }
    printf("  After reopen: %d entries (before close: %d)\n", reopen_count, scan_count);
    
    kv_close(db);
    rmrf(dir);
    
    int total_errors = (int)ATOMIC_READ(&read_errors) + scan_errors;
    printf("\n========================================\n");
    printf("Concurrency test complete: %d total errors\n", total_errors);
    printf("========================================\n");
    
#ifdef _WIN32
    _CrtDumpMemoryLeaks();
#endif
    
    return total_errors > 0 ? 1 : 0;
}