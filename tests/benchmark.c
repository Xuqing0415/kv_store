#include "kv_store.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
#include <windows.h>
#include <crtdbg.h>
#endif

static double get_time_sec() {
#ifdef _WIN32
    LARGE_INTEGER freq, count;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&count);
    return (double)count.QuadPart / (double)freq.QuadPart;
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
#endif
}

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

#define NUM_KEYS 30000
#define VALUE_SIZE 256

int main() {
    setbuf(stdout, NULL);
    
#ifdef _WIN32
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
#endif
    
    const char* dir = "./benchmark_db";
    rmrf(dir);
    
    printf("========================================\n");
    printf("KV Store Performance Benchmark\n");
    printf("========================================\n");
    printf("Entries: %d, Value size: %d bytes\n", NUM_KEYS, VALUE_SIZE);
    
    /* 生成随机数据 */
    char** keys = (char**)malloc(NUM_KEYS * sizeof(char*));
    char** values = (char**)malloc(NUM_KEYS * sizeof(char*));
    char value_template[VALUE_SIZE];
    memset(value_template, 'X', VALUE_SIZE);
    
    for (int i = 0; i < NUM_KEYS; i++) {
        keys[i] = (char*)malloc(32);
        snprintf(keys[i], 32, "key_%08d", i);
        values[i] = (char*)malloc(VALUE_SIZE);
        memcpy(values[i], value_template, VALUE_SIZE);
        /* 在末尾加一些变化 */
        snprintf(values[i] + VALUE_SIZE - 16, 16, "_%08d", i);
    }
    
    /* 1. 顺序写入测试 */
    printf("\n--- Sequential Write ---\n");
    kv_store_t* db = kv_open(dir);
    if (!db) {
        printf("ERROR: Failed to open database\n");
        return 1;
    }
    
    double start = get_time_sec();
    for (int i = 0; i < NUM_KEYS; i++) {
        kv_put(db, keys[i], strlen(keys[i]), values[i], VALUE_SIZE);
    }
    double end = get_time_sec();
    double elapsed = end - start;
    printf("  Wrote %d entries in %.3f sec\n", NUM_KEYS, elapsed);
    printf("  Throughput: %.0f ops/sec\n", NUM_KEYS / elapsed);
    printf("  Data size: %.1f MB\n", (double)(NUM_KEYS * (12 + VALUE_SIZE)) / (1024 * 1024));
    
    /* 等待后台 compaction 完成，避免并发删除 SSTable 导致读 miss */
    printf("\n  Waiting for compaction to settle...\n");
    #ifdef _WIN32
    Sleep(5000);
    #else
    sleep(5);
    #endif
    printf("  Compaction wait complete\n");
    
    /* 2. 随机读取测试 */
    printf("\n--- Random Read ---\n");
    srand(42);
    int hits = 0, misses = 0;
    start = get_time_sec();
    for (int i = 0; i < NUM_KEYS; i++) {
        int idx = rand() % NUM_KEYS;
        char* value = NULL;
        size_t vlen = 0;
        if (kv_get(db, keys[idx], strlen(keys[idx]), &value, &vlen) == 0) {
            hits++;
            kv_free(value);
        } else {
            misses++;
        }
    }
    end = get_time_sec();
    elapsed = end - start;
    printf("  Read %d entries in %.3f sec\n", NUM_KEYS, elapsed);
    printf("  Throughput: %.0f ops/sec\n", NUM_KEYS / elapsed);
    printf("  Hits: %d, Misses: %d\n", hits, misses);
    
    /* 3. 顺序读取（扫描）测试 */
    printf("\n--- Sequential Scan ---\n");
    start = get_time_sec();
    kv_iter_t* iter = kv_scan(db, NULL, 0, NULL, 0);
    if (iter) {
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        int count = 0;
        while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            count++;
            kv_free(k);
            kv_free(v);
        }
        kv_iter_free(iter);
        end = get_time_sec();
        elapsed = end - start;
        printf("  Scanned %d entries in %.3f sec\n", count, elapsed);
        printf("  Throughput: %.0f ops/sec\n", count / elapsed);
    }
    
    /* 4. 删除测试 */
    printf("\n--- Delete ---\n");
    int del_count = NUM_KEYS / 10;
    start = get_time_sec();
    for (int i = 0; i < del_count; i++) {
        kv_delete(db, keys[i], strlen(keys[i]));
    }
    end = get_time_sec();
    elapsed = end - start;
    printf("  Deleted %d entries in %.3f sec\n", del_count, elapsed);
    printf("  Throughput: %.0f ops/sec\n", del_count / elapsed);
    
    kv_close(db);
    
    /* 5. 持久化恢复测试 */
    printf("\n--- Reopen & Verify ---\n");
    start = get_time_sec();
    db = kv_open(dir);
    end = get_time_sec();
    printf("  Reopen time: %.3f sec\n", end - start);
    
    /* 验证删除后的数据 */
    hits = 0; misses = 0;
    for (int i = 0; i < NUM_KEYS; i++) {
        char* value = NULL;
        size_t vlen = 0;
        if (kv_get(db, keys[i], strlen(keys[i]), &value, &vlen) == 0) {
            if (i < del_count) {
                printf("  ERROR: Key %s should have been deleted!\n", keys[i]);
            }
            hits++;
            kv_free(value);
        } else {
            if (i >= del_count) {
                printf("  ERROR: Key %s should exist!\n", keys[i]);
            }
            misses++;
        }
    }
    printf("  Verified: hits=%d, misses=%d (expected %d deleted)\n", hits, misses, del_count);
    
    /* 随机抽查 200 个 key 的值是否正确 */
    printf("\n--- Random Spot Check ---\n");
    int verify_errors = 0;
    srand(12345);
    for (int i = 0; i < 200; i++) {
        int idx = rand() % NUM_KEYS;
        char* value = NULL;
        size_t vlen = 0;
        int ret = kv_get(db, keys[idx], strlen(keys[idx]), &value, &vlen);
        if (ret == 0) {
            if (idx < del_count) {
                printf("  ERROR: Deleted key %s should NOT be found!\n", keys[idx]);
                verify_errors++;
            }
            if (vlen != VALUE_SIZE) {
                printf("  ERROR: Key %s has wrong value size %zu (expected %d)\n", keys[idx], vlen, VALUE_SIZE);
                verify_errors++;
            } else if (memcmp(value, values[idx], VALUE_SIZE) != 0) {
                printf("  ERROR: Key %s has wrong value\n", keys[idx]);
                verify_errors++;
            }
            kv_free(value);
        } else {
            /* 被删除的 key 应该 miss，未删除的 key 不应该 miss */
            if (idx >= del_count) {
                printf("  ERROR: Key %s should exist but was not found!\n", keys[idx]);
                verify_errors++;
            }
        }
    }
    printf("  Spot check: %d errors\n", verify_errors);
    
    /* 扫描所有 key 确认总数 */
    printf("\n--- Total Scan ---\n");
    kv_iter_t* scan_iter = kv_scan(db, NULL, 0, NULL, 0);
    int scan_count = 0;
    if (scan_iter) {
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        while (kv_iter_next(scan_iter, &k, &kl, &v, &vl) == 0) {
            scan_count++;
            kv_free(k); kv_free(v);
        }
        kv_iter_free(scan_iter);
    }
    printf("  Total keys in DB: %d (expected ~%d after %d deletes)\n", 
           scan_count, NUM_KEYS - del_count, del_count);
    
    kv_close(db);
    rmrf(dir);
    
    /* 清理 */
    for (int i = 0; i < NUM_KEYS; i++) {
        free(keys[i]);
        free(values[i]);
    }
    free(keys);
    free(values);
    
    printf("\n========================================\n");
    printf("Benchmark complete!\n");
    printf("========================================\n");
    
#ifdef _WIN32
    _CrtDumpMemoryLeaks();
#endif
    
    return 0;
}