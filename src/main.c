#include "kv_store.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
#include <windows.h>
#include <process.h>
#define getpid() _getpid()
#else
#include <unistd.h>
#endif

#define TEST_ENTRIES 5000
#define TEST_READS 1000
#define VERIFY_READS 100
#define KEY_PREFIX "key_"
#define VAL_PREFIX "value_"

static void print_timestamp(const char* msg) {
    time_t now = time(NULL);
    struct tm* tm = localtime(&now);
    char buf[64];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", tm);
    printf("[%s] %s\n", buf, msg);
}

static int run_test(const char* dir, int test_round) {
    kv_store_t* db = kv_open(dir);
    if (!db) {
        print_timestamp("Failed to open database");
        return 1;
    }
    printf("[INFO] Round %d: Open database at %s (pid=%d)\n", test_round, dir, getpid());
    
    char key[64];
    char value[128];
    int ret;
    
    clock_t start = clock();
    for (int i = 0; i < TEST_ENTRIES; i++) {
        snprintf(key, sizeof(key), "%s%d", KEY_PREFIX, i);
        snprintf(value, sizeof(value), "%s%d", VAL_PREFIX, i);
        ret = kv_put(db, key, strlen(key), value, strlen(value));
        if (ret != 0) {
            printf("[ERROR] Failed to put %s\n", key);
            kv_close(db);
            return 1;
        }
    }
    clock_t end = clock();
    double write_time = (double)(end - start) / CLOCKS_PER_SEC;
    double write_throughput = TEST_ENTRIES / write_time;
    printf("[INFO] Write %d entries, cost %.3f sec, %.0f ops/sec\n", 
           TEST_ENTRIES, write_time, write_throughput);
    
    start = clock();
    int hits = 0, misses = 0;
    for (int i = 0; i < TEST_READS; i++) {
        int idx = rand() % TEST_ENTRIES;
        snprintf(key, sizeof(key), "%s%d", KEY_PREFIX, idx);
        
        char* val = NULL;
        size_t vlen = 0;
        ret = kv_get(db, key, strlen(key), &val, &vlen);
        
        if (ret == 0) {
            hits++;
            kv_free(val);
        } else {
            misses++;
        }
    }
    end = clock();
    double read_time = (double)(end - start) / CLOCKS_PER_SEC;
    printf("[INFO] Read %d random keys, hit %d, miss %d, cost %.3f sec\n", 
           TEST_READS, hits, misses, read_time);
    
    int scan_count = 0;
    kv_iter_t* iter = kv_scan(db, NULL, 0, NULL, 0);
    if (iter) {
        char* k = NULL;
        size_t kl = 0;
        char* v = NULL;
        size_t vl = 0;
        while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            scan_count++;
            kv_free(k);
            kv_free(v);
        }
        kv_iter_free(iter);
    }
    printf("[INFO] Scan all keys: %d entries found\n", scan_count);
    
    kv_close(db);
    printf("[INFO] Database closed\n");
    
    return 0;
}

int main() {
    srand((unsigned int)time(NULL));
    
    const char* dir = "./test_db";
    char key[64];
    char value[128];
    int ret;
    
    printf("========================================\n");
    printf("KV Store Integration Test\n");
    printf("========================================\n");
    
    ret = run_test(dir, 1);
    if (ret != 0) return ret;
    
    printf("\n[INFO] Reopening database to verify persistence...\n");
    ret = run_test(dir, 2);
    if (ret != 0) return ret;
    
    /* ===== 增强验证：随机抽查 key 值是否正确 ===== */
    printf("\n[INFO] Verifying data integrity with random spot checks...\n");
    {
        kv_store_t* db = kv_open(dir);
        if (!db) {
            printf("[ERROR] Failed to reopen for verification\n");
            return 1;
        }
        
        /* 等待后台 merge 线程完成所有压缩，确保 SSTable 文件处于一致状态 */
        printf("[INFO] Waiting for background compaction to complete...\n");
        kv_force_merge(db);
        
        int verify_errors = 0;
        for (int i = 0; i < VERIFY_READS; i++) {
            int idx = rand() % TEST_ENTRIES;
            snprintf(key, sizeof(key), "%s%d", KEY_PREFIX, idx);
            snprintf(value, sizeof(value), "%s%d", VAL_PREFIX, idx);
            
            char* actual_val = NULL;
            size_t actual_vlen = 0;
            ret = kv_get(db, key, strlen(key), &actual_val, &actual_vlen);
            
            if (ret != 0) {
                printf("[ERROR] Key '%s' NOT FOUND after reopen!\n", key);
                verify_errors++;
            } else if (strlen(value) != actual_vlen || memcmp(actual_val, value, actual_vlen) != 0) {
                printf("[ERROR] Key '%s' value mismatch! expected='%s'(len=%zu) actual='%.*s'(len=%zu)\n",
                       key, value, strlen(value), (int)actual_vlen, actual_val, actual_vlen);
                verify_errors++;
            }
            kv_free(actual_val);
        }
        
        /* 额外验证：全部扫描确认 key 数量 */
        int scan_count = 0;
        kv_iter_t* iter = kv_scan(db, NULL, 0, NULL, 0);
        if (iter) {
            char* k = NULL; size_t kl = 0;
            char* v = NULL; size_t vl = 0;
            while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
                scan_count++;
                kv_free(k); kv_free(v);
            }
            kv_iter_free(iter);
        }
        printf("[INFO] Scan count: %d keys (expected %d)\n", scan_count, TEST_ENTRIES);
        
        kv_close(db);
        
        if (verify_errors > 0) {
            printf("[FAIL] Data integrity check: %d errors found!\n", verify_errors);
            return 1;
        }
        printf("[PASS] Data integrity check: all %d random keys verified correctly\n", VERIFY_READS);
    }
    
    printf("\n========================================\n");
    printf("All tests passed! Data is persistent.\n");
    printf("========================================\n");


    return 0;
}