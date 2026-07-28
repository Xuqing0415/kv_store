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

#define NUM_KEYS 5000
#define VALUE_SIZE 128

static int errors = 0;

#define CHECK(cond, msg) do { \
    if (!(cond)) { \
        printf("  FAIL: %s\n", msg); \
        errors++; \
    } else { \
        printf("  OK:   %s\n", msg); \
    } \
} while (0)

int main() {
    setbuf(stdout, NULL);
    
#ifdef _WIN32
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
#endif
    
    const char* dir = "./test_tombstone_scan_db";
    rmrf(dir);
    
    printf("========================================\n");
    printf("Tombstone & Range Scan Test\n");
    printf("========================================\n");
    printf("Entries: %d, Value size: %d bytes\n", NUM_KEYS, VALUE_SIZE);
    
    char** keys = (char**)malloc(NUM_KEYS * sizeof(char*));
    char** values = (char**)malloc(NUM_KEYS * sizeof(char*));
    char value_template[VALUE_SIZE];
    memset(value_template, 'X', VALUE_SIZE);
    
    for (int i = 0; i < NUM_KEYS; i++) {
        keys[i] = (char*)malloc(32);
        snprintf(keys[i], 32, "key_%08d", i);
        values[i] = (char*)malloc(VALUE_SIZE);
        memcpy(values[i], value_template, VALUE_SIZE);
        snprintf(values[i] + VALUE_SIZE - 16, 16, "_%08d", i);
    }
    
    /* ============================================================
     * Test 1: 写入全部数据，删除部分，强制刷盘 + 合并
     * ============================================================ */
    printf("\n=== Test 1: Write + Delete + Flush + Compact ===\n");
    
    kv_store_t* db = kv_open(dir);
    if (!db) {
        printf("ERROR: Failed to open database\n");
        return 1;
    }
    
    /* 写入所有 key */
    for (int i = 0; i < NUM_KEYS; i++) {
        kv_put(db, keys[i], strlen(keys[i]), values[i], VALUE_SIZE);
    }
    printf("  Wrote %d entries\n", NUM_KEYS);
    
    /* 删除前 1000 个 key */
    int del_count = 1000;
    for (int i = 0; i < del_count; i++) {
        kv_delete(db, keys[i], strlen(keys[i]));
    }
    printf("  Deleted %d entries\n", del_count);
    
    /* 强制合并，确保墓碑落盘并合并 */
    kv_force_merge(db);
    printf("  Force merge complete\n");
    
    /* 验证删除的 key 返回"不存在" */
    printf("\n--- Verifying deleted keys ---\n");
    int delete_miss = 0;
    int delete_hit = 0;
    for (int i = 0; i < del_count; i++) {
        char* val = NULL;
        size_t vlen = 0;
        if (kv_get(db, keys[i], strlen(keys[i]), &val, &vlen) == 0) {
            delete_hit++;
            kv_free(val);
            if (delete_hit <= 3) {
                printf("  ERROR: Deleted key %s should NOT be found!\n", keys[i]);
            }
        } else {
            delete_miss++;
        }
    }
    CHECK(delete_hit == 0, "All deleted keys return NOT_FOUND");
    printf("    Deleted keys: %d miss, %d hit (expected 0 hit)\n", delete_miss, delete_hit);
    
    /* 验证未删除的 key 仍然存在 */
    printf("\n--- Verifying surviving keys ---\n");
    int survive_hit = 0;
    int survive_miss = 0;
    for (int i = del_count; i < NUM_KEYS; i++) {
        char* val = NULL;
        size_t vlen = 0;
        if (kv_get(db, keys[i], strlen(keys[i]), &val, &vlen) == 0) {
            survive_hit++;
            if (vlen != VALUE_SIZE || memcmp(val, values[i], VALUE_SIZE) != 0) {
                printf("  ERROR: Key %s has wrong value!\n", keys[i]);
            }
            kv_free(val);
        } else {
            survive_miss++;
            if (survive_miss <= 3) {
                printf("  ERROR: Key %s should exist!\n", keys[i]);
            }
        }
    }
    CHECK(survive_miss == 0, "All surviving keys found");
    printf("    Surviving keys: %d hit, %d miss (expected %d hit)\n", 
           survive_hit, survive_miss, NUM_KEYS - del_count);
    
    kv_close(db);
    
    /* ============================================================
     * Test 2: 重启后验证墓碑持久化
     * ============================================================ */
    printf("\n=== Test 2: Reopen & Verify Tombstone Persistence ===\n");
    
    db = kv_open(dir);
    if (!db) {
        printf("ERROR: Reopen failed\n");
        return 1;
    }
    
    /* 再次验证删除 */
    int reopen_del_hit = 0;
    for (int i = 0; i < del_count; i++) {
        char* val = NULL;
        size_t vlen = 0;
        if (kv_get(db, keys[i], strlen(keys[i]), &val, &vlen) == 0) {
            reopen_del_hit++;
            kv_free(val);
        }
    }
    CHECK(reopen_del_hit == 0, "After reopen: deleted keys still NOT_FOUND");
    
    /* 再次验证存活 */
    int reopen_survive_miss = 0;
    for (int i = del_count; i < NUM_KEYS; i++) {
        char* val = NULL;
        size_t vlen = 0;
        if (kv_get(db, keys[i], strlen(keys[i]), &val, &vlen) != 0) {
            reopen_survive_miss++;
        } else {
            kv_free(val);
        }
    }
    CHECK(reopen_survive_miss == 0, "After reopen: surviving keys still found");
    printf("    Deleted key hits after reopen: %d (expected 0)\n", reopen_del_hit);
    printf("    Surviving key misses after reopen: %d (expected 0)\n", reopen_survive_miss);
    
    /* ============================================================
     * Test 3: 范围扫描 - 基础区间
     * ============================================================ */
    printf("\n=== Test 3: Range Scan - Basic Ranges ===\n");
    
    /* 扫描 key_00001000 ~ key_00001009（存活区，共 10 条） */
    /* 注意：kv_scan 是闭区间 [start, end] */
    kv_iter_t* iter = kv_scan(db, "key_00001000", 12, "key_00001009", 12);
    if (iter) {
        int scan_count = 0;
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            int idx = -1;
            for (int j = 0; j < NUM_KEYS; j++) {
                if (strlen(keys[j]) == kl && memcmp(keys[j], k, kl) == 0) {
                    idx = j;
                    break;
                }
            }
            if (idx < 0) {
                printf("  ERROR: Unknown key in scan: %.*s\n", (int)kl, k);
            } else if (idx < del_count) {
                printf("  ERROR: Deleted key %.*s appeared in scan!\n", (int)kl, k);
            }
            scan_count++;
            kv_free(k); kv_free(v);
        }
        kv_iter_free(iter);
        CHECK(scan_count == 10, "Range scan [1000, 1010) returns 10 entries");
        printf("    Scan count: %d (expected 10)\n", scan_count);
    } else {
        CHECK(0, "Range scan returned iterator");
    }
    
    /* 扫描删除区 key_00000000 ~ key_00000099（应返回 0 条） */
    iter = kv_scan(db, "key_00000000", 12, "key_00000100", 12);
    if (iter) {
        int scan_count = 0;
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            printf("  ERROR: Deleted key %.*s in range scan!\n", (int)kl, k);
            scan_count++;
            kv_free(k); kv_free(v);
        }
        kv_iter_free(iter);
        CHECK(scan_count == 0, "Range scan over deleted keys returns 0 entries");
        printf("    Scan count in deleted range: %d (expected 0)\n", scan_count);
    } else {
        CHECK(0, "Range scan returned iterator");
    }
    
    /* ============================================================
     * Test 4: 范围扫描 - 跨边界（跨 SSTable 和 MemTable）
     * ============================================================ */
    printf("\n=== Test 4: Range Scan - Cross-Boundary ===\n");
    
    /* 写入新数据到 MemTable，验证跨 MemTable+SSTable 扫描 */
    char cross_key[32];
    char cross_val[VALUE_SIZE];
    memset(cross_val, 'Z', VALUE_SIZE);
    
    /* 在删除区边界插入几条新 key */
    for (int i = 0; i < 5; i++) {
        snprintf(cross_key, sizeof(cross_key), "key_000000%02d", i + 50);
        snprintf(cross_val + VALUE_SIZE - 16, 16, "_cross_%02d", i);
        kv_put(db, cross_key, strlen(cross_key), cross_val, VALUE_SIZE);
    }
    
    /* 扫描跨删除区到存活区 */
    iter = kv_scan(db, "key_00000050", 12, "key_00001050", 12);
    if (iter) {
        int scan_count = 0;
        int deleted_appeared = 0;
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            scan_count++;
            /* 检查是否是删除的 key */
            for (int j = 0; j < del_count; j++) {
                if (strlen(keys[j]) == kl && memcmp(keys[j], k, kl) == 0) {
                    /* 检查是否是刚写入的 cross key（在删除区但被重新写入） */
                    /* key 格式: "key_00000050" ~ "key_00000054"，末尾两位是数字 */
                    int is_cross = 0;
                    for (int c = 0; c < 5; c++) {
                        if (kl == 12 && memcmp(k, "key_000000", 9) == 0) {
                            /* 位置 10-11 是最后两位数字 */
                            int kn = (k[10]-'0')*10 + (k[11]-'0');
                            if (kn == 50 + c) {
                                is_cross = 1;
                                break;
                            }
                        }
                    }
                    if (!is_cross) {
                        deleted_appeared++;
                        printf("  ERROR: Deleted key %.*s appeared!\n", (int)kl, k);
                    }
                    break;
                }
            }
            kv_free(k); kv_free(v);
        }
        kv_iter_free(iter);
        CHECK(deleted_appeared == 0, "No deleted keys in cross-boundary scan");
        printf("    Cross-boundary scan: %d entries, %d deleted (expected 0)\n", scan_count, deleted_appeared);
    }
    
    /* ============================================================
     * Test 5: 范围扫描 - 边界条件
     * ============================================================ */
    printf("\n=== Test 5: Range Scan - Edge Cases ===\n");
    
    /* 全量扫描（NULL 边界） */
    iter = kv_scan(db, NULL, 0, NULL, 0);
    if (iter) {
        int scan_count = 0;
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            scan_count++;
            kv_free(k); kv_free(v);
        }
        kv_iter_free(iter);
        int expected = NUM_KEYS - del_count + 5; /* 存活 + 5 条 cross keys */
        CHECK(scan_count == expected, "Full scan returns correct count");
        printf("    Full scan: %d entries (expected %d)\n", scan_count, expected);
    }
    
    /* 空区间扫描（start > end） */
    iter = kv_scan(db, "key_00005000", 12, "key_00001000", 12);
    if (iter) {
        int scan_count = 0;
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            scan_count++;
            kv_free(k); kv_free(v);
        }
        kv_iter_free(iter);
        CHECK(scan_count == 0, "Reverse range returns 0 entries");
    }
    
    /* 扫描不存在的前缀区间 */
    iter = kv_scan(db, "zzzzzzzzzzzz", 12, "zzzzzzzzzzzzz", 13);
    if (iter) {
        int scan_count = 0;
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            scan_count++;
            kv_free(k); kv_free(v);
        }
        kv_iter_free(iter);
        CHECK(scan_count == 0, "Non-existent range returns 0 entries");
    }
    
    /* 扫描单 key 区间（start == end，闭区间） */
    iter = kv_scan(db, "key_00002000", 12, "key_00002000", 12);
    if (iter) {
        int scan_count = 0;
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            scan_count++;
            /* 检查值是否正确 */
            if (kl == 12 && memcmp(k, "key_00002000", 12) == 0) {
                if (vl != VALUE_SIZE || memcmp(v, values[2000], VALUE_SIZE) != 0) {
                    printf("  ERROR: Value mismatch for key_00002000\n");
                }
            }
            kv_free(k); kv_free(v);
        }
        kv_iter_free(iter);
        CHECK(scan_count == 1, "Single-key scan returns 1 entry");
        printf("    Single-key scan: %d entries (expected 1)\n", scan_count);
    }
    
    /* ============================================================
     * Test 6: 扫描结果有序性验证
     * ============================================================ */
    printf("\n=== Test 6: Scan Ordering ===\n");
    
    iter = kv_scan(db, NULL, 0, NULL, 0);
    if (iter) {
        int scan_count = 0;
        int order_errors = 0;
        char* prev_k = NULL;
        size_t prev_kl = 0;
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        
        while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            if (prev_k) {
                /* 比较当前 key 是否 >= prev_key */
                size_t min_len = prev_kl < kl ? prev_kl : kl;
                int cmp = memcmp(prev_k, k, min_len);
                if (cmp > 0 || (cmp == 0 && prev_kl > kl)) {
                    printf("  ERROR: Out of order: %.*s > %.*s\n", 
                           (int)prev_kl, prev_k, (int)kl, k);
                    order_errors++;
                }
            }
            kv_free(prev_k);
            prev_k = k;
            prev_kl = kl;
            kv_free(v);
            scan_count++;
        }
        kv_free(prev_k);
        kv_iter_free(iter);
        CHECK(order_errors == 0, "Scan results are in sorted order");
        printf("    Scanned %d entries in order\n", scan_count);
    }
    
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
    printf("Test complete: %d errors\n", errors);
    printf("========================================\n");
    
#ifdef _WIN32
    _CrtDumpMemoryLeaks();
#endif
    
    return errors > 0 ? 1 : 0;
}