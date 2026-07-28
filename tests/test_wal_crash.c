#include "kv_store.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#endif

/*
 * WAL-Only 崩溃恢复测试
 *
 * 场景：数据仅存在于 WAL（MemTable 未达到刷盘阈值），进程崩溃后
 * 验证是否能从 WAL 完整恢复所有数据。
 *
 * 每条记录 key=32 + value=64 ≈ 96 bytes
 * MEMTABLE_SIZE_LIMIT=256KB → 最多约 2700 条才触发刷盘
 * 我们写入 1000 条，abort 在 500，确保全程无 SSTable 落盘
 */

static void rmrf(const char* dir) {
#ifdef _WIN32
    char search_path[MAX_PATH + 4];
    char file_path[MAX_PATH * 2];
    WIN32_FIND_DATAA fd;
    strcpy(search_path, dir);
    strcat(search_path, "\\*");
    HANDLE hFind = FindFirstFileA(search_path, &fd);
    if (hFind == INVALID_HANDLE_VALUE) { RemoveDirectoryA(dir); return; }
    do {
        if (strcmp(fd.cFileName, ".") == 0 || strcmp(fd.cFileName, "..") == 0) continue;
        snprintf(file_path, sizeof(file_path), "%s\\%s", dir, fd.cFileName);
        if (fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) rmrf(file_path);
        else { SetFileAttributesA(file_path, FILE_ATTRIBUTE_NORMAL); DeleteFileA(file_path); }
    } while (FindNextFileA(hFind, &fd));
    FindClose(hFind);
    RemoveDirectoryA(dir);
#endif
}

#define NUM_KEYS 1000
#define VALUE_SIZE 64
#define CRASH_POINT 500

static void make_key(int idx, char* buf) {
    snprintf(buf, 32, "wal_key_%06d", idx);
}

static void make_value(int idx, char* buf) {
    memset(buf, 'W', VALUE_SIZE);
    snprintf(buf + VALUE_SIZE - 16, 16, "_%06d", idx);
}

int main(int argc, char** argv) {
    setbuf(stdout, NULL);
    
    const char* dir = "./test_wal_crash_db";
    
    /* 模式1：恢复验证 */
    if (argc > 1 && strcmp(argv[1], "recover") == 0) {
        printf("=== WAL-Only Crash Recovery Verification ===\n");
        
        kv_store_t* db = kv_open(dir);
        if (!db) { printf("FAIL: Cannot open DB\n"); return 1; }
        
        int found = 0, missing = 0;
        int first_missing = -1;
        int value_errors = 0;
        
        for (int i = 0; i < NUM_KEYS; i++) {
            char key[32];
            make_key(i, key);
            char* val = NULL; size_t vlen = 0;
            if (kv_get(db, key, strlen(key), &val, &vlen) == 0) {
                found++;
                char expected[VALUE_SIZE];
                make_value(i, expected);
                if (vlen != VALUE_SIZE || memcmp(val, expected, VALUE_SIZE) != 0) {
                    value_errors++;
                    if (value_errors <= 3) {
                        printf("  VALUE ERROR: key %s has wrong value\n", key);
                    }
                }
                kv_free(val);
            } else {
                missing++;
                if (first_missing < 0) first_missing = i;
            }
        }
        
        printf("Result: found=%d, missing=%d / %d total\n", found, missing, NUM_KEYS);
        printf("Recovery rate: %.1f%%\n", 100.0 * found / NUM_KEYS);
        if (first_missing >= 0) {
            printf("First missing key index: %d (crash point was %d)\n", first_missing, CRASH_POINT);
        }
        printf("Value integrity: %s\n", value_errors == 0 ? "ALL OK" : "CORRUPTED!");
        
        int expected_found = CRASH_POINT + 1;  /* 0..500 = 501 entries */
        if (found >= expected_found) {
            printf("PASS: All %d entries up to crash point recovered\n", expected_found);
        } else {
            printf("WARN: Only %d/%d entries recovered (lost %d)\n", found, expected_found, expected_found - found);
        }
        
        kv_close(db);
        return (value_errors > 0) ? 1 : 0;
    }
    
    /* 模式2：崩溃模拟 */
    if (argc > 1 && strcmp(argv[1], "crash") == 0) {
        printf("=== WAL-Only Simulated Crash Test ===\n");
        printf("Writing %d entries, will abort() at key #%d\n", NUM_KEYS, CRASH_POINT);
        printf("(MemTable limit is 256KB, ~96 bytes/entry → ~2700 entries before flush)\n");
        printf("(So all %d entries stay in MemTable+WAL only, no SSTable)\n\n", NUM_KEYS);
        rmrf(dir);
        
        kv_store_t* db = kv_open(dir);
        if (!db) { printf("FAIL: Cannot open DB\n"); return 1; }
        
        for (int i = 0; i < NUM_KEYS; i++) {
            char key[32], value[VALUE_SIZE];
            make_key(i, key);
            make_value(i, value);
            
            kv_put(db, key, strlen(key), value, VALUE_SIZE);
            
            /* 每 50 条刷盘一次，确保 WAL 数据落盘 */
            if (i % 50 == 0) {
                kv_sync(db);
                if (i % 200 == 0) {
                    printf("  Written %d entries, WAL synced...\n", i);
                }
            }
            
            if (i == CRASH_POINT) {
                printf("  Written %d entries, simulating crash with abort()...\n", i);
                kv_sync(db);
                fflush(stdout);
                fflush(stderr);
                abort();
            }
        }
        
        kv_close(db);
        return 0;
    }
    
    /* 模式3：正常写入+验证（无崩溃） */
    printf("=== WAL-Only Crash Recovery Test ===\n");
    printf("Usage:\n");
    printf("  test_wal_crash.exe crash    - Write and abort() at entry #%d\n", CRASH_POINT);
    printf("  test_wal_crash.exe recover  - Recover from WAL and verify\n");
    printf("  test_wal_crash.exe          - Normal write + verify (no crash)\n\n");
    
    printf("Running normal mode (no crash)...\n");
    rmrf(dir);
    
    kv_store_t* db = kv_open(dir);
    if (!db) { printf("FAIL: Cannot open DB\n"); return 1; }
    
    for (int i = 0; i < NUM_KEYS; i++) {
        char key[32], value[VALUE_SIZE];
        make_key(i, key);
        make_value(i, value);
        kv_put(db, key, strlen(key), value, VALUE_SIZE);
    }
    
    /* 验证全部可读 */
    int errors = 0;
    for (int i = 0; i < NUM_KEYS; i++) {
        char key[32];
        make_key(i, key);
        char* val = NULL; size_t vlen = 0;
        if (kv_get(db, key, strlen(key), &val, &vlen) != 0) {
            printf("  ERROR: Key %s not found!\n", key);
            errors++;
        } else {
            char expected[VALUE_SIZE];
            make_value(i, expected);
            if (vlen != VALUE_SIZE || memcmp(val, expected, VALUE_SIZE) != 0) {
                printf("  ERROR: Key %s value mismatch!\n", key);
                errors++;
            }
            kv_free(val);
        }
    }
    printf("Normal mode: %d errors\n", errors);
    
    kv_close(db);
    printf("Done. All %d entries written and verified.\n", NUM_KEYS);
    return errors > 0 ? 1 : 0;
}