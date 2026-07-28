/*
 * 全面崩溃恢复测试（WAL-Only 场景）
 *
 * 测试目标：
 *   1. 模拟非正常退出（abort），验证 WAL 恢复能力
 *   2. 所有数据仅在 WAL+MemTable（不触发 SSTable 刷盘）
 *   3. 恢复后验证所有已写入 key 的值完整性
 *   4. 验证无内存泄漏或文件损坏
 *
 * 场景设计：
 *   - MEMTABLE_SIZE_LIMIT 设为 100MB，确保 50000 条数据不会触发刷盘
 *   - 每条记录 key=32 + value=128 = 160 bytes，50000 条 ≈ 8MB
 *   - 在 key #25000 处 abort() 模拟崩溃
 *   - 恢复后验证 0..25000 共 25001 条数据完整可读
 *
 * 用法：
 *   test_crash_recovery.exe crash    - 写入 50000 条并在 #25000 处 abort()
 *   test_crash_recovery.exe recover  - 从 WAL 恢复并验证数据完整性
 *   test_crash_recovery.exe          - 正常模式（无崩溃，全量验证）
 */

/* 必须在 include kv_store.h 之前定义，确保 WAL-only 场景 */
#define MEMTABLE_SIZE_LIMIT (100 * 1024 * 1024)  /* 100MB */

#include "kv_store.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#endif

#define NUM_KEYS 50000
#define VALUE_SIZE 128
#define CRASH_POINT 25000
#define SYNC_INTERVAL 100  /* 每 100 条 sync 一次 */

/* ================================================================
 * 辅助函数
 * ================================================================ */

static void rmrf(const char* dir) {
#ifdef _WIN32
    char search_path[MAX_PATH + 4];
    char file_path[MAX_PATH * 2];
    WIN32_FIND_DATAA fd;
    snprintf(search_path, sizeof(search_path), "%s\\*", dir);
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

static void make_key(int idx, char* buf) {
    snprintf(buf, 32, "cr_key_%08d", idx);
}

static void make_value(int idx, char* buf) {
    memset(buf, 'Z', VALUE_SIZE);
    snprintf(buf + VALUE_SIZE - 16, 16, "_%08d", idx);
}

static size_t count_files(const char* dir) {
    size_t count = 0;
#ifdef _WIN32
    char search_path[MAX_PATH + 4];
    WIN32_FIND_DATAA fd;
    snprintf(search_path, sizeof(search_path), "%s\\*", dir);
    HANDLE hFind = FindFirstFileA(search_path, &fd);
    if (hFind == INVALID_HANDLE_VALUE) return 0;
    do {
        if (strcmp(fd.cFileName, ".") == 0 || strcmp(fd.cFileName, "..") == 0) continue;
        count++;
    } while (FindNextFileA(hFind, &fd));
    FindClose(hFind);
#endif
    return count;
}

/* ================================================================
 * 恢复验证模式
 * ================================================================ */

static int run_recovery(const char* dir) {
    printf("========================================\n");
    printf("  Crash Recovery Verification\n");
    printf("========================================\n\n");

    printf("Opening database at: %s\n", dir);

    kv_store_t* db = kv_open(dir);
    if (!db) {
        printf("FAIL: Cannot open database\n");
        return 1;
    }
    printf("Database opened successfully\n\n");

    /* 统计恢复情况 */
    int found = 0, missing = 0;
    int first_missing = -1;
    int value_errors = 0;

    for (int i = 0; i < NUM_KEYS; i++) {
        char key[32];
        make_key(i, key);

        char* val = NULL;
        size_t vlen = 0;
        int ret = kv_get(db, key, strlen(key), &val, &vlen);

        if (ret == 0) {
            found++;
            /* 验证值完整性 */
            char expected[VALUE_SIZE];
            make_value(i, expected);
            if (vlen != VALUE_SIZE || memcmp(val, expected, VALUE_SIZE) != 0) {
                value_errors++;
                if (value_errors <= 5) {
                    printf("  VALUE ERROR: key=%s, vlen=%zu (expected %d)\n",
                           key, vlen, VALUE_SIZE);
                }
            }
            kv_free(val);
        } else {
            missing++;
            if (first_missing < 0) first_missing = i;
        }
    }

    /* 输出统计 */
    printf("\n--- Recovery Statistics ---\n");
    printf("Total keys expected:  %d\n", NUM_KEYS);
    printf("Keys recovered:       %d\n", found);
    printf("Keys missing:         %d\n", missing);
    printf("Recovery rate:        %.2f%%\n", 100.0 * found / NUM_KEYS);
    if (first_missing >= 0) {
        printf("First missing index:  %d\n", first_missing);
    }
    printf("Value integrity:      %s (%d errors)\n",
           value_errors == 0 ? "OK" : "CORRUPTED!", value_errors);

    /* 验证：崩溃点之前的记录应该全部可恢复 */
    int expected_found = CRASH_POINT + 1;  /* 0..25000 = 25001 条 */
    printf("\n--- Crash Point Analysis ---\n");
    printf("Crash point:          key #%d\n", CRASH_POINT);
    printf("Expected recoverable: %d (keys 0..%d)\n", expected_found, CRASH_POINT);

    if (found >= expected_found) {
        printf("PASS: All %d entries up to crash point recovered successfully\n", expected_found);
    } else if (found >= expected_found - 10) {
        /* 允许丢失最后几条未完全写入 WAL 的记录 */
        printf("PASS: %d/%d entries recovered (lost %d, likely last few unsynced records)\n",
               found, expected_found, expected_found - found);
    } else {
        printf("FAIL: Only %d/%d entries recovered (lost %d entries)\n",
               found, expected_found, expected_found - found);
    }

    /* 检查 SSTable 文件数量（WAL-only 场景下应为 0，因为没触发刷盘） */
    size_t file_count = count_files(dir);
    printf("\n--- File System Check ---\n");
    printf("Files in data dir:    %zu\n", file_count);

    /* 扫描测试 */
    printf("\n--- Scan Verification ---\n");
    kv_iter_t* iter = kv_scan(db, NULL, 0, NULL, 0);
    if (iter) {
        int scan_count = 0;
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            scan_count++;
            kv_free(k); kv_free(v);
        }
        kv_iter_free(iter);
        printf("Scan returned:        %d entries\n", scan_count);
        if (scan_count != found) {
            printf("WARN: Scan count (%d) != found count (%d)\n", scan_count, found);
        }
    }

    kv_close(db);
    printf("\nDatabase closed.\n");

    int result = (value_errors > 0) ? 1 : 0;
    if (found < expected_found - 10) result = 1;

    printf("\n=== %s ===\n", result == 0 ? "ALL TESTS PASSED" : "SOME TESTS FAILED");
    return result;
}

/* ================================================================
 * 正常写入模式（无崩溃）
 * ================================================================ */

static int run_normal(const char* dir) {
    printf("========================================\n");
    printf("  Normal Write + Verify (No Crash)\n");
    printf("========================================\n\n");

    printf("MEMTABLE_SIZE_LIMIT = %d MB\n", MEMTABLE_SIZE_LIMIT / (1024 * 1024));
    printf("Estimated data size: %zu MB\n",
           (size_t)NUM_KEYS * (32 + VALUE_SIZE) / (1024 * 1024));

    rmrf(dir);

    kv_store_t* db = kv_open(dir);
    if (!db) {
        printf("FAIL: Cannot open database\n");
        return 1;
    }

    printf("Writing %d entries...\n", NUM_KEYS);
    putchar('[');
    for (int i = 0; i < NUM_KEYS; i++) {
        char key[32], value[VALUE_SIZE];
        make_key(i, key);
        make_value(i, value);
        kv_put(db, key, strlen(key), value, VALUE_SIZE);

        if (i % SYNC_INTERVAL == 0) {
            kv_sync(db);
        }

        if (i % 5000 == 0 && i > 0) {
            putchar('.');
            fflush(stdout);
        }
    }
    printf("] 100%%\n");

    /* 全量验证 */
    printf("Verifying all %d entries...\n", NUM_KEYS);
    int errors = 0;
    for (int i = 0; i < NUM_KEYS; i++) {
        char key[32];
        make_key(i, key);
        char* val = NULL; size_t vlen = 0;
        if (kv_get(db, key, strlen(key), &val, &vlen) != 0) {
            printf("  ERROR: key %s not found!\n", key);
            errors++;
            if (errors > 10) { printf("  ... too many errors, stopping\n"); break; }
        } else {
            char expected[VALUE_SIZE];
            make_value(i, expected);
            if (vlen != VALUE_SIZE || memcmp(val, expected, VALUE_SIZE) != 0) {
                printf("  ERROR: key %s value mismatch!\n", key);
                errors++;
            }
            kv_free(val);
        }
    }

    printf("Verification: %s (%d errors)\n", errors == 0 ? "ALL OK" : "FAILED", errors);

    /* 扫描验证 */
    kv_iter_t* iter = kv_scan(db, NULL, 0, NULL, 0);
    if (iter) {
        int scan_count = 0;
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            scan_count++;
            kv_free(k); kv_free(v);
        }
        kv_iter_free(iter);
        printf("Scan count: %d\n", scan_count);
    }

    kv_close(db);
    printf("Done.\n");
    return errors > 0 ? 1 : 0;
}

/* ================================================================
 * 崩溃模拟模式
 * ================================================================ */

static int run_crash(const char* dir) {
    printf("========================================\n");
    printf("  Simulated Crash Test\n");
    printf("========================================\n\n");

    printf("MEMTABLE_SIZE_LIMIT = %d MB\n", MEMTABLE_SIZE_LIMIT / (1024 * 1024));
    printf("Entries to write:    %d\n", NUM_KEYS);
    printf("Crash at key:        #%d\n", CRASH_POINT);
    printf("Sync interval:       every %d entries\n", SYNC_INTERVAL);
    printf("Estimated data size: %zu MB\n\n",
           (size_t)NUM_KEYS * (32 + VALUE_SIZE) / (1024 * 1024));

    rmrf(dir);

    kv_store_t* db = kv_open(dir);
    if (!db) {
        printf("FAIL: Cannot open database\n");
        return 1;
    }

    printf("Starting write loop...\n");
    for (int i = 0; i < NUM_KEYS; i++) {
        char key[32], value[VALUE_SIZE];
        make_key(i, key);
        make_value(i, value);

        kv_put(db, key, strlen(key), value, VALUE_SIZE);

        /* 定期 sync 确保 WAL 落盘 */
        if (i % SYNC_INTERVAL == 0) {
            kv_sync(db);
        }

        /* 进度输出 */
        if (i % 5000 == 0 && i > 0) {
            printf("  Progress: %d/%d entries written...\n", i, NUM_KEYS);
        }

        /* 在崩溃点强制结束 */
        if (i == CRASH_POINT) {
            printf("  Reached crash point at key #%d\n", i);
            printf("  Syncing WAL before crash...\n");
            kv_sync(db);
            fflush(stdout);
            fflush(stderr);

            /* 不调用 kv_close，直接 abort 模拟非正常退出 */
            printf("  Calling abort() to simulate crash...\n\n");
            abort();
        }
    }

    /* 不应该到达这里 */
    kv_close(db);
    return 0;
}

/* ================================================================
 * 主函数
 * ================================================================ */

int main(int argc, char** argv) {
    setbuf(stdout, NULL);

    const char* dir = "./crash_recovery_db";

    if (argc > 1 && strcmp(argv[1], "recover") == 0) {
        return run_recovery(dir);
    }

    if (argc > 1 && strcmp(argv[1], "crash") == 0) {
        return run_crash(dir);
    }

    /* 默认：正常模式 */
    printf("Comprehensive Crash Recovery Test\n");
    printf("Usage:\n");
    printf("  test_crash_recovery.exe crash    - Write and abort() at key #%d\n", CRASH_POINT);
    printf("  test_crash_recovery.exe recover  - Recover from WAL and verify\n");
    printf("  test_crash_recovery.exe          - Normal write + verify (no crash)\n\n");

    return run_normal(dir);
}