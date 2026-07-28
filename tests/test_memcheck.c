/*
 * 内存泄漏检测工具
 *
 * 使用 Windows CRT 调试堆检测内存泄漏。
 * 将 _CrtDumpMemoryLeaks 输出重定向到 stderr 以便在控制台查看。
 *
 * 用法：
 *   test_memcheck.exe             - 运行基准测试并检测泄漏
 *   test_memcheck.exe crash       - 运行崩溃恢复测试并检测泄漏
 *   test_memcheck.exe concurrent  - 运行并发测试并检测泄漏
 */

#include "kv_store.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#include <crtdbg.h>
#endif

/* ================================================================
 * CRT 内存泄漏检测设置
 * ================================================================ */

static void setup_memcheck() {
#ifdef _WIN32
    /* 启用内存泄漏检测 */
    int tmpFlag = _CrtSetDbgFlag(_CRTDBG_REPORT_FLAG);
    tmpFlag |= _CRTDBG_LEAK_CHECK_DF;
    tmpFlag |= _CRTDBG_ALLOC_MEM_DF;
    /* 不自动 dump，我们在程序退出前手动 dump */
    tmpFlag &= ~_CRTDBG_LEAK_CHECK_DF;
    _CrtSetDbgFlag(tmpFlag);

    /* 将 CRT 调试输出重定向到 stderr */
    _CrtSetReportMode(_CRT_WARN, _CRTDBG_MODE_FILE);
    _CrtSetReportFile(_CRT_WARN, _CRTDBG_FILE_STDERR);
    _CrtSetReportMode(_CRT_ERROR, _CRTDBG_MODE_FILE);
    _CrtSetReportFile(_CRT_ERROR, _CRTDBG_FILE_STDERR);
    _CrtSetReportMode(_CRT_ASSERT, _CRTDBG_MODE_FILE);
    _CrtSetReportFile(_CRT_ASSERT, _CRTDBG_FILE_STDERR);
#endif
}

static void dump_memleaks() {
#ifdef _WIN32
    fprintf(stderr, "\n--- Memory Leak Check ---\n");
    _CrtDumpMemoryLeaks();
    fprintf(stderr, "--- End Memory Leak Check ---\n");
#endif
}

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

/* ================================================================
 * 基准内存测试
 * ================================================================ */

#define NUM_KEYS 5000
#define VALUE_SIZE 128

static int run_basic_test() {
    printf("=== Basic Memory Leak Test ===\n");
    printf("Operations: %d puts, %d gets, %d deletes, %d scans\n",
           NUM_KEYS, NUM_KEYS, NUM_KEYS / 10, 1);

    const char* dir = "./memcheck_db";
    rmrf(dir);

    /* 生成测试数据 */
    char** keys = (char**)malloc(NUM_KEYS * sizeof(char*));
    char** values = (char**)malloc(NUM_KEYS * sizeof(char*));
    for (int i = 0; i < NUM_KEYS; i++) {
        keys[i] = (char*)malloc(32);
        snprintf(keys[i], 32, "mk_%08d", i);
        values[i] = (char*)malloc(VALUE_SIZE);
        memset(values[i], 'M', VALUE_SIZE);
        snprintf(values[i] + VALUE_SIZE - 16, 16, "_%08d", i);
    }

    /* 打开数据库 */
    kv_store_t* db = kv_open(dir);
    if (!db) {
        printf("ERROR: Failed to open database\n");
        return 1;
    }

    /* 写入 */
    printf("Writing %d entries...\n", NUM_KEYS);
    for (int i = 0; i < NUM_KEYS; i++) {
        kv_put(db, keys[i], strlen(keys[i]), values[i], VALUE_SIZE);
    }

    /* 读取 */
    printf("Reading %d entries...\n", NUM_KEYS);
    int hits = 0, misses = 0;
    for (int i = 0; i < NUM_KEYS; i++) {
        char* val = NULL; size_t vlen = 0;
        if (kv_get(db, keys[i], strlen(keys[i]), &val, &vlen) == 0) {
            hits++;
            kv_free(val);
        } else {
            misses++;
        }
    }
    printf("  Hits: %d, Misses: %d\n", hits, misses);

    /* 删除 10% */
    int del_count = NUM_KEYS / 10;
    printf("Deleting %d entries...\n", del_count);
    for (int i = 0; i < del_count; i++) {
        kv_delete(db, keys[i], strlen(keys[i]));
    }

    /* 扫描 */
    printf("Scanning...\n");
    kv_iter_t* iter = kv_scan(db, NULL, 0, NULL, 0);
    if (iter) {
        int scan_count = 0;
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            scan_count++;
            kv_free(k);
            kv_free(v);
        }
        kv_iter_free(iter);
        printf("  Scanned: %d entries\n", scan_count);
    }

    /* 强制合并 */
    kv_force_merge(db);

    /* 关闭 */
    kv_close(db);

    /* 重新打开验证 */
    printf("Reopening...\n");
    db = kv_open(dir);
    if (db) {
        int verify_hits = 0, verify_misses = 0;
        for (int i = 0; i < NUM_KEYS; i++) {
            char* val = NULL; size_t vlen = 0;
            if (kv_get(db, keys[i], strlen(keys[i]), &val, &vlen) == 0) {
                verify_hits++;
                kv_free(val);
            } else {
                verify_misses++;
            }
        }
        printf("  After reopen: hits=%d, misses=%d (expected %d deleted)\n",
               verify_hits, verify_misses, del_count);
        kv_close(db);
    }

    rmrf(dir);

    /* 清理测试数据 */
    for (int i = 0; i < NUM_KEYS; i++) {
        free(keys[i]);
        free(values[i]);
    }
    free(keys);
    free(values);

    printf("Basic test complete.\n");
    return 0;
}

/* ================================================================
 * 主函数
 * ================================================================ */

int main() {
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    setup_memcheck();

    printf("========================================\n");
    printf("Memory Leak Detection Test\n");
    printf("========================================\n\n");

    int result = run_basic_test();

    printf("\n========================================\n");
    printf("Test result: %s\n", result == 0 ? "PASS" : "FAIL");
    printf("========================================\n");

    /* 手动 dump 内存泄漏 */
    dump_memleaks();

    return result;
}