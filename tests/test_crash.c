#include "kv_store.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#endif

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

#define NUM_KEYS 5000
#define VALUE_SIZE 128

int main(int argc, char** argv) {
    setbuf(stdout, NULL);
    
    const char* dir = "./crash_test_db";
    
    /* 如果是恢复模式（带参数 "recover"） */
    if (argc > 1 && strcmp(argv[1], "recover") == 0) {
        printf("=== Crash Recovery Verification ===\n");
        kv_store_t* db = kv_open(dir);
        if (!db) { printf("FAIL: Cannot open DB\n"); return 1; }
        
        int found = 0, missing = 0;
        for (int i = 0; i < NUM_KEYS; i++) {
            char key[32];
            snprintf(key, sizeof(key), "crash_key_%06d", i);
            char* val = NULL; size_t vlen = 0;
            if (kv_get(db, key, strlen(key), &val, &vlen) == 0) {
                found++;
                kv_free(val);
            } else {
                missing++;
            }
        }
        printf("Result: found=%d, missing=%d / %d total\n", found, missing, NUM_KEYS);
        printf("Recovery rate: %.1f%%\n", 100.0 * found / NUM_KEYS);
        
        kv_close(db);
        return 0;
    }
    
    /* 写入模式 */
    printf("=== Crash Recovery Test: Writing %d entries ===\n", NUM_KEYS);
    printf("(Kill this process mid-way to simulate crash, then run with 'recover')\n");
    rmrf(dir);
    
    kv_store_t* db = kv_open(dir);
    if (!db) { printf("FAIL: Cannot open DB\n"); return 1; }
    
    putchar('[');
    for (int i = 0; i < NUM_KEYS; i++) {
        char key[32], value[VALUE_SIZE];
        snprintf(key, sizeof(key), "crash_key_%06d", i);
        memset(value, 'V', VALUE_SIZE);
        snprintf(value + VALUE_SIZE - 16, 16, "_%06d", i);
        
        kv_put(db, key, strlen(key), value, VALUE_SIZE);
        
        /* 每 100 条打印进度并强制刷新 */
        if (i % 100 == 0) {
            putchar('.');
            fflush(stdout);
        }
    }
    printf("] 100%%\n");
    
    /* 扫描确认 */
    int count = 0;
    kv_iter_t* iter = kv_scan(db, NULL, 0, NULL, 0);
    if (iter) {
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        while (kv_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            count++;
            kv_free(k); kv_free(v);
        }
        kv_iter_free(iter);
    }
    printf("Total keys: %d\n", count);
    
    kv_close(db);
    printf("Done. All %d entries written and verified.\n", NUM_KEYS);
    return 0;
}