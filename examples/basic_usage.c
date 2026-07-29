/**
 * basic_usage.c — KV Store 基本用法演示
 *
 * 编译: gcc -I../include -L../build_win basic_usage.c -lkv_store -o basic_usage
 * 运行: ./basic_usage
 */

#include "kv_store.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(void) {
    const char* data_dir = "./example_db";

    printf("========================================\n");
    printf("  KV Store — Basic Usage Example\n");
    printf("========================================\n\n");

    /* 1. 打开数据库 */
    printf("[1] Opening database at %s...\n", data_dir);
    kv_store_t* db = kv_open(data_dir);
    if (!db) {
        fprintf(stderr, "ERROR: Failed to open database\n");
        return 1;
    }
    printf("    Database opened.\n\n");

    /* 2. 写入数据 */
    printf("[2] Writing data...\n");
    kv_put(db, "name", 4, "Alice", 5);
    kv_put(db, "age", 3, "30", 2);
    kv_put(db, "city", 4, "Shanghai", 8);
    kv_put(db, "language", 8, "C11", 3);
    printf("    Wrote 4 key-value pairs.\n\n");

    /* 3. 读取数据 */
    printf("[3] Reading data...\n");
    const char* keys[] = {"name", "age", "city", "language", "missing"};
    for (int i = 0; i < 5; i++) {
        char* val = NULL;
        size_t vlen = 0;
        if (kv_get(db, keys[i], strlen(keys[i]), &val, &vlen) == 0) {
            printf("    GET %s = %.*s\n", keys[i], (int)vlen, val);
            kv_free(val);
        } else {
            printf("    GET %s = (not found)\n", keys[i]);
        }
    }
    printf("\n");

    /* 4. 范围扫描 */
    printf("[4] Scanning all keys...\n");
    kv_iter_t* iter = kv_scan(db, NULL, 0, NULL, 0);
    if (iter) {
        char* key = NULL, *value = NULL;
        size_t klen = 0, vlen = 0;
        int count = 0;
        while (kv_iter_next(iter, &key, &klen, &value, &vlen) == 0) {
            printf("    %.*s => %.*s\n", (int)klen, key, (int)vlen, value);
            kv_free(key);
            kv_free(value);
            count++;
        }
        printf("    Total: %d entries\n", count);
        kv_iter_free(iter);
    }
    printf("\n");

    /* 5. 范围扫描（按前缀） */
    printf("[5] Scanning range [\"a\", \"m\")...\n");
    iter = kv_scan(db, "a", 1, "m", 1);
    if (iter) {
        char* key = NULL, *value = NULL;
        size_t klen = 0, vlen = 0;
        while (kv_iter_next(iter, &key, &klen, &value, &vlen) == 0) {
            printf("    %.*s => %.*s\n", (int)klen, key, (int)vlen, value);
            kv_free(key);
            kv_free(value);
        }
        kv_iter_free(iter);
    }
    printf("\n");

    /* 6. 删除数据 */
    printf("[6] Deleting \"language\"...\n");
    kv_delete(db, "language", 8);
    char* val = NULL;
    size_t vlen = 0;
    if (kv_get(db, "language", 8, &val, &vlen) == 0) {
        kv_free(val);
        printf("    (still exists)\n");
    } else {
        printf("    Deleted successfully.\n");
    }
    printf("\n");

    /* 7. 创建快照 */
    printf("[7] Creating snapshot...\n");
    kv_snapshot_t* snap = kv_snapshot_create(db);
    if (snap) {
        val = NULL;
        vlen = 0;
        if (kv_snapshot_get(snap, "name", 4, &val, &vlen) == 0) {
            printf("    Snapshot GET name = %.*s\n", (int)vlen, val);
            kv_free(val);
        }
        kv_snapshot_free(snap);
    }
    printf("\n");

    /* 8. 关闭数据库 */
    printf("[8] Closing database...\n");
    kv_close(db);
    printf("    Database closed.\n\n");

    printf("========================================\n");
    printf("  All examples completed successfully!\n");
    printf("========================================\n");

    return 0;
}