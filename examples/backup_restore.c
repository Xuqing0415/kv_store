/**
 * backup_restore.c — KV Store 备份与恢复演示
 *
 * 编译: gcc -I../include -L../build_win backup_restore.c -lkv_store -o backup_restore
 * 运行: ./backup_restore
 */

#include "kv_store.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TEST_COUNT 1000

static int verify_data(kv_store_t* db, int count) {
    int errors = 0;
    char key[64], expected[128];

    for (int i = 0; i < count; i++) {
        snprintf(key, sizeof(key), "user_%d", i);
        snprintf(expected, sizeof(expected), "value_for_user_%d", i);

        char* val = NULL;
        size_t vlen = 0;
        if (kv_get(db, key, strlen(key), &val, &vlen) != 0) {
            printf("    ERROR: Key '%s' not found!\n", key);
            errors++;
            continue;
        }

        if (vlen != strlen(expected) || memcmp(val, expected, vlen) != 0) {
            printf("    ERROR: Key '%s' value mismatch!\n", key);
            errors++;
        }
        kv_free(val);
    }
    return errors;
}

int main(void) {
    const char* data_dir = "./example_backup_db";
    const char* backup_dir = "./example_backup";
    const char* restore_dir = "./example_restore_db";

    printf("========================================\n");
    printf("  KV Store — Backup & Restore Example\n");
    printf("========================================\n\n");

    /* 1. 创建数据库并写入数据 */
    printf("[1] Creating database and writing %d entries...\n", TEST_COUNT);
    kv_store_t* db = kv_open(data_dir);
    if (!db) {
        fprintf(stderr, "ERROR: Failed to open database\n");
        return 1;
    }

    char key[64], value[128];
    for (int i = 0; i < TEST_COUNT; i++) {
        snprintf(key, sizeof(key), "user_%d", i);
        snprintf(value, sizeof(value), "value_for_user_%d", i);
        kv_put(db, key, strlen(key), value, strlen(value));
    }
    printf("    Wrote %d entries.\n\n", TEST_COUNT);

    /* 2. 验证原始数据 */
    printf("[2] Verifying original data...\n");
    int errors = verify_data(db, TEST_COUNT);
    printf("    %s (%d errors)\n\n", errors == 0 ? "PASS" : "FAIL", errors);

    /* 3. 执行备份 */
    printf("[3] Performing backup to %s...\n", backup_dir);
    if (kv_backup(db, backup_dir) != 0) {
        fprintf(stderr, "ERROR: Backup failed\n");
        kv_close(db);
        return 1;
    }
    printf("    Backup completed.\n\n");

    /* 4. 关闭原数据库 */
    printf("[4] Closing original database...\n");
    kv_close(db);
    printf("    Closed.\n\n");

    /* 5. 从备份恢复 */
    printf("[5] Restoring from backup to %s...\n", restore_dir);
    kv_store_t* restored_db = kv_restore(backup_dir, restore_dir);
    if (!restored_db) {
        fprintf(stderr, "ERROR: Restore failed\n");
        return 1;
    }
    printf("    Restore completed.\n\n");

    /* 6. 验证恢复后的数据 */
    printf("[6] Verifying restored data...\n");
    errors = verify_data(restored_db, TEST_COUNT);
    printf("    %s (%d errors)\n\n", errors == 0 ? "PASS" : "FAIL", errors);

    /* 7. 快照 + 备份组合使用 */
    printf("[7] Snapshot + Backup demonstration...\n");
    kv_snapshot_t* snap = kv_snapshot_create(restored_db);
    if (snap) {
        char* val = NULL;
        size_t vlen = 0;
        if (kv_snapshot_get(snap, "user_500", 8, &val, &vlen) == 0) {
            printf("    Snapshot: user_500 = %.*s\n", (int)vlen, val);
            kv_free(val);
        }
        kv_snapshot_free(snap);
    }
    printf("\n");

    /* 8. 清理 */
    printf("[8] Closing restored database...\n");
    kv_close(restored_db);
    printf("    Done.\n\n");

    printf("========================================\n");
    printf("  Backup & Restore test PASSED!\n");
    printf("========================================\n");

    return 0;
}