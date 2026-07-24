#include "wal.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#ifdef _WIN32
#include <windows.h>
#define rmdir(path) RemoveDirectoryA(path)
#else
#include <unistd.h>
#endif

static int test_pass = 0;
static int test_fail = 0;

#define TEST(name) static void test_##name(void)
#define ASSERT(cond) do { if (!(cond)) { printf("FAIL: %s at line %d\n", #cond, __LINE__); test_fail++; } else { test_pass++; } } while (0)
#define ASSERT_EQ(a, b) ASSERT((a) == (b))

static int replay_count = 0;

static int replay_callback(wal_record_t* record, void* arg) {
    (void)arg;
    replay_count++;
    
    if (replay_count == 1) {
        ASSERT(record->type == WAL_PUT);
        ASSERT(memcmp(record->key, "key1", 4) == 0);
        ASSERT(memcmp(record->value, "value1", 6) == 0);
    } else if (replay_count == 2) {
        ASSERT(record->type == WAL_PUT);
        ASSERT(memcmp(record->key, "key2", 4) == 0);
        ASSERT(memcmp(record->value, "value2", 6) == 0);
    }
    
    return 0;
}

TEST(wal_basic) {
    const char* path = "./test_wal.log";
    remove(path);
    
    wal_t* wal = wal_open(path);
    ASSERT(wal != NULL);
    
    ASSERT_EQ(wal_write(wal, WAL_PUT, "key1", 4, "value1", 6), 0);
    ASSERT_EQ(wal_write(wal, WAL_PUT, "key2", 4, "value2", 6), 0);
    
    wal_close(wal);
    
    wal = wal_open(path);
    ASSERT(wal != NULL);
    
    replay_count = 0;
    ASSERT_EQ(wal_replay(wal, replay_callback, NULL), 0);
    ASSERT_EQ(replay_count, 2);
    
    wal_close(wal);
    remove(path);
}

TEST(wal_recovery) {
    const char* path = "./test_wal_recovery.log";
    remove(path);
    
    wal_t* wal = wal_open(path);
    ASSERT(wal != NULL);
    
    for (int i = 0; i < 100; i++) {
        char key[32];
        char value[32];
        snprintf(key, sizeof(key), "key_%d", i);
        snprintf(value, sizeof(value), "value_%d", i);
        ASSERT_EQ(wal_write(wal, WAL_PUT, key, strlen(key), value, strlen(value)), 0);
    }
    
    wal_close(wal);
    
    wal = wal_open(path);
    ASSERT(wal != NULL);
    
    replay_count = 0;
    ASSERT_EQ(wal_replay(wal, replay_callback, NULL), 0);
    ASSERT_EQ(replay_count, 100);
    
    wal_close(wal);
    remove(path);
}

TEST(wal_delete) {
    const char* path = "./test_wal_delete.log";
    remove(path);
    
    wal_t* wal = wal_open(path);
    ASSERT(wal != NULL);
    
    ASSERT_EQ(wal_write(wal, WAL_PUT, "key1", 4, "value1", 6), 0);
    ASSERT_EQ(wal_write(wal, WAL_DELETE, "key1", 4, NULL, 0), 0);
    
    wal_close(wal);
    
    wal = wal_open(path);
    ASSERT(wal != NULL);
    
    replay_count = 0;
    ASSERT_EQ(wal_replay(wal, replay_callback, NULL), 0);
    ASSERT_EQ(replay_count, 2);
    
    wal_close(wal);
    remove(path);
}

int main() {
    printf("Running WAL tests...\n");
    
    test_wal_basic();
    test_wal_recovery();
    test_wal_delete();
    
    printf("Pass: %d, Fail: %d\n", test_pass, test_fail);
    
    return test_fail == 0 ? 0 : 1;
}