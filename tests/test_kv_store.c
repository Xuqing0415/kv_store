#include "kv_store.h"
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

static void rmrf(const char* dir) {
#ifdef _WIN32
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rmdir /s /q \"%s\"", dir);
    system(cmd);
#else
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf \"%s\"", dir);
    system(cmd);
#endif
}

TEST(kv_basic) {
    const char* dir = "./test_kv_basic";
    rmrf(dir);
    
    kv_store_t* db = kv_open(dir);
    ASSERT(db != NULL);
    
    ASSERT_EQ(kv_put(db, "key1", 4, "value1", 6), 0);
    ASSERT_EQ(kv_put(db, "key2", 4, "value2", 6), 0);
    
    char* value = NULL;
    size_t vlen = 0;
    
    ASSERT_EQ(kv_get(db, "key1", 4, &value, &vlen), 0);
    ASSERT_EQ(vlen, 6);
    ASSERT(memcmp(value, "value1", 6) == 0);
    kv_free(value);
    
    ASSERT_EQ(kv_get(db, "key2", 4, &value, &vlen), 0);
    ASSERT_EQ(vlen, 6);
    ASSERT(memcmp(value, "value2", 6) == 0);
    kv_free(value);
    
    ASSERT_EQ(kv_get(db, "key3", 4, &value, &vlen), -1);
    
    kv_close(db);
    rmrf(dir);
}

TEST(kv_delete) {
    const char* dir = "./test_kv_delete";
    rmrf(dir);
    
    kv_store_t* db = kv_open(dir);
    ASSERT(db != NULL);
    
    ASSERT_EQ(kv_put(db, "key1", 4, "value1", 6), 0);
    
    char* value = NULL;
    size_t vlen = 0;
    
    ASSERT_EQ(kv_get(db, "key1", 4, &value, &vlen), 0);
    kv_free(value);
    
    ASSERT_EQ(kv_delete(db, "key1", 4), 0);
    
    ASSERT_EQ(kv_get(db, "key1", 4, &value, &vlen), -1);
    
    kv_close(db);
    rmrf(dir);
}

TEST(kv_persistence) {
    const char* dir = "./test_kv_persistence";
    rmrf(dir);
    
    kv_store_t* db = kv_open(dir);
    ASSERT(db != NULL);
    
    for (int i = 0; i < 100; i++) {
        char key[32];
        char value[32];
        snprintf(key, sizeof(key), "key_%d", i);
        snprintf(value, sizeof(value), "value_%d", i);
        ASSERT_EQ(kv_put(db, key, strlen(key), value, strlen(value)), 0);
    }
    
    kv_close(db);
    
    db = kv_open(dir);
    ASSERT(db != NULL);
    
    for (int i = 0; i < 100; i++) {
        char key[32];
        char expected[32];
        snprintf(key, sizeof(key), "key_%d", i);
        snprintf(expected, sizeof(expected), "value_%d", i);
        
        char* value = NULL;
        size_t vlen = 0;
        ASSERT_EQ(kv_get(db, key, strlen(key), &value, &vlen), 0);
        ASSERT(memcmp(value, expected, strlen(expected)) == 0);
        kv_free(value);
    }
    
    kv_close(db);
    rmrf(dir);
}

TEST(kv_scan) {
    const char* dir = "./test_kv_scan";
    rmrf(dir);
    
    kv_store_t* db = kv_open(dir);
    ASSERT(db != NULL);
    
    ASSERT_EQ(kv_put(db, "a", 1, "1", 1), 0);
    ASSERT_EQ(kv_put(db, "b", 1, "2", 1), 0);
    ASSERT_EQ(kv_put(db, "c", 1, "3", 1), 0);
    ASSERT_EQ(kv_put(db, "d", 1, "4", 1), 0);
    ASSERT_EQ(kv_put(db, "e", 1, "5", 1), 0);
    
    kv_iter_t* iter = kv_scan(db, "b", 1, "d", 1);
    ASSERT(iter != NULL);
    
    char* k = NULL;
    size_t kl = 0;
    char* v = NULL;
    size_t vl = 0;
    
    ASSERT_EQ(kv_iter_next(iter, &k, &kl, &v, &vl), 0);
    ASSERT_EQ(kl, 1);
    ASSERT(k[0] == 'b');
    kv_free(k); kv_free(v);
    
    ASSERT_EQ(kv_iter_next(iter, &k, &kl, &v, &vl), 0);
    ASSERT_EQ(kl, 1);
    ASSERT(k[0] == 'c');
    kv_free(k); kv_free(v);
    
    ASSERT_EQ(kv_iter_next(iter, &k, &kl, &v, &vl), 0);
    ASSERT_EQ(kl, 1);
    ASSERT(k[0] == 'd');
    kv_free(k); kv_free(v);
    
    ASSERT_EQ(kv_iter_next(iter, &k, &kl, &v, &vl), -1);
    
    kv_iter_free(iter);
    kv_close(db);
    rmrf(dir);
}

TEST(kv_update) {
    const char* dir = "./test_kv_update";
    rmrf(dir);
    
    kv_store_t* db = kv_open(dir);
    ASSERT(db != NULL);
    
    ASSERT_EQ(kv_put(db, "key", 3, "old", 3), 0);
    
    char* value = NULL;
    size_t vlen = 0;
    
    ASSERT_EQ(kv_get(db, "key", 3, &value, &vlen), 0);
    ASSERT_EQ(vlen, 3);
    ASSERT(memcmp(value, "old", 3) == 0);
    kv_free(value);
    
    ASSERT_EQ(kv_put(db, "key", 3, "new", 3), 0);
    
    ASSERT_EQ(kv_get(db, "key", 3, &value, &vlen), 0);
    ASSERT_EQ(vlen, 3);
    ASSERT(memcmp(value, "new", 3) == 0);
    kv_free(value);
    
    kv_close(db);
    rmrf(dir);
}

int main() {
    printf("Running KV Store tests...\n");
    
    test_kv_basic();
    test_kv_delete();
    test_kv_update();
    test_kv_scan();
    test_kv_persistence();
    
    printf("Pass: %d, Fail: %d\n", test_pass, test_fail);
    
    return test_fail == 0 ? 0 : 1;
}