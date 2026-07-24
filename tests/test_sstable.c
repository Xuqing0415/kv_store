#include "sstable.h"
#include "skiplist.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int test_pass = 0;
static int test_fail = 0;

#define TEST(name) static void test_##name(void)
#define ASSERT(cond) do { if (!(cond)) { printf("FAIL: %s at line %d\n", #cond, __LINE__); test_fail++; } else { test_pass++; } } while (0)
#define ASSERT_EQ(a, b) ASSERT((a) == (b))

TEST(sstable_write_read) {
    const char* path = "./test_sstable.sst";
    remove(path);
    
    skiplist_t* sl = skiplist_new();
    ASSERT(sl != NULL);
    
    ASSERT_EQ(skiplist_insert(sl, "apple", 5, "fruit", 5), 0);
    ASSERT_EQ(skiplist_insert(sl, "banana", 6, "fruit", 5), 0);
    ASSERT_EQ(skiplist_insert(sl, "cherry", 6, "fruit", 5), 0);
    
    ASSERT_EQ(sstable_write(path, 1, sl), 0);
    skiplist_free(sl);
    
    sstable_t* sst = sstable_open(path, 1);
    ASSERT(sst != NULL);
    
    char* value = NULL;
    size_t vlen = 0;
    
    ASSERT_EQ(sstable_lookup(sst, "banana", 6, &value, &vlen), 0);
    ASSERT_EQ(vlen, 5);
    ASSERT(memcmp(value, "fruit", 5) == 0);
    kv_free(value);
    
    ASSERT_EQ(sstable_lookup(sst, "nonexistent", 11, &value, &vlen), -1);
    
    sstable_close(sst);
    remove(path);
}

TEST(sstable_iter) {
    const char* path = "./test_sstable_iter.sst";
    remove(path);
    
    skiplist_t* sl = skiplist_new();
    ASSERT(sl != NULL);
    
    ASSERT_EQ(skiplist_insert(sl, "a", 1, "1", 1), 0);
    ASSERT_EQ(skiplist_insert(sl, "b", 1, "2", 1), 0);
    ASSERT_EQ(skiplist_insert(sl, "c", 1, "3", 1), 0);
    
    ASSERT_EQ(sstable_write(path, 2, sl), 0);
    skiplist_free(sl);
    
    sstable_t* sst = sstable_open(path, 2);
    ASSERT(sst != NULL);
    
    sstable_iter_t* iter = sstable_new_iterator(sst);
    ASSERT(iter != NULL);
    
    char* k = NULL;
    size_t kl = 0;
    char* v = NULL;
    size_t vl = 0;
    
    ASSERT_EQ(sstable_iter_next(iter, &k, &kl, &v, &vl), 0);
    ASSERT_EQ(kl, 1);
    ASSERT(k[0] == 'a');
    kv_free(k); kv_free(v);
    
    ASSERT_EQ(sstable_iter_next(iter, &k, &kl, &v, &vl), 0);
    ASSERT_EQ(kl, 1);
    ASSERT(k[0] == 'b');
    kv_free(k); kv_free(v);
    
    ASSERT_EQ(sstable_iter_next(iter, &k, &kl, &v, &vl), 0);
    ASSERT_EQ(kl, 1);
    ASSERT(k[0] == 'c');
    kv_free(k); kv_free(v);
    
    ASSERT_EQ(sstable_iter_next(iter, &k, &kl, &v, &vl), -1);
    
    sstable_iter_free(iter);
    sstable_close(sst);
    remove(path);
}

TEST(sstable_multiple_entries) {
    const char* path = "./test_sstable_multi.sst";
    remove(path);
    
    skiplist_t* sl = skiplist_new();
    ASSERT(sl != NULL);
    
    for (int i = 0; i < 100; i++) {
        char key[32];
        char value[32];
        snprintf(key, sizeof(key), "key_%03d", i);
        snprintf(value, sizeof(value), "value_%03d", i);
        ASSERT_EQ(skiplist_insert(sl, key, strlen(key), value, strlen(value)), 0);
    }
    
    ASSERT_EQ(sstable_write(path, 3, sl), 0);
    skiplist_free(sl);
    
    sstable_t* sst = sstable_open(path, 3);
    ASSERT(sst != NULL);
    
    for (int i = 0; i < 100; i++) {
        char key[32];
        char expected[32];
        snprintf(key, sizeof(key), "key_%03d", i);
        snprintf(expected, sizeof(expected), "value_%03d", i);
        
        char* value = NULL;
        size_t vlen = 0;
        ASSERT_EQ(sstable_lookup(sst, key, strlen(key), &value, &vlen), 0);
        ASSERT(memcmp(value, expected, strlen(expected)) == 0);
        kv_free(value);
    }
    
    sstable_close(sst);
    remove(path);
}

int main() {
    printf("Running SSTable tests...\n");
    
    test_sstable_write_read();
    test_sstable_iter();
    test_sstable_multiple_entries();
    
    printf("Pass: %d, Fail: %d\n", test_pass, test_fail);
    
    return test_fail == 0 ? 0 : 1;
}