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
#define ASSERT_NE(a, b) ASSERT((a) != (b))

TEST(skiplist_basic) {
    skiplist_t* sl = skiplist_new();
    ASSERT(sl != NULL);
    ASSERT_EQ(skiplist_count(sl), 0);
    
    ASSERT_EQ(skiplist_insert(sl, "key1", 4, "value1", 6), 0);
    ASSERT_EQ(skiplist_count(sl), 1);
    
    char* value = NULL;
    size_t vlen = 0;
    ASSERT_EQ(skiplist_lookup(sl, "key1", 4, &value, &vlen), 0);
    ASSERT_EQ(vlen, 6);
    ASSERT(memcmp(value, "value1", 6) == 0);
    kv_free(value);
    
    ASSERT_EQ(skiplist_lookup(sl, "key2", 4, &value, &vlen), -1);
    
    ASSERT_EQ(skiplist_delete(sl, "key1", 4), 0);
    ASSERT_EQ(skiplist_count(sl), 1);
    ASSERT_EQ(skiplist_lookup(sl, "key1", 4, &value, &vlen), -1);
    
    skiplist_free(sl);
}

TEST(skiplist_iter) {
    skiplist_t* sl = skiplist_new();
    ASSERT(sl != NULL);
    
    ASSERT_EQ(skiplist_insert(sl, "a", 1, "1", 1), 0);
    ASSERT_EQ(skiplist_insert(sl, "b", 1, "2", 1), 0);
    ASSERT_EQ(skiplist_insert(sl, "c", 1, "3", 1), 0);
    
    skiplist_iter_t* iter = skiplist_new_iterator(sl);
    ASSERT(iter != NULL);
    
    char* k = NULL;
    size_t kl = 0;
    char* v = NULL;
    size_t vl = 0;
    
    ASSERT_EQ(skiplist_iter_next(iter, &k, &kl, &v, &vl), 0);
    ASSERT_EQ(kl, 1);
    ASSERT(k[0] == 'a');
    kv_free(k); kv_free(v);
    
    ASSERT_EQ(skiplist_iter_next(iter, &k, &kl, &v, &vl), 0);
    ASSERT_EQ(kl, 1);
    ASSERT(k[0] == 'b');
    kv_free(k); kv_free(v);
    
    ASSERT_EQ(skiplist_iter_next(iter, &k, &kl, &v, &vl), 0);
    ASSERT_EQ(kl, 1);
    ASSERT(k[0] == 'c');
    kv_free(k); kv_free(v);
    
    ASSERT_EQ(skiplist_iter_next(iter, &k, &kl, &v, &vl), -1);
    
    skiplist_iter_free(iter);
    skiplist_free(sl);
}

TEST(skiplist_update) {
    skiplist_t* sl = skiplist_new();
    ASSERT(sl != NULL);
    
    ASSERT_EQ(skiplist_insert(sl, "key", 3, "old", 3), 0);
    
    char* value = NULL;
    size_t vlen = 0;
    ASSERT_EQ(skiplist_lookup(sl, "key", 3, &value, &vlen), 0);
    ASSERT_EQ(vlen, 3);
    kv_free(value);
    
    ASSERT_EQ(skiplist_insert(sl, "key", 3, "new", 3), 0);
    
    ASSERT_EQ(skiplist_lookup(sl, "key", 3, &value, &vlen), 0);
    ASSERT_EQ(vlen, 3);
    ASSERT(memcmp(value, "new", 3) == 0);
    kv_free(value);
    
    ASSERT_EQ(skiplist_count(sl), 1);
    
    skiplist_free(sl);
}

int main() {
    printf("Running skiplist tests...\n");
    
    test_skiplist_basic();
    test_skiplist_iter();
    test_skiplist_update();
    
    printf("Pass: %d, Fail: %d\n", test_pass, test_fail);
    
    return test_fail == 0 ? 0 : 1;
}