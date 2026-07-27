#include "sstable.h"
#include "skiplist.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
    setbuf(stdout, NULL);
    
    const char* path = "./test_iter_debug.sst";
    remove(path);
    
    int num_entries = 1000;
    int value_size = 256;
    
    printf("Creating skiplist with %d entries...\n", num_entries);
    
    skiplist_t* sl = skiplist_new();
    char value_template[256];
    memset(value_template, 'X', value_size);
    
    for (int i = 0; i < num_entries; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%08d", i);
        char val[256];
        memcpy(val, value_template, value_size);
        snprintf(val + value_size - 16, 16, "_%08d", i);
        skiplist_insert(sl, key, strlen(key), val, value_size);
    }
    
    printf("Writing SSTable...\n");
    int ret = sstable_write(path, 1, sl);
    printf("sstable_write returned: %d\n", ret);
    skiplist_free(sl);
    
    printf("Opening SSTable...\n");
    sstable_t* sst = sstable_open(path, 1);
    if (!sst) {
        printf("ERROR: Failed to open SSTable\n");
        return 1;
    }
    printf("SSTable file_size: %zu, index_size: %zu\n", sst->file_size, sst->index_size);
    
    /* Test 1: Lookup all entries */
    printf("\nTest 1: Looking up all %d entries...\n", num_entries);
    int lookup_hits = 0, lookup_misses = 0;
    for (int i = 0; i < num_entries; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%08d", i);
        char* value = NULL;
        size_t vlen = 0;
        if (sstable_lookup(sst, key, strlen(key), &value, &vlen, NULL) == 0) {
            lookup_hits++;
            kv_free(value);
        } else {
            lookup_misses++;
            if (lookup_misses <= 5) {
                printf("  MISS: %s\n", key);
            }
        }
    }
    printf("Lookup: hits=%d, misses=%d\n", lookup_hits, lookup_misses);
    
    /* Test 2: Iterate all entries */
    printf("\nTest 2: Iterating all entries...\n");
    sstable_iter_t* iter = sstable_new_iterator(sst);
    if (!iter) {
        printf("ERROR: Failed to create iterator\n");
        sstable_close(sst);
        return 1;
    }
    
    int iter_count = 0;
    char* k = NULL; size_t kl = 0;
    char* v = NULL; size_t vl = 0;
    
    while (sstable_iter_next(iter, &k, &kl, &v, &vl) == 0) {
        if (iter_count < 3 || iter_count >= num_entries - 3) {
            printf("  #%d: key=%.*s, vlen=%zu\n", iter_count, (int)kl, k, vl);
        }
        if (iter_count == 3 && num_entries > 6) {
            printf("  ...\n");
        }
        iter_count++;
        kv_free(k);
        kv_free(v);
    }
    printf("Iterator: count=%d (expected %d)\n", iter_count, num_entries);
    
    sstable_iter_free(iter);
    sstable_close(sst);
    remove(path);
    
    printf("\n%s\n", (lookup_hits == num_entries && iter_count == num_entries) ? "ALL PASSED!" : "FAILED!");
    return (lookup_hits == num_entries && iter_count == num_entries) ? 0 : 1;
}