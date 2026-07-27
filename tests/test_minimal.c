#include "sstable.h"
#include "skiplist.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
    printf("Step 1: Create skiplist\n");
    fflush(stdout);
    
    skiplist_t* sl = skiplist_new();
    if (!sl) { printf("FAIL: skiplist_new\n"); return 1; }
    
    printf("Step 2: Insert entries\n");
    fflush(stdout);
    
    skiplist_insert(sl, "apple", 5, "fruit", 5);
    skiplist_insert(sl, "banana", 6, "fruit", 5);
    skiplist_insert(sl, "cherry", 6, "fruit", 5);
    
    printf("Step 3: Write SSTable\n");
    fflush(stdout);
    
    remove("./test_min.sst");
    int ret = sstable_write("./test_min.sst", 1, sl);
    printf("sstable_write returned: %d\n", ret);
    fflush(stdout);
    skiplist_free(sl);
    
    printf("Step 4: Open SSTable\n");
    fflush(stdout);
    
    sstable_t* sst = sstable_open("./test_min.sst", 1);
    if (!sst) { printf("FAIL: sstable_open\n"); return 1; }
    
    printf("Step 5: Lookup\n");
    fflush(stdout);
    
    char* value = NULL;
    size_t vlen = 0;
    ret = sstable_lookup(sst, "banana", 6, &value, &vlen, NULL);
    printf("sstable_lookup returned: %d\n", ret);
    if (ret == 0) {
        printf("Value: %.*s, len: %zu\n", (int)vlen, value, vlen);
        kv_free(value);
    }
    fflush(stdout);
    
    sstable_close(sst);
    remove("./test_min.sst");
    
    printf("ALL DONE\n");
    fflush(stdout);
    return 0;
}