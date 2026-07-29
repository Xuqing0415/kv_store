#include "sstable.h"
#include "mem.h"
#include <stdio.h>
#include <string.h>

int main() {
    const char* path = "./test_db/8.sst";
    uint64_t file_id = 8;
    
    sstable_t* sst = sstable_open(path, file_id);
    if (!sst) {
        printf("Failed to open %s\n", path);
        return 1;
    }
    
    printf("File size: %zu\n", sst->file_size);
    printf("Index offset: %llu, size: %zu\n", 
           (unsigned long long)sst->index_offset, sst->index_size);
    printf("Compression: %d\n", sst->compression_type);
    
    /* Test specific failing keys */
    const char* test_keys[] = {"key_1617", "key_1636", "key_1683", "key_1693"};
    for (int i = 0; i < 4; i++) {
        char* val = NULL;
        size_t vlen = 0;
        int ret = sstable_lookup(sst, test_keys[i], strlen(test_keys[i]), &val, &vlen, NULL);
        printf("Lookup '%s': ret=%d", test_keys[i], ret);
        if (ret == 0) {
            printf(" value='%.*s' len=%zu", (int)vlen, val, vlen);
            kv_free(val);
        }
        printf("\n");
    }
    
    /* Also test a known good key */
    const char* good_key = "key_0";
    char* val = NULL;
    size_t vlen = 0;
    int ret = sstable_lookup(sst, good_key, strlen(good_key), &val, &vlen, NULL);
    printf("Lookup '%s': ret=%d", good_key, ret);
    if (ret == 0) {
        printf(" value='%.*s'", (int)vlen, val);
        kv_free(val);
    }
    printf("\n");
    
    /* Scan all entries to verify total count */
    int scan_count = 0;
    sstable_iter_t* iter = sstable_new_iterator(sst);
    if (iter) {
        char* k = NULL; size_t kl = 0;
        char* v = NULL; size_t vl = 0;
        while (sstable_iter_next(iter, &k, &kl, &v, &vl) == 0) {
            scan_count++;
            if (scan_count <= 3 || scan_count >= 4998) {
                printf("  [%d] key='%.*s' vlen=%zu\n", scan_count, (int)kl, k, vl);
            }
            kv_free(k);
            kv_free(v);
        }
        sstable_iter_free(iter);
    }
    printf("Total entries scanned: %d\n", scan_count);
    
    sstable_close(sst);
    return 0;
}