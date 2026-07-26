#include "kv_store.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#endif

int main() {
    /* 禁用缓冲，确保所有输出立即可见 */
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);
    
    printf("Step 1: Starting...\n");
    
    printf("Step 2: kv_open...\n");
    kv_store_t* db = kv_open("./test_minimal2");
    printf("Step 3: kv_open returned %p\n", (void*)db);
    
    if (db) {
        printf("Step 4: kv_put...\n");
        kv_put(db, "hello", 5, "world", 5);
        printf("Step 5: kv_put done\n");
        
        printf("Step 6: kv_close...\n");
        kv_close(db);
        printf("Step 7: kv_close done\n");
    }
    
    printf("Step 8: All done!\n");
    return 0;
}