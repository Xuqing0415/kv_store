#include "skiplist.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int main() {
    printf("Step 1: Start\n");
    fflush(stdout);
    
    printf("Step 2: Calling srand\n");
    fflush(stdout);
    srand((unsigned int)time(NULL));
    
    printf("Step 3: Calling skiplist_new\n");
    fflush(stdout);
    skiplist_t* sl = skiplist_new();
    printf("Step 4: skiplist_new done, sl=%p\n", sl);
    fflush(stdout);
    
    if (!sl) {
        printf("Error: skiplist_new returned NULL\n");
        return 1;
    }
    
    printf("Step 5: sl->header=%p, sl->level=%d\n", sl->header, sl->level);
    fflush(stdout);
    
    printf("Step 6: Testing sl->header->forward[0]=%p\n", sl->header->forward[0]);
    fflush(stdout);
    
    printf("Step 7: Testing rand()=%d\n", rand());
    fflush(stdout);
    
    printf("Step 8: Calling skiplist_insert\n");
    fflush(stdout);
    int ret = skiplist_insert(sl, "key", 3, "value", 5);
    printf("Step 9: skiplist_insert done, ret=%d\n", ret);
    fflush(stdout);
    
    skiplist_free(sl);
    printf("Step 9: skiplist_free done\n");
    fflush(stdout);
    
    printf("All steps passed!\n");
    fflush(stdout);
    
    return 0;
}