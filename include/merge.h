#ifndef MERGE_H
#define MERGE_H

#include <stddef.h>
#include <stdint.h>

#define MAX_LEVELS 7
#define L0_FILE_LIMIT 4
#define L1_SIZE_LIMIT (10 * 1024 * 1024)

typedef struct merge_context {
    char* dir_path;
    void* manifest;
    void* cache;
    int stop;
} merge_context_t;

void merge_scheduler_start(merge_context_t* ctx);
void merge_scheduler_stop(merge_context_t* ctx);
int merge_should_trigger(void* manifest);
int merge_execute(merge_context_t* ctx, int level);

#endif