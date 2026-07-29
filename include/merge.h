#ifndef MERGE_H
#define MERGE_H

#include <stddef.h>
#include <stdint.h>
#include "compression.h"

#ifdef _WIN32
#include <windows.h>
#endif

#define MAX_LEVELS 7
#define L0_FILE_LIMIT 2  /* 降低到2，便于快速触发合并测试 */
#define L1_SIZE_LIMIT (10 * 1024 * 1024)

typedef struct merge_context {
    char* dir_path;
    void* manifest;
    void* cache;
    void* manifest_lock;
    #ifdef _WIN32
    volatile LONG stop;
    HANDLE thread;
    #else
    volatile int stop;
    pthread_t thread;
    #endif
    int thread_started;
    compression_type_t compression_type;  /* 压缩算法类型 */
} merge_context_t;

void merge_scheduler_start(merge_context_t* ctx);
void merge_scheduler_stop(merge_context_t* ctx);
void merge_scheduler_join(merge_context_t* ctx);
int merge_should_trigger(merge_context_t* ctx);
int merge_execute(merge_context_t* ctx, int level);

#endif