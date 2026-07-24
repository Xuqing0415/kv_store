#include "merge.h"
#include "manifest.h"
#include "mem.h"
#include "sstable.h"
#include "skiplist.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#include <process.h>
#define THREAD_TYPE unsigned(__stdcall*)(void*)
#define THREAD_RET unsigned
#define THREAD_CREATE(h, a, f, d) ((*(h) = (HANDLE)_beginthreadex(NULL, 0, (f), (d), 0, NULL)) != 0)
#define THREAD_JOIN(h) WaitForSingleObject((h), INFINITE)
#define THREAD_CLOSE(h) CloseHandle((h))
#define SLEEP_MS(ms) Sleep((ms))
#else
#include <pthread.h>
#include <unistd.h>
#define THREAD_TYPE void*(*)(void*)
#define THREAD_RET void*
#define THREAD_CREATE(h, a, f, d) (pthread_create((h), (a), (f), (d)) == 0)
#define THREAD_JOIN(h) pthread_join((h), NULL)
#define THREAD_CLOSE(h) ((void)(h))
#define SLEEP_MS(ms) usleep((ms) * 1000)
#endif

static THREAD_RET merge_worker(void* arg) {
    merge_context_t* ctx = (merge_context_t*)arg;
    
    while (1) {
        #ifdef _WIN32
        LONG stop_flag = InterlockedCompareExchange(&ctx->stop, 0, 0);
        #else
        volatile int stop_flag = ctx->stop;
        #endif
        
        if (stop_flag) {
            break;
        }
        
        if (merge_should_trigger(ctx->manifest)) {
            for (int level = 0; level < MAX_LEVELS - 1; level++) {
                merge_execute(ctx, level);
            }
        }
        SLEEP_MS(1000);
    }
    
    return (THREAD_RET)0;
}

void merge_scheduler_start(merge_context_t* ctx) {
    if (!ctx) return;
    
#ifdef _WIN32
    HANDLE thread;
#else
    pthread_t thread;
#endif
    
    THREAD_CREATE(&thread, NULL, (THREAD_TYPE)merge_worker, ctx);
}

void merge_scheduler_stop(merge_context_t* ctx) {
    if (!ctx) return;
    
    ctx->stop = 1;
}

int merge_should_trigger(void* manifest) {
    if (!manifest) return 0;
    
    manifest_t* m = (manifest_t*)manifest;
    
    manifest_file_t** files = NULL;
    size_t count = 0;
    
    if (manifest_list_files(m, 0, &files, &count) == 0) {
        if (count >= L0_FILE_LIMIT) {
            kv_free(files);
            return 1;
        }
        kv_free(files);
    }
    
    for (int level = 1; level < MAX_LEVELS; level++) {
        if (manifest_list_files(m, level, &files, &count) == 0) {
            size_t total_size = 0;
            for (size_t i = 0; i < count; i++) {
                total_size += files[i]->file_size;
            }
            
            size_t limit = L1_SIZE_LIMIT;
            for (int i = 1; i < level; i++) {
                limit *= 10;
            }
            
            if (total_size >= limit) {
                kv_free(files);
                return 1;
            }
            kv_free(files);
        }
    }
    
    return 0;
}

static int merge_files(merge_context_t* ctx, manifest_file_t** l0_files, size_t l0_count, 
                       manifest_file_t** l1_files, size_t l1_count, int target_level) {
    if (!ctx || l0_count == 0) return -1;
    
    skiplist_t* merged = skiplist_new();
    if (!merged) return -1;
    
    char path[512];
    
    for (size_t i = 0; i < l0_count; i++) {
        snprintf(path, sizeof(path), "%s/%llu.sst", ctx->dir_path, (unsigned long long)l0_files[i]->file_id);
        sstable_t* sst = sstable_open(path, l0_files[i]->file_id);
        if (!sst) continue;
        
        sstable_iter_t* iter = sstable_new_iterator(sst);
        if (!iter) {
            sstable_close(sst);
            continue;
        }
        
        char* key = NULL;
        size_t klen = 0;
        char* value = NULL;
        size_t vlen = 0;
        
        while (sstable_iter_next(iter, &key, &klen, &value, &vlen) == 0) {
            skiplist_insert(merged, key, klen, value, vlen);
            kv_free(key);
            kv_free(value);
        }
        
        sstable_iter_free(iter);
        sstable_close(sst);
    }
    
    for (size_t i = 0; i < l1_count; i++) {
        snprintf(path, sizeof(path), "%s/%llu.sst", ctx->dir_path, (unsigned long long)l1_files[i]->file_id);
        sstable_t* sst = sstable_open(path, l1_files[i]->file_id);
        if (!sst) continue;
        
        sstable_iter_t* iter = sstable_new_iterator(sst);
        if (!iter) {
            sstable_close(sst);
            continue;
        }
        
        char* key = NULL;
        size_t klen = 0;
        char* value = NULL;
        size_t vlen = 0;
        
        while (sstable_iter_next(iter, &key, &klen, &value, &vlen) == 0) {
            skiplist_insert(merged, key, klen, value, vlen);
            kv_free(key);
            kv_free(value);
        }
        
        sstable_iter_free(iter);
        sstable_close(sst);
    }
    
    manifest_t* m = (manifest_t*)ctx->manifest;
    uint64_t new_file_id = manifest_next_file_id(m);
    
    snprintf(path, sizeof(path), "%s/%llu.sst", ctx->dir_path, (unsigned long long)new_file_id);
    
    if (sstable_write(path, new_file_id, merged) != 0) {
        skiplist_free(merged);
        return -1;
    }
    
    skiplist_iter_t* iter = skiplist_new_iterator(merged);
    char* smallest_key = NULL;
    size_t sklen = 0;
    char* largest_key = NULL;
    size_t lklen = 0;
    char* value = NULL;
    size_t vlen = 0;
    
    if (skiplist_iter_next(iter, &smallest_key, &sklen, &value, &vlen) == 0) {
        kv_free(value);
        while (skiplist_iter_next(iter, &largest_key, &lklen, &value, &vlen) == 0) {
            kv_free(smallest_key);
            smallest_key = largest_key;
            sklen = lklen;
            kv_free(value);
        }
    }
    
    skiplist_iter_free(iter);
    
    FILE* f = fopen(path, "rb");
    size_t file_size = 0;
    if (f) {
        fseek(f, 0, SEEK_END);
        file_size = ftell(f);
        fclose(f);
    }
    
    manifest_add_file(m, new_file_id, target_level, smallest_key, sklen, largest_key, lklen, file_size);
    
    kv_free(smallest_key);
    kv_free(largest_key);
    skiplist_free(merged);
    
    for (size_t i = 0; i < l0_count; i++) {
        manifest_remove_file(m, l0_files[i]->file_id);
        snprintf(path, sizeof(path), "%s/%llu.sst", ctx->dir_path, (unsigned long long)l0_files[i]->file_id);
        remove(path);
    }
    
    for (size_t i = 0; i < l1_count; i++) {
        manifest_remove_file(m, l1_files[i]->file_id);
        snprintf(path, sizeof(path), "%s/%llu.sst", ctx->dir_path, (unsigned long long)l1_files[i]->file_id);
        remove(path);
    }
    
    manifest_sync(m);
    
    return 0;
}

int merge_execute(merge_context_t* ctx, int level) {
    if (!ctx) return -1;
    
    manifest_t* m = (manifest_t*)ctx->manifest;
    
    manifest_file_t** l0_files = NULL;
    size_t l0_count = 0;
    
    if (manifest_list_files(m, level, &l0_files, &l0_count) != 0 || l0_count == 0) {
        return -1;
    }
    
    if (level == 0 && l0_count < L0_FILE_LIMIT) {
        kv_free(l0_files);
        return 0;
    }
    
    manifest_file_t** l1_files = NULL;
    size_t l1_count = 0;
    
    if (level + 1 < MAX_LEVELS) {
        manifest_list_files(m, level + 1, &l1_files, &l1_count);
    }
    
    int ret = merge_files(ctx, l0_files, l0_count, l1_files, l1_count, level + 1);
    
    kv_free(l0_files);
    kv_free(l1_files);
    
    return ret;
}