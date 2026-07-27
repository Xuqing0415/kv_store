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
#define THREAD_CREATE(h, a, f, d) ((void)(*(h) = (HANDLE)_beginthreadex(NULL, 0, (f), (d), 0, NULL)))
#define THREAD_JOIN(h) WaitForSingleObject((h), INFINITE)
#define THREAD_CLOSE(h) CloseHandle((h))
#define SLEEP_MS(ms) Sleep((ms))
#define MANIFEST_LOCK(ctx) EnterCriticalSection((CRITICAL_SECTION*)(ctx)->manifest_lock)
#define MANIFEST_UNLOCK(ctx) LeaveCriticalSection((CRITICAL_SECTION*)(ctx)->manifest_lock)
#else
#include <pthread.h>
#include <unistd.h>
#define THREAD_TYPE void*(*)(void*)
#define THREAD_RET void*
#define THREAD_CREATE(h, a, f, d) (pthread_create((h), (a), (f), (d)) == 0)
#define THREAD_JOIN(h) pthread_join((h), NULL)
#define THREAD_CLOSE(h) ((void)(h))
#define SLEEP_MS(ms) usleep((ms) * 1000)
#define MANIFEST_LOCK(ctx) pthread_mutex_lock((pthread_mutex_t*)(ctx)->manifest_lock)
#define MANIFEST_UNLOCK(ctx) pthread_mutex_unlock((pthread_mutex_t*)(ctx)->manifest_lock)
#endif

static THREAD_RET merge_worker(void* arg) {
    merge_context_t* ctx = (merge_context_t*)arg;
    
    printf("[MERGE] Background worker thread started\n");
    int loop_count = 0;
    
    while (1) {
        #ifdef _WIN32
        LONG stop_flag = InterlockedCompareExchange(&ctx->stop, 0, 0);
        #else
        volatile int stop_flag = ctx->stop;
        #endif
        
        if (stop_flag) {
            printf("[MERGE] Worker received stop signal, exiting\n");
            break;
        }
        
        loop_count++;
        if (loop_count % 5 == 0) {
            printf("[MERGE] Worker alive, loop #%d\n", loop_count);
        }
        
        if (merge_should_trigger(ctx)) {
            printf("[MERGE] Compaction triggered, running levels 0..%d\n", MAX_LEVELS - 2);
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
    
    ctx->stop = 0;
    ctx->thread_started = 1;
    
    THREAD_CREATE(&ctx->thread, NULL, (THREAD_TYPE)merge_worker, ctx);
}

void merge_scheduler_stop(merge_context_t* ctx) {
    if (!ctx) return;
    
    ctx->stop = 1;
}

void merge_scheduler_join(merge_context_t* ctx) {
    if (!ctx || !ctx->thread_started) return;
    
    ctx->stop = 1;
    THREAD_JOIN(ctx->thread);
    THREAD_CLOSE(ctx->thread);
    ctx->thread_started = 0;
}

int merge_should_trigger(merge_context_t* ctx) {
    if (!ctx || !ctx->manifest) return 0;
    
    manifest_t* m = (manifest_t*)ctx->manifest;
    
    MANIFEST_LOCK(ctx);
    
    manifest_file_t** files = NULL;
    size_t count = 0;
    
    if (manifest_list_files(m, 0, &files, &count) == 0) {
        printf("[MERGE] Level 0 has %zu files (limit=%d)\n", count, L0_FILE_LIMIT);
        if (count >= L0_FILE_LIMIT) {
            kv_free(files);
            MANIFEST_UNLOCK(ctx);
            printf("[MERGE] L0 file count %zu >= limit %d, triggering compaction\n", count, L0_FILE_LIMIT);
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
                MANIFEST_UNLOCK(ctx);
                return 1;
            }
            kv_free(files);
        }
    }
    
    MANIFEST_UNLOCK(ctx);
    return 0;
}

static int merge_files(merge_context_t* ctx, manifest_file_t** src_files, size_t src_count, 
                       manifest_file_t** dst_files, size_t dst_count, int src_level, int target_level) {
    if (!ctx || src_count == 0) return -1;
    
    printf("[MERGE] Merging %zu L%d files + %zu L%d files into level %d\n", 
           src_count, src_level, dst_count, target_level, target_level);
    
    skiplist_t* merged = skiplist_new();
    if (!merged) return -1;
    
    char path[512];
    
    for (size_t i = 0; i < src_count; i++) {
        snprintf(path, sizeof(path), "%s/%llu.sst", ctx->dir_path, (unsigned long long)src_files[i]->file_id);
        sstable_t* sst = sstable_open(path, src_files[i]->file_id);
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
    
    for (size_t i = 0; i < dst_count; i++) {
        snprintf(path, sizeof(path), "%s/%llu.sst", ctx->dir_path, (unsigned long long)dst_files[i]->file_id);
        sstable_t* sst = sstable_open(path, dst_files[i]->file_id);
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
        printf("[MERGE] ERROR: Failed to write merged SSTable %s\n", path);
        skiplist_free(merged);
        return -1;
    }
    printf("[MERGE] Merged SSTable written: %s\n", path);
    
    skiplist_iter_t* iter = skiplist_new_iterator(merged);
    char* smallest_key = NULL;
    size_t sklen = 0;
    char* largest_key = NULL;
    size_t lklen = 0;
    char* value = NULL;
    size_t vlen = 0;
    
    if (skiplist_iter_next(iter, &smallest_key, &sklen, &value, &vlen) == 0) {
        kv_free(value);
        /* 复制 smallest_key 作为 largest_key 的初始值（处理单条目情况） */
        largest_key = kv_malloc(sklen);
        if (largest_key) {
            memcpy(largest_key, smallest_key, sklen);
            lklen = sklen;
        }
        char* next_key = NULL;
        size_t next_klen = 0;
        while (skiplist_iter_next(iter, &next_key, &next_klen, &value, &vlen) == 0) {
            kv_free(value);
            kv_free(largest_key);
            largest_key = next_key;
            lklen = next_klen;
            next_key = NULL;
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
    
    for (size_t i = 0; i < src_count; i++) {
        manifest_remove_file(m, src_files[i]->file_id);
        snprintf(path, sizeof(path), "%s/%llu.sst", ctx->dir_path, (unsigned long long)src_files[i]->file_id);
        remove(path);
    }
    
    for (size_t i = 0; i < dst_count; i++) {
        manifest_remove_file(m, dst_files[i]->file_id);
        snprintf(path, sizeof(path), "%s/%llu.sst", ctx->dir_path, (unsigned long long)dst_files[i]->file_id);
        remove(path);
    }
    
    manifest_sync(m);
    
    printf("[MERGE] Compaction complete: removed %zu old files, created new SSTable\n", 
           src_count + dst_count);
    return 0;
}

int merge_execute(merge_context_t* ctx, int level) {
    if (!ctx) return -1;
    
    manifest_t* m = (manifest_t*)ctx->manifest;
    
    MANIFEST_LOCK(ctx);
    
    manifest_file_t** l0_files = NULL;
    size_t l0_count = 0;
    
    if (manifest_list_files(m, level, &l0_files, &l0_count) != 0 || l0_count == 0) {
        MANIFEST_UNLOCK(ctx);
        return -1;
    }
    
    if (level == 0 && l0_count < L0_FILE_LIMIT) {
        kv_free(l0_files);
        MANIFEST_UNLOCK(ctx);
        return 0;
    }
    
    manifest_file_t** l1_files = NULL;
    size_t l1_count = 0;
    
    if (level + 1 < MAX_LEVELS) {
        manifest_list_files(m, level + 1, &l1_files, &l1_count);
    }
    
    int ret = merge_files(ctx, l0_files, l0_count, l1_files, l1_count, level, level + 1);
    
    kv_free(l0_files);
    kv_free(l1_files);
    
    MANIFEST_UNLOCK(ctx);
    return ret;
}