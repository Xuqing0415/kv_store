#include "kv_store.h"
#include "bloom_filter.h"
#include "lru_cache.h"
#include "manifest.h"
#include "merge.h"
#include "mem.h"
#include "sstable.h"
#include "skiplist.h"
#include "wal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#include <process.h>
typedef SRWLOCK RWLOCK;
#define RWLOCK_INIT(p) InitializeSRWLock((p))
#define RWLOCK_RDLOCK(p) AcquireSRWLockShared((p))
#define RWLOCK_WRLOCK(p) AcquireSRWLockExclusive((p))
#define RWLOCK_UNLOCK(p) ReleaseSRWLockShared((p))
#define RWLOCK_UNLOCK_W(p) ReleaseSRWLockExclusive((p))
#define RWLOCK_DESTROY(p) ((void)(p))
#else
#include <pthread.h>
typedef pthread_rwlock_t RWLOCK;
#define RWLOCK_INIT(p) pthread_rwlock_init((p), NULL)
#define RWLOCK_RDLOCK(p) pthread_rwlock_rdlock((p))
#define RWLOCK_WRLOCK(p) pthread_rwlock_wrlock((p))
#define RWLOCK_UNLOCK(p) pthread_rwlock_unlock((p))
#define RWLOCK_UNLOCK_W(p) pthread_rwlock_unlock((p))
#define RWLOCK_DESTROY(p) pthread_rwlock_destroy((p))
#endif

typedef struct kv_store {
    char* dir_path;
    skiplist_t* memtable;
    skiplist_t* immutable_memtable;
    wal_t* wal;
    manifest_t* manifest;
    lru_cache_t* block_cache;
    merge_context_t merge_ctx;
    RWLOCK rwlock;
    #ifdef _WIN32
    CRITICAL_SECTION manifest_lock;
    #else
    pthread_mutex_t manifest_lock;
    #endif
    size_t memtable_size;
} kv_store_t;

typedef struct kv_iter {
    kv_store_t* db;
    /* 合并后的统一跳表 + 迭代器：在 kv_scan 中一次性构建，保证排序和去重 */
    skiplist_t* merged;
    skiplist_iter_t* merged_iter;
    char* start_key;
    size_t start_len;
    char* end_key;
    size_t end_len;
} kv_iter_t;

static int kv_compare_keys(const char* a, size_t a_len, const char* b, size_t b_len) {
    size_t min_len = a_len < b_len ? a_len : b_len;
    int cmp = memcmp(a, b, min_len);
    if (cmp != 0) return cmp;
    if (a_len < b_len) return -1;
    if (a_len > b_len) return 1;
    return 0;
}

static int wal_replay_callback(wal_record_t* record, void* arg) {
    kv_store_t* db = (kv_store_t*)arg;
    
    if (record->type == WAL_PUT) {
        skiplist_insert(db->memtable, record->key, record->key_len, record->value, record->value_len);
    } else if (record->type == WAL_DELETE) {
        /* 使用 tombstone 插入来标记删除 */
        skiplist_insert(db->memtable, record->key, record->key_len, NULL, 0);
    }
    
    return 0;
}

static int kv_flush_memtable(kv_store_t* db) {
    if (!db || !db->immutable_memtable) return -1;
    
    size_t count = skiplist_count(db->immutable_memtable);
    printf("[FLUSH] Flushing MemTable with %zu entries to SSTable...\n", count);
    
    char path[512];
    uint64_t file_id = manifest_next_file_id(db->manifest);
    
    snprintf(path, sizeof(path), "%s/%llu.sst", db->dir_path, (unsigned long long)file_id);
    
    if (sstable_write(path, file_id, db->immutable_memtable) != 0) {
        printf("[FLUSH] ERROR: Failed to write SSTable %s\n", path);
        return -1;
    }
    printf("[FLUSH] SSTable written: %s\n", path);
    
    skiplist_iter_t* iter = skiplist_new_iterator(db->immutable_memtable);
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
    
    manifest_add_file(db->manifest, file_id, 0, smallest_key, sklen, largest_key, lklen, file_size);
    manifest_sync(db->manifest);
    
    kv_free(smallest_key);
    kv_free(largest_key);
    skiplist_free(db->immutable_memtable);
    db->immutable_memtable = NULL;
    
    if (db->wal) {
        wal_close(db->wal);
    }
    
    char wal_path[512];
    snprintf(wal_path, sizeof(wal_path), "%s/wal.log", db->dir_path);
    db->wal = wal_open(wal_path);
    
    printf("[FLUSH] MemTable flushed successfully, new WAL opened\n");
    return 0;
}

static int kv_switch_memtable(kv_store_t* db) {
    if (!db) return -1;
    
    printf("[SWITCH] MemTable size %zu >= limit %d, switching...\n", 
           db->memtable_size, MEMTABLE_SIZE_LIMIT);
    
    RWLOCK_WRLOCK(&db->rwlock);
    
    db->immutable_memtable = db->memtable;
    db->memtable = skiplist_new();
    db->memtable_size = 0;
    
    RWLOCK_UNLOCK_W(&db->rwlock);
    
    kv_flush_memtable(db);
    
    return 0;
}

kv_store_t* kv_open(const char* dir_path) {
    if (!dir_path) return NULL;
    
#ifdef _WIN32
    CreateDirectoryA(dir_path, NULL);
#else
    mkdir(dir_path, 0755);
#endif
    
    kv_store_t* db = kv_malloc(sizeof(kv_store_t));
    if (!db) return NULL;
    
    db->dir_path = kv_strdup(dir_path);
    if (!db->dir_path) {
        kv_free(db);
        return NULL;
    }
    
    db->memtable = skiplist_new();
    if (!db->memtable) {
        kv_free(db->dir_path);
        kv_free(db);
        return NULL;
    }
    
    db->immutable_memtable = NULL;
    db->memtable_size = 0;
    
    char wal_path[512];
    snprintf(wal_path, sizeof(wal_path), "%s/wal.log", dir_path);
    db->wal = wal_open(wal_path);
    
    if (db->wal) {
        wal_replay(db->wal, wal_replay_callback, db);
    }
    
    db->manifest = manifest_open(dir_path);
    if (!db->manifest) {
        skiplist_free(db->memtable);
        wal_close(db->wal);
        kv_free(db->dir_path);
        kv_free(db);
        return NULL;
    }
    
    db->block_cache = lru_cache_new(1024);
    
    db->merge_ctx.dir_path = db->dir_path;
    db->merge_ctx.manifest = db->manifest;
    db->merge_ctx.cache = db->block_cache;
    db->merge_ctx.manifest_lock = &db->manifest_lock;
    db->merge_ctx.stop = 0;
    db->merge_ctx.thread_started = 0;
    
    RWLOCK_INIT(&db->rwlock);
    
    #ifdef _WIN32
    InitializeCriticalSection(&db->manifest_lock);
    #else
    pthread_mutex_init(&db->manifest_lock, NULL);
    #endif
    
    merge_scheduler_start(&db->merge_ctx);
    
    return db;
}

void kv_close(kv_store_t* db) {
    if (!db) return;
    
    /* 先停止并等待 merge 后台线程退出，防止 use-after-free */
    merge_scheduler_join(&db->merge_ctx);
    
    if (db->immutable_memtable) {
        kv_flush_memtable(db);
    }
    
    if (db->memtable && skiplist_count(db->memtable) > 0) {
        db->immutable_memtable = db->memtable;
        kv_flush_memtable(db);
        /* kv_flush_memtable 已释放 immutable_memtable，避免 double-free */
        db->memtable = NULL;
    }
    
    if (db->memtable) {
        skiplist_free(db->memtable);
    }
    
    if (db->wal) {
        wal_close(db->wal);
    }
    
    manifest_close(db->manifest);
    lru_cache_free(db->block_cache);
    
    RWLOCK_DESTROY(&db->rwlock);
    
    #ifdef _WIN32
    DeleteCriticalSection(&db->manifest_lock);
    #else
    pthread_mutex_destroy(&db->manifest_lock);
    #endif
    
    kv_free(db->dir_path);
    kv_free(db);
}

int kv_put(kv_store_t* db, const char* key, size_t klen, const char* val, size_t vlen) {
    if (!db || !key || klen == 0 || !val) return -1;
    
    RWLOCK_WRLOCK(&db->rwlock);
    
    if (db->wal) {
        wal_write(db->wal, WAL_PUT, key, klen, val, vlen);
    }
    
    skiplist_insert(db->memtable, key, klen, val, vlen);
    db->memtable_size += klen + vlen;
    
    if (db->memtable_size >= MEMTABLE_SIZE_LIMIT) {
        RWLOCK_UNLOCK_W(&db->rwlock);
        kv_switch_memtable(db);
        return 0;
    }
    
    RWLOCK_UNLOCK_W(&db->rwlock);
    
    return 0;
}

int kv_get(kv_store_t* db, const char* key, size_t klen, char** out_val, size_t* out_vlen) {
    if (!db || !key || klen == 0 || !out_val || !out_vlen) return -1;
    
    RWLOCK_RDLOCK(&db->rwlock);
    
    char* value = NULL;
    size_t vlen = 0;
    
    int ret;
    
    ret = skiplist_lookup(db->memtable, key, klen, &value, &vlen);
    if (ret == 0) {
        *out_val = value;
        *out_vlen = vlen;
        RWLOCK_UNLOCK(&db->rwlock);
        return 0;
    }
    if (ret == -2) {
        /* tombstone found in MemTable, key is deleted */
        RWLOCK_UNLOCK(&db->rwlock);
        return -1;
    }
    
    if (db->immutable_memtable) {
        ret = skiplist_lookup(db->immutable_memtable, key, klen, &value, &vlen);
        if (ret == 0) {
            *out_val = value;
            *out_vlen = vlen;
            RWLOCK_UNLOCK(&db->rwlock);
            return 0;
        }
        if (ret == -2) {
            /* tombstone found in Immutable MemTable */
            RWLOCK_UNLOCK(&db->rwlock);
            return -1;
        }
    }
    
    (void)db->memtable;
    (void)db->immutable_memtable;
    
    #ifdef _WIN32
    EnterCriticalSection(&db->manifest_lock);
    #else
    pthread_mutex_lock(&db->manifest_lock);
    #endif
    
    manifest_file_t** files = NULL;
    size_t count = 0;
    
    if (manifest_list_files(db->manifest, -1, &files, &count) != 0) {
        #ifdef _WIN32
        LeaveCriticalSection(&db->manifest_lock);
        #else
        pthread_mutex_unlock(&db->manifest_lock);
        #endif
        RWLOCK_UNLOCK(&db->rwlock);
        return -1;
    }
    
    manifest_file_t** files_copy = kv_malloc(count * sizeof(manifest_file_t*));
    for (size_t i = 0; i < count; i++) {
        files_copy[i] = kv_malloc(sizeof(manifest_file_t));
        memcpy(files_copy[i], files[i], sizeof(manifest_file_t));
    }
    
    #ifdef _WIN32
    LeaveCriticalSection(&db->manifest_lock);
    #else
    pthread_mutex_unlock(&db->manifest_lock);
    #endif
    
    RWLOCK_UNLOCK(&db->rwlock);
    
    char path[512];
    
    for (int level = 0; level < MAX_LEVELS; level++) {
        for (size_t i = 0; i < count; i++) {
            if (files_copy[i]->level != level) continue;
            
            snprintf(path, sizeof(path), "%s/%llu.sst", db->dir_path, (unsigned long long)files_copy[i]->file_id);
            sstable_t* sst = sstable_open(path, files_copy[i]->file_id);
            if (!sst) continue;
            
            int sst_ret = sstable_lookup(sst, key, klen, &value, &vlen, db->block_cache);
            if (sst_ret == 0) {
                *out_val = value;
                *out_vlen = vlen;
                sstable_close(sst);
                for (size_t j = 0; j < count; j++) {
                    kv_free(files_copy[j]);
                }
                kv_free(files_copy);
                kv_free(files);
                return 0;
            }
            if (sst_ret == -2) {
                /* tombstone found in SSTable, key is deleted */
                sstable_close(sst);
                for (size_t j = 0; j < count; j++) {
                    kv_free(files_copy[j]);
                }
                kv_free(files_copy);
                kv_free(files);
                return -1;
            }
            
            sstable_close(sst);
        }
    }
    
    for (size_t i = 0; i < count; i++) {
        kv_free(files_copy[i]);
    }
    kv_free(files_copy);
    kv_free(files);
    
    return -1;
}

int kv_delete(kv_store_t* db, const char* key, size_t klen) {
    if (!db || !key || klen == 0) return -1;
    
    RWLOCK_WRLOCK(&db->rwlock);
    
    if (db->wal) {
        wal_write(db->wal, WAL_DELETE, key, klen, NULL, 0);
    }
    
    /* 使用 tombstone 插入（vlen=0）替代 delete，确保 tombstone 能持久化到 SSTable */
    skiplist_insert(db->memtable, key, klen, NULL, 0);
    db->memtable_size += klen;
    
    if (db->memtable_size >= MEMTABLE_SIZE_LIMIT) {
        RWLOCK_UNLOCK_W(&db->rwlock);
        kv_switch_memtable(db);
        return 0;
    }
    
    RWLOCK_UNLOCK_W(&db->rwlock);
    
    return 0;
}

/* 辅助函数：将跳表数据合并到目标跳表（使用范围过滤） */
/* 注意：插入所有条目（包括 tombstone），去重和过滤在 kv_iter_next 中处理 */
static void merge_skiplist_into(skiplist_t* dst, skiplist_t* src,
                                 const char* start_key, size_t start_len,
                                 const char* end_key, size_t end_len) {
    if (!dst || !src) return;
    
    skiplist_iter_t* it = skiplist_new_iterator(src);
    if (!it) return;
    
    char* k = NULL;
    size_t kl = 0;
    char* v = NULL;
    size_t vl = 0;
    
    while (skiplist_iter_next(it, &k, &kl, &v, &vl) == 0) {
        int include = 1;
        if (start_key && start_len > 0) {
            if (kv_compare_keys(k, kl, start_key, start_len) < 0) include = 0;
        }
        if (end_key && end_len > 0) {
            if (kv_compare_keys(k, kl, end_key, end_len) > 0) include = 0;
        }
        if (include) {
            /* 插入所有条目（包括 tombstone vl==0），skiplist_insert 覆盖实现去重 */
            skiplist_insert(dst, k, kl, v, vl);
        }
        kv_free(k);
        kv_free(v);
    }
    
    skiplist_iter_free(it);
}

/* 辅助函数：将 SSTable 数据合并到目标跳表（使用范围过滤） */
/* 注意：插入所有条目（包括 tombstone），去重和过滤在 kv_iter_next 中处理 */
static void merge_sstable_into(skiplist_t* dst, sstable_t* sst,
                                const char* start_key, size_t start_len,
                                const char* end_key, size_t end_len) {
    if (!dst || !sst) return;
    
    sstable_iter_t* it = sstable_new_iterator(sst);
    if (!it) return;
    
    char* k = NULL;
    size_t kl = 0;
    char* v = NULL;
    size_t vl = 0;
    
    while (sstable_iter_next(it, &k, &kl, &v, &vl) == 0) {
        int include = 1;
        if (start_key && start_len > 0) {
            if (kv_compare_keys(k, kl, start_key, start_len) < 0) include = 0;
        }
        if (end_key && end_len > 0) {
            if (kv_compare_keys(k, kl, end_key, end_len) > 0) include = 0;
        }
        if (include) {
            /* 插入所有条目（包括 tombstone vl==0），skiplist_insert 覆盖实现去重 */
            skiplist_insert(dst, k, kl, v, vl);
        }
        kv_free(k);
        kv_free(v);
    }
    
    sstable_iter_free(it);
}

kv_iter_t* kv_scan(kv_store_t* db, const char* start, size_t slen, const char* end, size_t elen) {
    if (!db) return NULL;
    
    kv_iter_t* iter = kv_malloc(sizeof(kv_iter_t));
    if (!iter) return NULL;
    
    iter->db = db;
    iter->merged = NULL;
    iter->merged_iter = NULL;
    
    if (start) {
        iter->start_key = kv_malloc(slen);
        memcpy(iter->start_key, start, slen);
        iter->start_len = slen;
    } else {
        iter->start_key = NULL;
        iter->start_len = 0;
    }
    
    if (end) {
        iter->end_key = kv_malloc(elen);
        memcpy(iter->end_key, end, elen);
        iter->end_len = elen;
    } else {
        iter->end_key = NULL;
        iter->end_len = 0;
    }
    
    /* 创建合并跳表：跳表的 insert 会自动覆盖旧值（去重），且天然有序 */
    /* 合并顺序：旧数据 → 新数据，后插入的覆盖先插入的，确保最新数据优先 */
    /* SSTable 高层级（最旧）→ 低层级 → Immutable MemTable → Active MemTable（最新） */
    iter->merged = skiplist_new();
    if (!iter->merged) {
        kv_iter_free(iter);
        return NULL;
    }
    
    RWLOCK_RDLOCK(&db->rwlock);
    
    /* 1. 先合并 SSTable：从高层级到低层级（旧→新），后插入的低层级会覆盖高层级 */
    #ifdef _WIN32
    EnterCriticalSection(&db->manifest_lock);
    #else
    pthread_mutex_lock(&db->manifest_lock);
    #endif
    
    manifest_file_t** files = NULL;
    size_t count = 0;
    
    if (manifest_list_files(db->manifest, -1, &files, &count) == 0 && count > 0) {
        for (int level = MAX_LEVELS - 1; level >= 0; level--) {
            char path[512];
            for (size_t i = 0; i < count; i++) {
                if (files[i]->level != level) continue;
                snprintf(path, sizeof(path), "%s/%llu.sst", db->dir_path, (unsigned long long)files[i]->file_id);
                sstable_t* sst = sstable_open(path, files[i]->file_id);
                if (sst) {
                    merge_sstable_into(iter->merged, sst, start, slen, end, elen);
                    sstable_close(sst);
                }
            }
        }
        kv_free(files);
    }
    
    #ifdef _WIN32
    LeaveCriticalSection(&db->manifest_lock);
    #else
    pthread_mutex_unlock(&db->manifest_lock);
    #endif
    
    /* 2. 合并 Immutable MemTable（比 SSTable 新） */
    if (db->immutable_memtable) {
        merge_skiplist_into(iter->merged, db->immutable_memtable, start, slen, end, elen);
    }
    
    /* 3. 合并活跃 MemTable（最新，最后合并，覆盖所有旧数据） */
    merge_skiplist_into(iter->merged, db->memtable, start, slen, end, elen);
    
    RWLOCK_UNLOCK(&db->rwlock);
    
    /* 创建统一迭代器 */
    iter->merged_iter = skiplist_new_iterator(iter->merged);
    
    return iter;
}

int kv_iter_next(kv_iter_t* iter, char** key, size_t* klen, char** val, size_t* vlen) {
    if (!iter || !key || !klen || !val || !vlen) return -1;
    if (!iter->merged_iter) return -1;
    
    /* 跳过 tombstone（vlen==0 的条目），它们仅用于去重 */
    while (1) {
        int ret = skiplist_iter_next(iter->merged_iter, key, klen, val, vlen);
        if (ret != 0) return ret;
        if (*vlen > 0) return 0;
        /* tombstone：释放资源并继续 */
        kv_free(*key);
        kv_free(*val);
    }
}

void kv_iter_free(kv_iter_t* iter) {
    if (!iter) return;
    
    if (iter->merged_iter) {
        skiplist_iter_free(iter->merged_iter);
    }
    
    if (iter->merged) {
        skiplist_free(iter->merged);
    }
    
    kv_free(iter->start_key);
    kv_free(iter->end_key);
    kv_free(iter);
}