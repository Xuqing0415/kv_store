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
/* 原子操作：递增 long long */
#define ATOMIC_INC64(var) InterlockedIncrement64(&(var))
#define ATOMIC_READ64(var) InterlockedExchangeAdd64(&(var), 0)
#else
#include <pthread.h>
#include <dirent.h>
#include <sys/stat.h>
typedef pthread_rwlock_t RWLOCK;
#define RWLOCK_INIT(p) pthread_rwlock_init((p), NULL)
#define RWLOCK_RDLOCK(p) pthread_rwlock_rdlock((p))
#define RWLOCK_WRLOCK(p) pthread_rwlock_wrlock((p))
#define RWLOCK_UNLOCK(p) pthread_rwlock_unlock((p))
#define RWLOCK_UNLOCK_W(p) pthread_rwlock_unlock((p))
#define RWLOCK_DESTROY(p) pthread_rwlock_destroy((p))
/* 原子操作：递增 long long */
#define ATOMIC_INC64(var) __atomic_fetch_add(&(var), 1, __ATOMIC_RELAXED)
#define ATOMIC_READ64(var) __atomic_load_n(&(var), __ATOMIC_RELAXED)
#endif

typedef struct kv_store {
    char* dir_path;
    skiplist_t* memtable;
    skiplist_t* immutable_memtable;
    wal_mgr_t* wal_mgr;
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
    compression_type_t compression_type;  /* 压缩算法：zstd / lz4 / none */

    /* metrics counters */
    long long puts_total;
    long long gets_total;
    long long get_misses_total;
    long long deletes_total;
    long long scans_total;
    long long compactions_total;

    /* Raft 模式：禁用 WAL，由 Raft 日志统一持久化 */
    int raft_mode;
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
    
    if (sstable_write(path, file_id, db->immutable_memtable, db->compression_type) != 0) {
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
    
    /* 归档旧 WAL（数据已安全写入 SSTable），创建新 WAL */
    if (db->wal_mgr) {
        wal_mgr_archive(db->wal_mgr);
    }
    
    printf("[FLUSH] MemTable flushed successfully, WAL archived & new WAL opened\n");
    return 0;
}

static int kv_switch_memtable(kv_store_t* db) {
    if (!db) return -1;
    
    RWLOCK_WRLOCK(&db->rwlock);
    
    /* 检查是否有其他线程已经触发了切换 */
    if (db->immutable_memtable) {
        /* 已有线程在刷盘，无需重复切换 */
        RWLOCK_UNLOCK_W(&db->rwlock);
        return 0;
    }
    
    /* 重新检查 memtable 大小（可能已被其他线程切换） */
    if (db->memtable_size < MEMTABLE_SIZE_LIMIT && skiplist_count(db->memtable) > 0) {
        RWLOCK_UNLOCK_W(&db->rwlock);
        return 0;
    }
    
    printf("[SWITCH] MemTable size %zu >= limit %d, switching...\n", 
           db->memtable_size, MEMTABLE_SIZE_LIMIT);
    
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
    db->compression_type = COMPRESSION_ZSTD;  /* 默认使用 zstd */
    
    /* 初始化指标计数器 */
    db->puts_total = 0;
    db->gets_total = 0;
    db->get_misses_total = 0;
    db->deletes_total = 0;
    db->scans_total = 0;
    db->compactions_total = 0;
    db->raft_mode = 0;
    
    db->wal_mgr = wal_mgr_open(dir_path);
    
    if (db->wal_mgr) {
        wal_mgr_replay(db->wal_mgr, wal_replay_callback, db);
    }
    
    db->manifest = manifest_open(dir_path);
    if (!db->manifest) {
        skiplist_free(db->memtable);
        wal_mgr_close(db->wal_mgr);
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
    db->merge_ctx.compression_type = db->compression_type;
    
    RWLOCK_INIT(&db->rwlock);
    
    #ifdef _WIN32
    InitializeCriticalSection(&db->manifest_lock);
    #else
    pthread_mutex_init(&db->manifest_lock, NULL);
    #endif
    
    merge_scheduler_start(&db->merge_ctx);
    
    return db;
}

/* Raft 模式打开：不创建 WAL，不重放 WAL。
 * 状态恢复由 Raft 日志重放完成（调用者负责）。
 * 注意：仍需要 MANIFEST 和 SSTable 文件来支持快照恢复。 */
kv_store_t* kv_open_raft(const char* dir_path) {
    if (!dir_path) return NULL;
    
#ifdef _WIN32
    CreateDirectoryA(dir_path, NULL);
#else
    mkdir(dir_path, 0755);
#endif
    
    kv_store_t* db = kv_malloc(sizeof(kv_store_t));
    if (!db) return NULL;
    
    memset(db, 0, sizeof(kv_store_t));
    
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
    db->compression_type = COMPRESSION_ZSTD;
    
    /* 初始化指标计数器 */
    db->puts_total = 0;
    db->gets_total = 0;
    db->get_misses_total = 0;
    db->deletes_total = 0;
    db->scans_total = 0;
    db->compactions_total = 0;
    
    /* Raft 模式：不创建 WAL */
    db->raft_mode = 1;
    db->wal_mgr = NULL;
    
    db->manifest = manifest_open(dir_path);
    if (!db->manifest) {
        skiplist_free(db->memtable);
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
    db->merge_ctx.compression_type = db->compression_type;
    
    RWLOCK_INIT(&db->rwlock);
    
    #ifdef _WIN32
    InitializeCriticalSection(&db->manifest_lock);
    #else
    pthread_mutex_init(&db->manifest_lock, NULL);
    #endif
    
    merge_scheduler_start(&db->merge_ctx);
    
    printf("[KV] Opened in Raft mode (no WAL) at %s\n", dir_path);
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
    
    if (db->wal_mgr) {
        wal_mgr_close(db->wal_mgr);
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
    
    /* Raft 模式下跳过 WAL 写入（由 Raft 日志统一持久化） */
    if (db->wal_mgr && !db->raft_mode) {
        wal_mgr_write(db->wal_mgr, WAL_PUT, key, klen, val, vlen);
    }
    
    skiplist_insert(db->memtable, key, klen, val, vlen);
    db->memtable_size += klen + vlen;
    
    ATOMIC_INC64(db->puts_total);
    
    if (db->memtable_size >= MEMTABLE_SIZE_LIMIT) {
        RWLOCK_UNLOCK_W(&db->rwlock);
        kv_switch_memtable(db);
        return 0;
    }
    
    RWLOCK_UNLOCK_W(&db->rwlock);
    
    return 0;
}

int kv_get(kv_store_t* db, const char* key, size_t klen, char** out_val, size_t* out_vlen) {
    if (!db || !key || klen == 0 || !out_val || !out_vlen) {
        ATOMIC_INC64(db->get_misses_total);
        return -1;
    }
    
    RWLOCK_RDLOCK(&db->rwlock);
    
    char* value = NULL;
    size_t vlen = 0;
    
    int ret;
    
    ret = skiplist_lookup(db->memtable, key, klen, &value, &vlen);
    if (ret == 0) {
        *out_val = value;
        *out_vlen = vlen;
        RWLOCK_UNLOCK(&db->rwlock);
        ATOMIC_INC64(db->gets_total);
        return 0;
    }
    if (ret == -2) {
        /* tombstone found in MemTable, key is deleted */
        RWLOCK_UNLOCK(&db->rwlock);
        ATOMIC_INC64(db->get_misses_total);
        return -1;
    }
    
    if (db->immutable_memtable) {
        ret = skiplist_lookup(db->immutable_memtable, key, klen, &value, &vlen);
        if (ret == 0) {
            *out_val = value;
            *out_vlen = vlen;
            RWLOCK_UNLOCK(&db->rwlock);
            ATOMIC_INC64(db->gets_total);
            return 0;
        }
        if (ret == -2) {
            /* tombstone found in Immutable MemTable */
            RWLOCK_UNLOCK(&db->rwlock);
            ATOMIC_INC64(db->get_misses_total);
            return -1;
        }
    }
    
    (void)db->memtable;
    (void)db->immutable_memtable;
    
    /* 释放 RDLOCK 再获取 manifest_lock，避免活锁：
     * 如果 reader 持有 RDLOCK 时等待 manifest_lock（被 merge 线程持有），
     * 所有 writer 都会被阻塞，导致系统卡死。 */
    RWLOCK_UNLOCK(&db->rwlock);
    
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
        ATOMIC_INC64(db->get_misses_total);
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
                ATOMIC_INC64(db->gets_total);
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
                ATOMIC_INC64(db->get_misses_total);
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
    
    ATOMIC_INC64(db->get_misses_total);
    return -1;
}

int kv_sync(kv_store_t* db) {
    if (!db) return -1;
    if (db->wal_mgr) {
        return wal_mgr_sync(db->wal_mgr);
    }
    return -1;
}

int kv_force_merge(kv_store_t* db) {
    if (!db) return -1;
    
    ATOMIC_INC64(db->compactions_total);
    
    printf("[MERGE] Force merge triggered by user, running levels 0..%d\n", MAX_LEVELS - 2);
    fflush(stdout);
    
    for (int level = 0; level < MAX_LEVELS - 1; level++) {
        merge_execute(&db->merge_ctx, level);
    }
    
    printf("[MERGE] Force merge complete\n");
    fflush(stdout);
    return 0;
}

void kv_metrics_snapshot(kv_store_t* db, kv_metrics_snapshot_t* out) {
    if (!db || !out) return;
    out->puts_total        = ATOMIC_READ64(db->puts_total);
    out->gets_total        = ATOMIC_READ64(db->gets_total);
    out->get_misses_total  = ATOMIC_READ64(db->get_misses_total);
    out->deletes_total     = ATOMIC_READ64(db->deletes_total);
    out->scans_total       = ATOMIC_READ64(db->scans_total);
    out->compactions_total = ATOMIC_READ64(db->compactions_total);
}

int kv_delete(kv_store_t* db, const char* key, size_t klen) {
    if (!db || !key || klen == 0) return -1;
    
    RWLOCK_WRLOCK(&db->rwlock);
    
    /* Raft 模式下跳过 WAL 写入（由 Raft 日志统一持久化） */
    if (db->wal_mgr && !db->raft_mode) {
        wal_mgr_write(db->wal_mgr, WAL_DELETE, key, klen, NULL, 0);
    }
    
    /* 使用 tombstone 插入（vlen=0）替代 delete，确保 tombstone 能持久化到 SSTable */
    skiplist_insert(db->memtable, key, klen, NULL, 0);
    db->memtable_size += klen;
    
    ATOMIC_INC64(db->deletes_total);
    
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
    
    ATOMIC_INC64(db->scans_total);
    
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

/* ================================================================
 * 快照实现
 *
 * 快照在创建时捕获当前 MANIFEST 中的 SSTable 文件列表，
 * 后续所有读取操作基于该快照的文件列表，不受后续写入/合并影响。
 *
 * 设计要点：
 *   - 快照是只读的，不包含 MemTable（仅 SSTable 层）
 *   - 创建快照前建议先 kv_sync() + kv_force_merge() 确保数据落盘
 *   - 如果快照引用的 SSTable 被合并删除，读取会返回 -1
 *   - 快照持有独立的 LRU 缓存，避免与主库竞争
 * ================================================================ */

typedef struct kv_snapshot {
    char* dir_path;
    manifest_file_t** files;
    size_t file_count;
    lru_cache_t* block_cache;
} kv_snapshot_t;

static int snap_compare_keys(const char* a, size_t a_len, const char* b, size_t b_len) {
    size_t min_len = a_len < b_len ? a_len : b_len;
    int cmp = memcmp(a, b, min_len);
    if (cmp != 0) return cmp;
    if (a_len < b_len) return -1;
    if (a_len > b_len) return 1;
    return 0;
}

kv_snapshot_t* kv_snapshot_create(kv_store_t* db) {
    if (!db) return NULL;

    kv_snapshot_t* snap = kv_malloc(sizeof(kv_snapshot_t));
    if (!snap) return NULL;

    snap->dir_path = kv_strdup(db->dir_path);
    if (!snap->dir_path) {
        kv_free(snap);
        return NULL;
    }

    snap->files = NULL;
    snap->file_count = 0;
    snap->block_cache = lru_cache_new(512);

    /* 在 manifest_lock 下复制文件列表 */
    #ifdef _WIN32
    EnterCriticalSection(&db->manifest_lock);
    #else
    pthread_mutex_lock(&db->manifest_lock);
    #endif

    manifest_file_t** raw_files = NULL;
    size_t raw_count = 0;
    if (manifest_list_files(db->manifest, -1, &raw_files, &raw_count) == 0 && raw_count > 0) {
        snap->files = kv_malloc(raw_count * sizeof(manifest_file_t*));
        if (snap->files) {
            for (size_t i = 0; i < raw_count; i++) {
                snap->files[i] = kv_malloc(sizeof(manifest_file_t));
                if (snap->files[i]) {
                    memcpy(snap->files[i], raw_files[i], sizeof(manifest_file_t));
                    /* 深拷贝 key 字段 */
                    snap->files[i]->smallest_key = kv_malloc(raw_files[i]->smallest_key_len);
                    snap->files[i]->largest_key = kv_malloc(raw_files[i]->largest_key_len);
                    if (snap->files[i]->smallest_key && snap->files[i]->largest_key) {
                        memcpy(snap->files[i]->smallest_key, raw_files[i]->smallest_key, raw_files[i]->smallest_key_len);
                        memcpy(snap->files[i]->largest_key, raw_files[i]->largest_key, raw_files[i]->largest_key_len);
                        snap->file_count++;
                    } else {
                        kv_free(snap->files[i]->smallest_key);
                        kv_free(snap->files[i]->largest_key);
                        kv_free(snap->files[i]);
                        snap->files[i] = NULL;
                    }
                }
            }
        }
        kv_free(raw_files);
    }

    #ifdef _WIN32
    LeaveCriticalSection(&db->manifest_lock);
    #else
    pthread_mutex_unlock(&db->manifest_lock);
    #endif

    printf("[SNAPSHOT] Created with %zu SSTable files\n", snap->file_count);
    return snap;
}

void kv_snapshot_free(kv_snapshot_t* snap) {
    if (!snap) return;

    for (size_t i = 0; i < snap->file_count; i++) {
        if (snap->files[i]) {
            kv_free(snap->files[i]->smallest_key);
            kv_free(snap->files[i]->largest_key);
            kv_free(snap->files[i]);
        }
    }
    kv_free(snap->files);
    lru_cache_free(snap->block_cache);
    kv_free(snap->dir_path);
    kv_free(snap);
}

int kv_snapshot_get(kv_snapshot_t* snap, const char* key, size_t klen, char** out_val, size_t* out_vlen) {
    if (!snap || !key || klen == 0 || !out_val || !out_vlen) return -1;

    char path[512];
    char* value = NULL;
    size_t vlen = 0;

    /* 按 level 从低到高查找（L0 最新，优先匹配） */
    for (int level = 0; level < MAX_LEVELS; level++) {
        for (size_t i = 0; i < snap->file_count; i++) {
            if (!snap->files[i] || snap->files[i]->level != level) continue;

            /* 范围检查：key 是否在该 SSTable 的 key 范围内 */
            if (snap->files[i]->smallest_key && snap->files[i]->largest_key) {
                if (snap_compare_keys(key, klen, snap->files[i]->smallest_key, snap->files[i]->smallest_key_len) < 0) continue;
                if (snap_compare_keys(key, klen, snap->files[i]->largest_key, snap->files[i]->largest_key_len) > 0) continue;
            }

            snprintf(path, sizeof(path), "%s/%llu.sst", snap->dir_path, (unsigned long long)snap->files[i]->file_id);
            sstable_t* sst = sstable_open(path, snap->files[i]->file_id);
            if (!sst) continue;

            int sst_ret = sstable_lookup(sst, key, klen, &value, &vlen, snap->block_cache);
            if (sst_ret == 0) {
                *out_val = value;
                *out_vlen = vlen;
                sstable_close(sst);
                return 0;
            }
            if (sst_ret == -2) {
                /* tombstone */
                sstable_close(sst);
                return -1;
            }
            sstable_close(sst);
        }
    }

    return -1;
}

kv_iter_t* kv_snapshot_scan(kv_snapshot_t* snap, const char* start, size_t slen, const char* end, size_t elen) {
    if (!snap) return NULL;

    kv_iter_t* iter = kv_malloc(sizeof(kv_iter_t));
    if (!iter) return NULL;

    iter->db = NULL;  /* 快照不关联 db */
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

    iter->merged = skiplist_new();
    if (!iter->merged) {
        kv_iter_free(iter);
        return NULL;
    }

    /* 合并 SSTable：从高层级到低层级（旧→新） */
    char path[512];
    for (int level = MAX_LEVELS - 1; level >= 0; level--) {
        for (size_t i = 0; i < snap->file_count; i++) {
            if (!snap->files[i] || snap->files[i]->level != level) continue;

            snprintf(path, sizeof(path), "%s/%llu.sst", snap->dir_path, (unsigned long long)snap->files[i]->file_id);
            sstable_t* sst = sstable_open(path, snap->files[i]->file_id);
            if (sst) {
                merge_sstable_into(iter->merged, sst, start, slen, end, elen);
                sstable_close(sst);
            }
        }
    }

    iter->merged_iter = skiplist_new_iterator(iter->merged);
    return iter;
}

/* ====================== 备份与恢复 ====================== */

/* 递归创建目录（跨平台） */
static int mkdir_recursive(const char* path) {
    char tmp[512];
    snprintf(tmp, sizeof(tmp), "%s", path);
    size_t len = strlen(tmp);

    /* 去掉末尾斜杠 */
    if (len > 0 && (tmp[len - 1] == '/' || tmp[len - 1] == '\\')) {
        tmp[len - 1] = '\0';
    }

    for (size_t i = 0; tmp[i]; i++) {
        if (tmp[i] == '/' || tmp[i] == '\\') {
            if (i == 0) continue;
            char saved = tmp[i];
            tmp[i] = '\0';
#ifdef _WIN32
            CreateDirectoryA(tmp, NULL);
#else
            mkdir(tmp, 0755);
#endif
            tmp[i] = saved;
        }
    }
#ifdef _WIN32
    CreateDirectoryA(tmp, NULL);
#else
    mkdir(tmp, 0755);
#endif
    return 0;
}

/* 复制文件：src → dst */
static int copy_file(const char* src, const char* dst) {
    FILE* fsrc = fopen(src, "rb");
    if (!fsrc) return -1;

    FILE* fdst = fopen(dst, "wb");
    if (!fdst) {
        fclose(fsrc);
        return -1;
    }

    char buf[65536];  /* 64KB buffer */
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), fsrc)) > 0) {
        if (fwrite(buf, 1, n, fdst) != n) {
            fclose(fsrc);
            fclose(fdst);
            return -1;
        }
    }

    fclose(fsrc);
    fclose(fdst);
    return 0;
}

/* ================================================================
 * Raft 模式：内部写入函数（无 WAL）
 * ================================================================ */

void kv_set_raft_mode(kv_store_t* db, int enabled) {
    if (!db) return;
    db->raft_mode = enabled;
    if (enabled && db->wal_mgr) {
        /* 关闭 WAL 管理器，不再写入独立 WAL */
        wal_mgr_close(db->wal_mgr);
        db->wal_mgr = NULL;
    }
    printf("[KV] Raft mode %s\n", enabled ? "enabled" : "disabled");
}

int kv_put_internal(kv_store_t* db, const char* key, size_t klen, const char* val, size_t vlen) {
    if (!db || !key || klen == 0 || !val) return -1;

    RWLOCK_WRLOCK(&db->rwlock);

    skiplist_insert(db->memtable, key, klen, val, vlen);
    db->memtable_size += klen + vlen;

    ATOMIC_INC64(db->puts_total);

    if (db->memtable_size >= MEMTABLE_SIZE_LIMIT) {
        RWLOCK_UNLOCK_W(&db->rwlock);
        kv_switch_memtable(db);
        return 0;
    }

    RWLOCK_UNLOCK_W(&db->rwlock);
    return 0;
}

int kv_delete_internal(kv_store_t* db, const char* key, size_t klen) {
    if (!db || !key || klen == 0) return -1;

    RWLOCK_WRLOCK(&db->rwlock);

    /* tombstone 插入 */
    skiplist_insert(db->memtable, key, klen, NULL, 0);
    db->memtable_size += klen;

    ATOMIC_INC64(db->deletes_total);

    if (db->memtable_size >= MEMTABLE_SIZE_LIMIT) {
        RWLOCK_UNLOCK_W(&db->rwlock);
        kv_switch_memtable(db);
        return 0;
    }

    RWLOCK_UNLOCK_W(&db->rwlock);
    return 0;
}

int kv_backup(kv_store_t* db, const char* backup_dir) {
    if (!db || !backup_dir) return -1;

    printf("[BACKUP] Starting backup to %s...\n", backup_dir);

    /* 1. 获取写锁，确保没有并发写入 */
    RWLOCK_WRLOCK(&db->rwlock);

    /* 2. 刷盘所有 MemTable：先切 active → immutable，再刷 immutable */
    if (db->memtable && skiplist_count(db->memtable) > 0) {
        db->immutable_memtable = db->memtable;
        db->memtable = skiplist_new();
        db->memtable_size = 0;
    }

    RWLOCK_UNLOCK_W(&db->rwlock);

    /* 刷 immutable memtable 到 SSTable */
    if (db->immutable_memtable) {
        kv_flush_memtable(db);
    }

    /* 3. 同步 WAL */
    if (db->wal_mgr) {
        wal_mgr_sync(db->wal_mgr);
    }

    /* 4. 同步 Manifest */
    manifest_sync(db->manifest);

    /* 5. 停止 merge 调度器，等待后台合并完成 */
    merge_scheduler_stop(&db->merge_ctx);
    merge_scheduler_join(&db->merge_ctx);

    /* 6. 创建备份目录 */
    mkdir_recursive(backup_dir);

    /* 7. 复制所有数据文件 */
    char src_path[512], dst_path[512];
    int errors = 0;

    /* 复制 MANIFEST */
    snprintf(src_path, sizeof(src_path), "%s/" MANIFEST_FILE_NAME, db->dir_path);
    snprintf(dst_path, sizeof(dst_path), "%s/" MANIFEST_FILE_NAME, backup_dir);
    if (copy_file(src_path, dst_path) != 0) {
        printf("[BACKUP] WARNING: Failed to copy MANIFEST\n");
        errors++;
    }

    /* 复制 MANIFEST.tmp（如果存在） */
    snprintf(src_path, sizeof(src_path), "%s/" MANIFEST_TMP_FILE_NAME, db->dir_path);
    snprintf(dst_path, sizeof(dst_path), "%s/" MANIFEST_TMP_FILE_NAME, backup_dir);
    copy_file(src_path, dst_path);  /* 忽略错误，tmp 文件可能不存在 */

    /* 复制 WAL 文件（使用新的编号命名） */
    {
        char wal_pattern[512];
        snprintf(wal_pattern, sizeof(wal_pattern), "%s/wal_*.log", db->dir_path);

#ifdef _WIN32
        WIN32_FIND_DATAA fd;
        HANDLE hFind = FindFirstFileA(wal_pattern, &fd);
        if (hFind != INVALID_HANDLE_VALUE) {
            do {
                snprintf(src_path, sizeof(src_path), "%s/%s", db->dir_path, fd.cFileName);
                snprintf(dst_path, sizeof(dst_path), "%s/%s", backup_dir, fd.cFileName);
                if (copy_file(src_path, dst_path) != 0) {
                    printf("[BACKUP] WARNING: Failed to copy WAL %s\n", fd.cFileName);
                    errors++;
                }
            } while (FindNextFileA(hFind, &fd));
            FindClose(hFind);
        }
#else
        /* Linux: 使用 opendir/readdir 遍历 WAL 文件 */
        {
            DIR* dir = opendir(db->dir_path);
            if (dir) {
                struct dirent* entry;
                while ((entry = readdir(dir)) != NULL) {
                    const char* name = entry->d_name;
                    size_t name_len = strlen(name);
                    /* 匹配 wal_*.log 模式 */
                    if (name_len < 8) continue;
                    if (strncmp(name, "wal_", 4) != 0) continue;
                    const char* ext = name + name_len - 4;
                    if (strcmp(ext, ".log") != 0) continue;
                    snprintf(src_path, sizeof(src_path), "%s/%s", db->dir_path, name);
                    snprintf(dst_path, sizeof(dst_path), "%s/%s", backup_dir, name);
                    if (copy_file(src_path, dst_path) != 0) {
                        printf("[BACKUP] WARNING: Failed to copy WAL %s\n", name);
                        errors++;
                    }
                }
                closedir(dir);
            }
        }
#endif
    }

    /* 复制所有 SSTable 文件 */
    #ifdef _WIN32
    EnterCriticalSection(&db->manifest_lock);
    #else
    pthread_mutex_lock(&db->manifest_lock);
    #endif

    manifest_file_t** files = NULL;
    size_t file_count = 0;
    if (manifest_list_files(db->manifest, -1, &files, &file_count) == 0) {
        for (size_t i = 0; i < file_count; i++) {
            snprintf(src_path, sizeof(src_path), "%s/%llu.sst", db->dir_path, (unsigned long long)files[i]->file_id);
            snprintf(dst_path, sizeof(dst_path), "%s/%llu.sst", backup_dir, (unsigned long long)files[i]->file_id);
            if (copy_file(src_path, dst_path) != 0) {
                printf("[BACKUP] WARNING: Failed to copy SSTable %llu\n", (unsigned long long)files[i]->file_id);
                errors++;
            }
        }
        kv_free(files);
    }

    #ifdef _WIN32
    LeaveCriticalSection(&db->manifest_lock);
    #else
    pthread_mutex_unlock(&db->manifest_lock);
    #endif

    printf("[BACKUP] Copied %zu SSTable files\n", file_count);

    /* 8. 重新启动 merge 调度器 */
    merge_scheduler_start(&db->merge_ctx);

    if (errors > 0) {
        printf("[BACKUP] Completed with %d errors\n", errors);
        return -1;
    }

    printf("[BACKUP] Backup completed successfully to %s\n", backup_dir);
    return 0;
}

kv_store_t* kv_restore(const char* backup_dir, const char* target_dir) {
    if (!backup_dir || !target_dir) return NULL;

    printf("[RESTORE] Restoring from %s to %s...\n", backup_dir, target_dir);

    /* 1. 创建目标目录 */
    mkdir_recursive(target_dir);

    /* 2. 复制备份文件到目标目录 */
    char src_path[512], dst_path[512];

    /* MANIFEST */
    snprintf(src_path, sizeof(src_path), "%s/" MANIFEST_FILE_NAME, backup_dir);
    snprintf(dst_path, sizeof(dst_path), "%s/" MANIFEST_FILE_NAME, target_dir);
    if (copy_file(src_path, dst_path) != 0) {
        printf("[RESTORE] ERROR: Backup MANIFEST not found\n");
        return NULL;
    }

    /* MANIFEST.tmp */
    snprintf(src_path, sizeof(src_path), "%s/" MANIFEST_TMP_FILE_NAME, backup_dir);
    snprintf(dst_path, sizeof(dst_path), "%s/" MANIFEST_TMP_FILE_NAME, target_dir);
    copy_file(src_path, dst_path);  /* 忽略错误 */

    /* WAL */
    snprintf(src_path, sizeof(src_path), "%s/wal.log", backup_dir);
    snprintf(dst_path, sizeof(dst_path), "%s/wal.log", target_dir);
    copy_file(src_path, dst_path);  /* 忽略错误 */

    /* 复制所有 .sst 文件 */
    #ifdef _WIN32
    WIN32_FIND_DATAA find_data;
    HANDLE find_handle;
    char search_pattern[512];
    snprintf(search_pattern, sizeof(search_pattern), "%s/*.sst", backup_dir);
    find_handle = FindFirstFileA(search_pattern, &find_data);
    if (find_handle != INVALID_HANDLE_VALUE) {
        do {
            snprintf(src_path, sizeof(src_path), "%s/%s", backup_dir, find_data.cFileName);
            snprintf(dst_path, sizeof(dst_path), "%s/%s", target_dir, find_data.cFileName);
            copy_file(src_path, dst_path);
        } while (FindNextFileA(find_handle, &find_data));
        FindClose(find_handle);
    }
    #else
    /* 使用 shell 通配符复制 */
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "cp %s/*.sst %s/ 2>/dev/null", backup_dir, target_dir);
    system(cmd);
    #endif

    printf("[RESTORE] Files copied, opening database at %s...\n", target_dir);

    /* 3. 打开恢复后的数据库 */
    kv_store_t* db = kv_open(target_dir);
    if (!db) {
        printf("[RESTORE] ERROR: Failed to open restored database\n");
        return NULL;
    }

    printf("[RESTORE] Database restored successfully\n");
    return db;
}

/* ================================================================
 * 快照导入：高效地将快照目录中的 SSTable 文件导入到当前数据库
 *
 * 流程：
 *   1. 停止 merge 调度器
 *   2. 刷盘活跃 memtable
 *   3. 清空 manifest
 *   4. 复制 MANIFEST 从快照目录到数据目录
 *   5. 扫描快照目录中的 .sst 文件并复制到数据目录
 *   6. 重新加载 manifest
 *   7. 重启 merge 调度器
 * ================================================================ */
int kv_import_snapshot_files(kv_store_t* db, const char* snapshot_data_dir) {
    if (!db || !snapshot_data_dir) return -1;

    printf("[IMPORT] Importing snapshot files from %s...\n", snapshot_data_dir);

    /* 1. 停止 merge 调度器并等待后台线程退出 */
    merge_scheduler_stop(&db->merge_ctx);
    merge_scheduler_join(&db->merge_ctx);

    /* 2. 刷盘活跃 memtable */
    RWLOCK_WRLOCK(&db->rwlock);
    if (db->memtable && skiplist_count(db->memtable) > 0) {
        db->immutable_memtable = db->memtable;
        db->memtable = skiplist_new();
        db->memtable_size = 0;
    }
    RWLOCK_UNLOCK_W(&db->rwlock);

    if (db->immutable_memtable) {
        kv_flush_memtable(db);
    }

    /* 3. 清空 manifest */
    manifest_clear(db->manifest);

    /* 4. 复制 MANIFEST 从快照目录到数据目录 */
    char src_path[512], dst_path[512];
    snprintf(src_path, sizeof(src_path), "%s/" MANIFEST_FILE_NAME, snapshot_data_dir);
    snprintf(dst_path, sizeof(dst_path), "%s/" MANIFEST_FILE_NAME, db->dir_path);
    if (copy_file(src_path, dst_path) != 0) {
        printf("[IMPORT] WARNING: MANIFEST not found in snapshot %s\n", src_path);
        /* 没有 MANIFEST 不算致命错误，继续尝试导入 */
    }

    /* 也复制 MANIFEST.tmp（如果存在） */
    snprintf(src_path, sizeof(src_path), "%s/" MANIFEST_TMP_FILE_NAME, snapshot_data_dir);
    snprintf(dst_path, sizeof(dst_path), "%s/" MANIFEST_TMP_FILE_NAME, db->dir_path);
    copy_file(src_path, dst_path);  /* 忽略错误 */

    /* 5. 扫描快照目录中的 .sst 文件并复制到数据目录 */
    char search_pattern[512];
    snprintf(search_pattern, sizeof(search_pattern), "%s/*.sst", snapshot_data_dir);

#ifdef _WIN32
    WIN32_FIND_DATAA find_data;
    HANDLE h_find = FindFirstFileA(search_pattern, &find_data);
    if (h_find != INVALID_HANDLE_VALUE) {
        do {
            snprintf(src_path, sizeof(src_path), "%s/%s", snapshot_data_dir, find_data.cFileName);
            snprintf(dst_path, sizeof(dst_path), "%s/%s", db->dir_path, find_data.cFileName);

            if (copy_file(src_path, dst_path) != 0) {
                printf("[IMPORT] ERROR: Failed to copy %s\n", find_data.cFileName);
                FindClose(h_find);
                merge_scheduler_start(&db->merge_ctx);
                return -1;
            }
            printf("[IMPORT] Copied %s to data directory\n", find_data.cFileName);
        } while (FindNextFileA(h_find, &find_data));
        FindClose(h_find);
    }
#else
    /* Linux: 使用 opendir/readdir 扫描 .sst 文件 */
    {
        DIR* dir = opendir(snapshot_data_dir);
        if (dir) {
            struct dirent* entry;
            while ((entry = readdir(dir)) != NULL) {
                const char* name = entry->d_name;
                size_t name_len = strlen(name);
                if (name_len < 4 || strcmp(name + name_len - 4, ".sst") != 0) continue;

                snprintf(src_path, sizeof(src_path), "%s/%s", snapshot_data_dir, name);
                snprintf(dst_path, sizeof(dst_path), "%s/%s", db->dir_path, name);

                if (copy_file(src_path, dst_path) != 0) {
                    printf("[IMPORT] ERROR: Failed to copy %s\n", name);
                    closedir(dir);
                    merge_scheduler_start(&db->merge_ctx);
                    return -1;
                }
                printf("[IMPORT] Copied %s to data directory\n", name);
            }
            closedir(dir);
        }
    }
#endif

    /* 6. 从复制的 MANIFEST 文件重新加载 manifest */
    manifest_load(db->manifest);

    /* 7. 同步 manifest 到磁盘 */
    manifest_sync(db->manifest);

    /* 8. 重启 merge 调度器 */
    merge_scheduler_start(&db->merge_ctx);

    printf("[IMPORT] Snapshot import completed successfully\n");
    return 0;
}