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
    size_t memtable_size;
} kv_store_t;

typedef struct kv_iter {
    kv_store_t* db;
    skiplist_iter_t* mem_iter;
    skiplist_iter_t* imm_iter;
    sstable_iter_t** sst_iters;
    size_t sst_iter_count;
    char* start_key;
    size_t start_len;
    char* end_key;
    size_t end_len;
    int started;
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
        skiplist_delete(db->memtable, record->key, record->key_len);
    }
    
    return 0;
}

static int kv_flush_memtable(kv_store_t* db) {
    if (!db || !db->immutable_memtable) return -1;
    
    char path[512];
    uint64_t file_id = manifest_next_file_id(db->manifest);
    
    snprintf(path, sizeof(path), "%s/%llu.sst", db->dir_path, (unsigned long long)file_id);
    
    if (sstable_write(path, file_id, db->immutable_memtable) != 0) {
        return -1;
    }
    
    skiplist_iter_t* iter = skiplist_new_iterator(db->immutable_memtable);
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
    
    return 0;
}

static int kv_switch_memtable(kv_store_t* db) {
    if (!db) return -1;
    
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
    db->merge_ctx.stop = 0;
    
    RWLOCK_INIT(&db->rwlock);
    
    merge_scheduler_start(&db->merge_ctx);
    
    return db;
}

void kv_close(kv_store_t* db) {
    if (!db) return;
    
    db->merge_ctx.stop = 1;
    
    if (db->immutable_memtable) {
        kv_flush_memtable(db);
    }
    
    if (db->memtable && skiplist_count(db->memtable) > 0) {
        db->immutable_memtable = db->memtable;
        kv_flush_memtable(db);
    }
    
    skiplist_free(db->memtable);
    
    if (db->wal) {
        wal_close(db->wal);
    }
    
    manifest_close(db->manifest);
    lru_cache_free(db->block_cache);
    
    RWLOCK_DESTROY(&db->rwlock);
    
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
    
    if (skiplist_lookup(db->memtable, key, klen, &value, &vlen) == 0) {
        *out_val = value;
        *out_vlen = vlen;
        RWLOCK_UNLOCK(&db->rwlock);
        return 0;
    }
    
    if (db->immutable_memtable) {
        if (skiplist_lookup(db->immutable_memtable, key, klen, &value, &vlen) == 0) {
            *out_val = value;
            *out_vlen = vlen;
            RWLOCK_UNLOCK(&db->rwlock);
            return 0;
        }
    }
    
    RWLOCK_UNLOCK(&db->rwlock);
    
    manifest_file_t** files = NULL;
    size_t count = 0;
    
    if (manifest_list_files(db->manifest, -1, &files, &count) != 0) {
        return -1;
    }
    
    char path[512];
    
    for (int level = 0; level < MAX_LEVELS; level++) {
        for (size_t i = 0; i < count; i++) {
            if (files[i]->level != level) continue;
            
            snprintf(path, sizeof(path), "%s/%llu.sst", db->dir_path, (unsigned long long)files[i]->file_id);
            sstable_t* sst = sstable_open(path, files[i]->file_id);
            if (!sst) continue;
            
            if (sstable_lookup(sst, key, klen, &value, &vlen) == 0) {
                *out_val = value;
                *out_vlen = vlen;
                sstable_close(sst);
                kv_free(files);
                return 0;
            }
            
            sstable_close(sst);
        }
    }
    
    kv_free(files);
    
    return -1;
}

int kv_delete(kv_store_t* db, const char* key, size_t klen) {
    if (!db || !key || klen == 0) return -1;
    
    RWLOCK_WRLOCK(&db->rwlock);
    
    if (db->wal) {
        wal_write(db->wal, WAL_DELETE, key, klen, NULL, 0);
    }
    
    skiplist_delete(db->memtable, key, klen);
    
    if (db->memtable_size >= MEMTABLE_SIZE_LIMIT) {
        RWLOCK_UNLOCK_W(&db->rwlock);
        kv_switch_memtable(db);
        return 0;
    }
    
    RWLOCK_UNLOCK_W(&db->rwlock);
    
    return 0;
}

kv_iter_t* kv_scan(kv_store_t* db, const char* start, size_t slen, const char* end, size_t elen) {
    if (!db) return NULL;
    
    kv_iter_t* iter = kv_malloc(sizeof(kv_iter_t));
    if (!iter) return NULL;
    
    iter->db = db;
    
    iter->mem_iter = skiplist_new_iterator(db->memtable);
    iter->imm_iter = db->immutable_memtable ? skiplist_new_iterator(db->immutable_memtable) : NULL;
    
    manifest_file_t** files = NULL;
    size_t count = 0;
    
    if (manifest_list_files(db->manifest, -1, &files, &count) == 0) {
        iter->sst_iters = kv_malloc(count * sizeof(sstable_iter_t*));
        if (iter->sst_iters) {
            char path[512];
            iter->sst_iter_count = 0;
            
            for (size_t i = 0; i < count; i++) {
                snprintf(path, sizeof(path), "%s/%llu.sst", db->dir_path, (unsigned long long)files[i]->file_id);
                sstable_t* sst = sstable_open(path, files[i]->file_id);
                if (sst) {
                    iter->sst_iters[iter->sst_iter_count++] = sstable_new_iterator(sst);
                    sstable_close(sst);
                }
            }
        }
        kv_free(files);
    } else {
        iter->sst_iters = NULL;
        iter->sst_iter_count = 0;
    }
    
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
    
    iter->started = 0;
    
    return iter;
}

int kv_iter_next(kv_iter_t* iter, char** key, size_t* klen, char** val, size_t* vlen) {
    if (!iter || !key || !klen || !val || !vlen) return -1;
    
    int cmp;
    
    if (iter->mem_iter) {
        char* k = NULL;
        size_t kl = 0;
        char* v = NULL;
        size_t vl = 0;
        
        while (skiplist_iter_next(iter->mem_iter, &k, &kl, &v, &vl) == 0) {
            if (iter->start_key) {
                cmp = kv_compare_keys(k, kl, iter->start_key, iter->start_len);
                if (cmp < 0) {
                    kv_free(k);
                    kv_free(v);
                    continue;
                }
            }
            
            if (iter->end_key) {
                cmp = kv_compare_keys(k, kl, iter->end_key, iter->end_len);
                if (cmp >= 0) {
                    kv_free(k);
                    kv_free(v);
                    continue;
                }
            }
            
            *key = k;
            *klen = kl;
            *val = v;
            *vlen = vl;
            return 0;
        }
    }
    
    if (iter->imm_iter) {
        char* k = NULL;
        size_t kl = 0;
        char* v = NULL;
        size_t vl = 0;
        
        while (skiplist_iter_next(iter->imm_iter, &k, &kl, &v, &vl) == 0) {
            if (iter->start_key) {
                cmp = kv_compare_keys(k, kl, iter->start_key, iter->start_len);
                if (cmp < 0) {
                    kv_free(k);
                    kv_free(v);
                    continue;
                }
            }
            
            if (iter->end_key) {
                cmp = kv_compare_keys(k, kl, iter->end_key, iter->end_len);
                if (cmp >= 0) {
                    kv_free(k);
                    kv_free(v);
                    continue;
                }
            }
            
            *key = k;
            *klen = kl;
            *val = v;
            *vlen = vl;
            return 0;
        }
    }
    
    for (size_t i = 0; i < iter->sst_iter_count; i++) {
        char* k = NULL;
        size_t kl = 0;
        char* v = NULL;
        size_t vl = 0;
        
        while (sstable_iter_next(iter->sst_iters[i], &k, &kl, &v, &vl) == 0) {
            if (iter->start_key) {
                cmp = kv_compare_keys(k, kl, iter->start_key, iter->start_len);
                if (cmp < 0) {
                    kv_free(k);
                    kv_free(v);
                    continue;
                }
            }
            
            if (iter->end_key) {
                cmp = kv_compare_keys(k, kl, iter->end_key, iter->end_len);
                if (cmp >= 0) {
                    kv_free(k);
                    kv_free(v);
                    continue;
                }
            }
            
            *key = k;
            *klen = kl;
            *val = v;
            *vlen = vl;
            return 0;
        }
    }
    
    return -1;
}

void kv_iter_free(kv_iter_t* iter) {
    if (!iter) return;
    
    if (iter->mem_iter) {
        skiplist_iter_free(iter->mem_iter);
    }
    
    if (iter->imm_iter) {
        skiplist_iter_free(iter->imm_iter);
    }
    
    if (iter->sst_iters) {
        for (size_t i = 0; i < iter->sst_iter_count; i++) {
            sstable_iter_free(iter->sst_iters[i]);
        }
        kv_free(iter->sst_iters);
    }
    
    kv_free(iter->start_key);
    kv_free(iter->end_key);
    kv_free(iter);
}