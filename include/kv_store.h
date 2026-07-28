#ifndef KV_STORE_H
#define KV_STORE_H

#include <stddef.h>

#ifndef MEMTABLE_SIZE_LIMIT
#define MEMTABLE_SIZE_LIMIT (256 * 1024)  /* 256KB，便于快速触发刷盘测试 */
#endif

typedef struct kv_store kv_store_t;
typedef struct kv_iter kv_iter_t;
typedef struct kv_snapshot kv_snapshot_t;

kv_store_t* kv_open(const char* dir_path);
void kv_close(kv_store_t* db);
int kv_put(kv_store_t* db, const char* key, size_t klen, const char* val, size_t vlen);
int kv_get(kv_store_t* db, const char* key, size_t klen, char** out_val, size_t* out_vlen);
int kv_delete(kv_store_t* db, const char* key, size_t klen);
int kv_sync(kv_store_t* db);
int kv_force_merge(kv_store_t* db);
kv_iter_t* kv_scan(kv_store_t* db, const char* start, size_t slen, const char* end, size_t elen);
int kv_iter_next(kv_iter_t* iter, char** key, size_t* klen, char** val, size_t* vlen);
void kv_iter_free(kv_iter_t* iter);

/* 快照：固定时间点只读视图，用于备份和重复读场景 */
kv_snapshot_t* kv_snapshot_create(kv_store_t* db);
int kv_snapshot_get(kv_snapshot_t* snap, const char* key, size_t klen, char** out_val, size_t* out_vlen);
kv_iter_t* kv_snapshot_scan(kv_snapshot_t* snap, const char* start, size_t slen, const char* end, size_t elen);
void kv_snapshot_free(kv_snapshot_t* snap);

#endif