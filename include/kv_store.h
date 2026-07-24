#ifndef KV_STORE_H
#define KV_STORE_H

#include <stddef.h>

#define MEMTABLE_SIZE_LIMIT (4 * 1024 * 1024)

typedef struct kv_store kv_store_t;
typedef struct kv_iter kv_iter_t;

kv_store_t* kv_open(const char* dir_path);
void kv_close(kv_store_t* db);
int kv_put(kv_store_t* db, const char* key, size_t klen, const char* val, size_t vlen);
int kv_get(kv_store_t* db, const char* key, size_t klen, char** out_val, size_t* out_vlen);
int kv_delete(kv_store_t* db, const char* key, size_t klen);
kv_iter_t* kv_scan(kv_store_t* db, const char* start, size_t slen, const char* end, size_t elen);
int kv_iter_next(kv_iter_t* iter, char** key, size_t* klen, char** val, size_t* vlen);
void kv_iter_free(kv_iter_t* iter);

#endif