#ifndef LRU_CACHE_H
#define LRU_CACHE_H

#include <stddef.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#endif

typedef struct lru_node {
    char* key;
    size_t key_len;
    void* value;
    size_t value_len;
    struct lru_node* prev;
    struct lru_node* next;
} lru_node_t;

typedef struct lru_cache {
    lru_node_t* head;
    lru_node_t* tail;
    lru_node_t** table;
    size_t capacity;
    size_t size;
    size_t hash_mask;
#ifdef _WIN32
    CRITICAL_SECTION lock;
#else
    pthread_mutex_t lock;
#endif
} lru_cache_t;

lru_cache_t* lru_cache_new(size_t capacity);
void lru_cache_free(lru_cache_t* cache);
int lru_cache_lookup(lru_cache_t* cache, const char* key, size_t klen, void** out_value, size_t* out_vlen);
int lru_cache_insert(lru_cache_t* cache, const char* key, size_t klen, const void* value, size_t vlen);
void lru_cache_remove(lru_cache_t* cache, const char* key, size_t klen);
size_t lru_cache_size(lru_cache_t* cache);

#endif