#include "lru_cache.h"
#include "mem.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static size_t lru_hash(const char* key, size_t len, size_t mask) {
    uint32_t h = 0x811c9dc5;
    for (size_t i = 0; i < len; i++) {
        h ^= key[i];
        h *= 0x01000193;
    }
    return h & mask;
}

static size_t next_power_of_two(size_t v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    return v;
}

static lru_node_t* lru_node_new(const char* key, size_t klen, const void* value, size_t vlen) {
    lru_node_t* node = kv_malloc(sizeof(lru_node_t));
    if (!node) return NULL;
    
    node->key = kv_malloc(klen);
    if (!node->key) {
        kv_free(node);
        return NULL;
    }
    memcpy(node->key, key, klen);
    node->key_len = klen;
    
    node->value = kv_malloc(vlen);
    if (!node->value) {
        kv_free(node->key);
        kv_free(node);
        return NULL;
    }
    memcpy(node->value, value, vlen);
    node->value_len = vlen;
    
    node->prev = NULL;
    node->next = NULL;
    
    return node;
}

static void lru_node_free(lru_node_t* node) {
    if (!node) return;
    kv_free(node->key);
    kv_free(node->value);
    kv_free(node);
}

static void lru_remove(lru_cache_t* cache, lru_node_t* node) {
    if (node->prev) {
        node->prev->next = node->next;
    } else {
        cache->head = node->next;
    }
    
    if (node->next) {
        node->next->prev = node->prev;
    } else {
        cache->tail = node->prev;
    }
    
    size_t idx = lru_hash(node->key, node->key_len, cache->hash_mask);
    if (cache->table[idx] == node) {
        cache->table[idx] = NULL;
    }
}

static void lru_add_to_head(lru_cache_t* cache, lru_node_t* node) {
    node->next = cache->head;
    node->prev = NULL;
    
    if (cache->head) {
        cache->head->prev = node;
    }
    cache->head = node;
    
    if (!cache->tail) {
        cache->tail = node;
    }
    
    size_t idx = lru_hash(node->key, node->key_len, cache->hash_mask);
    if (cache->table[idx] == NULL) {
        cache->table[idx] = node;
    }
}

lru_cache_t* lru_cache_new(size_t capacity) {
    if (capacity == 0) return NULL;
    
    lru_cache_t* cache = kv_malloc(sizeof(lru_cache_t));
    if (!cache) return NULL;
    
    cache->capacity = capacity;
    cache->size = 0;
    cache->head = NULL;
    cache->tail = NULL;
    
    size_t table_size = next_power_of_two(capacity);
    cache->hash_mask = table_size - 1;
    cache->table = kv_calloc(table_size, sizeof(lru_node_t*));
    if (!cache->table) {
        kv_free(cache);
        return NULL;
    }
    
#ifdef _WIN32
    InitializeCriticalSection(&cache->lock);
#else
    pthread_mutex_init(&cache->lock, NULL);
#endif
    
    return cache;
}

void lru_cache_free(lru_cache_t* cache) {
    if (!cache) return;
    
#ifdef _WIN32
    DeleteCriticalSection(&cache->lock);
#else
    pthread_mutex_destroy(&cache->lock);
#endif
    
    lru_node_t* node = cache->head;
    while (node) {
        lru_node_t* next = node->next;
        lru_node_free(node);
        node = next;
    }
    
    kv_free(cache->table);
    kv_free(cache);
}

int lru_cache_lookup(lru_cache_t* cache, const char* key, size_t klen, void** out_value, size_t* out_vlen) {
    if (!cache || !key || klen == 0 || !out_value || !out_vlen) return -1;
    
#ifdef _WIN32
    EnterCriticalSection(&cache->lock);
#else
    pthread_mutex_lock(&cache->lock);
#endif
    
    size_t idx = lru_hash(key, klen, cache->hash_mask);
    lru_node_t* node = cache->table[idx];
    
    /* 先检查哈希表直接命中的节点 */
    if (node && node->key_len == klen && memcmp(node->key, key, klen) == 0) {
        lru_remove(cache, node);
        lru_add_to_head(cache, node);
        
        *out_value = kv_malloc(node->value_len);
        if (!*out_value) {
#ifdef _WIN32
            LeaveCriticalSection(&cache->lock);
#else
            pthread_mutex_unlock(&cache->lock);
#endif
            return -1;
        }
        memcpy(*out_value, node->value, node->value_len);
        *out_vlen = node->value_len;
        
#ifdef _WIN32
        LeaveCriticalSection(&cache->lock);
#else
        pthread_mutex_unlock(&cache->lock);
#endif
        return 0;
    }
    
    /* 哈希冲突：遍历链表查找匹配节点 */
    node = cache->head;
    while (node) {
        if (node->key_len == klen && memcmp(node->key, key, klen) == 0) {
            lru_remove(cache, node);
            lru_add_to_head(cache, node);
            /* 更新哈希表指向该节点 */
            cache->table[idx] = node;
            
            *out_value = kv_malloc(node->value_len);
            if (!*out_value) {
#ifdef _WIN32
                LeaveCriticalSection(&cache->lock);
#else
                pthread_mutex_unlock(&cache->lock);
#endif
                return -1;
            }
            memcpy(*out_value, node->value, node->value_len);
            *out_vlen = node->value_len;
            
#ifdef _WIN32
            LeaveCriticalSection(&cache->lock);
#else
            pthread_mutex_unlock(&cache->lock);
#endif
            return 0;
        }
        node = node->next;
    }
    
#ifdef _WIN32
    LeaveCriticalSection(&cache->lock);
#else
    pthread_mutex_unlock(&cache->lock);
#endif
    
    return -1;
}

int lru_cache_insert(lru_cache_t* cache, const char* key, size_t klen, const void* value, size_t vlen) {
    if (!cache || !key || klen == 0 || !value) return -1;
    
#ifdef _WIN32
    EnterCriticalSection(&cache->lock);
#else
    pthread_mutex_lock(&cache->lock);
#endif
    
    size_t idx = lru_hash(key, klen, cache->hash_mask);
    lru_node_t* node = cache->table[idx];
    
    if (node && node->key_len == klen && memcmp(node->key, key, klen) == 0) {
        lru_remove(cache, node);
        lru_node_free(node);
        cache->size--;
    }
    
    while (cache->size >= cache->capacity && cache->tail) {
        lru_node_t* tail = cache->tail;
        lru_remove(cache, tail);
        lru_node_free(tail);
        cache->size--;
    }
    
    node = lru_node_new(key, klen, value, vlen);
    if (!node) {
#ifdef _WIN32
        LeaveCriticalSection(&cache->lock);
#else
        pthread_mutex_unlock(&cache->lock);
#endif
        return -1;
    }
    
    lru_add_to_head(cache, node);
    cache->size++;
    
#ifdef _WIN32
    LeaveCriticalSection(&cache->lock);
#else
    pthread_mutex_unlock(&cache->lock);
#endif
    
    return 0;
}

void lru_cache_remove(lru_cache_t* cache, const char* key, size_t klen) {
    if (!cache || !key || klen == 0) return;
    
#ifdef _WIN32
    EnterCriticalSection(&cache->lock);
#else
    pthread_mutex_lock(&cache->lock);
#endif
    
    size_t idx = lru_hash(key, klen, cache->hash_mask);
    lru_node_t* node = cache->table[idx];
    
    if (!node) {
#ifdef _WIN32
        LeaveCriticalSection(&cache->lock);
#else
        pthread_mutex_unlock(&cache->lock);
#endif
        return;
    }
    
    if (node->key_len != klen || memcmp(node->key, key, klen) != 0) {
#ifdef _WIN32
        LeaveCriticalSection(&cache->lock);
#else
        pthread_mutex_unlock(&cache->lock);
#endif
        return;
    }
    
    lru_remove(cache, node);
    lru_node_free(node);
    cache->size--;
    
#ifdef _WIN32
    LeaveCriticalSection(&cache->lock);
#else
    pthread_mutex_unlock(&cache->lock);
#endif
}

size_t lru_cache_size(lru_cache_t* cache) {
    if (!cache) return 0;
    return cache->size;
}