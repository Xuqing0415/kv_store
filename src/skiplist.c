#include "skiplist.h"
#include "mem.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int skiplist_random_level(void) {
    int level = 1;
    while ((rand() & 0xFFFF) < (SKIPLIST_P * 0xFFFF)) {
        level++;
    }
    return (level < SKIPLIST_MAX_LEVEL) ? level : SKIPLIST_MAX_LEVEL;
}

static skiplist_node_t* skiplist_node_new(const char* key, size_t klen, const char* value, size_t vlen, int level) {
    skiplist_node_t* node = kv_malloc(sizeof(skiplist_node_t) + (level - 1) * sizeof(skiplist_node_t*));
    if (!node) return NULL;
    
    node->key = kv_malloc(klen);
    if (!node->key) {
        kv_free(node);
        return NULL;
    }
    memcpy(node->key, key, klen);
    node->key_len = klen;
    
    if (value) {
        node->value = kv_malloc(vlen);
        if (!node->value) {
            kv_free(node->key);
            kv_free(node);
            return NULL;
        }
        memcpy(node->value, value, vlen);
    } else {
        node->value = NULL;
    }
    node->value_len = vlen;
    node->deleted = 0;
    
    for (int i = 0; i < level; i++) {
        node->forward[i] = NULL;
    }
    
    return node;
}

static void skiplist_node_free(skiplist_node_t* node) {
    if (!node) return;
    kv_free(node->key);
    kv_free(node->value);
    kv_free(node);
}

static int skiplist_key_compare(const char* a, size_t a_len, const char* b, size_t b_len) {
    size_t min_len = a_len < b_len ? a_len : b_len;
    int cmp = memcmp(a, b, min_len);
    if (cmp != 0) return cmp;
    if (a_len < b_len) return -1;
    if (a_len > b_len) return 1;
    return 0;
}

skiplist_t* skiplist_new(void) {
    srand((unsigned int)time(NULL));
    
    skiplist_t* sl = kv_malloc(sizeof(skiplist_t));
    if (!sl) return NULL;
    
    sl->header = skiplist_node_new("", 0, NULL, 0, SKIPLIST_MAX_LEVEL);
    if (!sl->header) {
        kv_free(sl);
        return NULL;
    }
    
    sl->tail = NULL;
    sl->level = 1;
    sl->count = 0;
    sl->memory_usage = 0;
    
    return sl;
}

void skiplist_free(skiplist_t* sl) {
    if (!sl) return;
    
    skiplist_node_t* node = sl->header->forward[0];
    skiplist_node_free(sl->header);
    
    while (node) {
        skiplist_node_t* next = node->forward[0];
        skiplist_node_free(node);
        node = next;
    }
    
    kv_free(sl);
}

int skiplist_insert(skiplist_t* sl, const char* key, size_t klen, const char* value, size_t vlen) {
    if (!sl || !key || klen == 0) return -1;
    
    skiplist_node_t* update[SKIPLIST_MAX_LEVEL];
    skiplist_node_t* x = sl->header;
    
    for (int i = sl->level - 1; i >= 0; i--) {
        while (x->forward[i] && skiplist_key_compare(x->forward[i]->key, x->forward[i]->key_len, key, klen) < 0) {
            x = x->forward[i];
        }
        update[i] = x;
    }
    
    x = x->forward[0];
    
    if (x && skiplist_key_compare(x->key, x->key_len, key, klen) == 0) {
        sl->memory_usage -= x->value_len;
        kv_free(x->value);
        if (value) {
            x->value = kv_malloc(vlen);
            if (!x->value) return -1;
            memcpy(x->value, value, vlen);
        } else {
            x->value = NULL;
        }
        x->value_len = vlen;
        x->deleted = 0;
        sl->memory_usage += vlen;
        return 0;
    }
    
    int level = skiplist_random_level();
    if (level > sl->level) {
        for (int i = sl->level; i < level; i++) {
            update[i] = sl->header;
        }
        sl->level = level;
    }
    
    x = skiplist_node_new(key, klen, value, vlen, level);
    if (!x) return -1;
    
    for (int i = 0; i < level; i++) {
        x->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = x;
    }
    
    sl->count++;
    sl->memory_usage += klen + vlen + sizeof(skiplist_node_t) + (level - 1) * sizeof(skiplist_node_t*);
    
    return 0;
}

int skiplist_lookup(skiplist_t* sl, const char* key, size_t klen, char** out_value, size_t* out_vlen) {
    if (!sl || !key || klen == 0 || !out_value || !out_vlen) return -1;
    
    skiplist_node_t* x = sl->header;
    
    for (int i = sl->level - 1; i >= 0; i--) {
        while (x->forward[i] && skiplist_key_compare(x->forward[i]->key, x->forward[i]->key_len, key, klen) < 0) {
            x = x->forward[i];
        }
    }
    
    x = x->forward[0];
    
    if (x && skiplist_key_compare(x->key, x->key_len, key, klen) == 0) {
        if (x->deleted) {
            return -1;
        }
        *out_value = kv_malloc(x->value_len);
        if (!*out_value) return -1;
        memcpy(*out_value, x->value, x->value_len);
        *out_vlen = x->value_len;
        return 0;
    }
    
    return -1;
}

int skiplist_delete(skiplist_t* sl, const char* key, size_t klen) {
    if (!sl || !key || klen == 0) return -1;
    
    skiplist_node_t* update[SKIPLIST_MAX_LEVEL];
    skiplist_node_t* x = sl->header;
    
    for (int i = sl->level - 1; i >= 0; i--) {
        while (x->forward[i] && skiplist_key_compare(x->forward[i]->key, x->forward[i]->key_len, key, klen) < 0) {
            x = x->forward[i];
        }
        update[i] = x;
    }
    
    x = x->forward[0];
    
    if (x && skiplist_key_compare(x->key, x->key_len, key, klen) == 0) {
        x->deleted = 1;
        return 0;
    }
    
    return -1;
}

size_t skiplist_count(skiplist_t* sl) {
    if (!sl) return 0;
    return sl->count;
}

size_t skiplist_memory_usage(skiplist_t* sl) {
    if (!sl) return 0;
    return sl->memory_usage;
}

skiplist_iter_t* skiplist_new_iterator(skiplist_t* sl) {
    if (!sl) return NULL;
    
    skiplist_iter_t* iter = kv_malloc(sizeof(skiplist_iter_t));
    if (!iter) return NULL;
    
    iter->current = sl->header->forward[0];
    return iter;
}

void skiplist_iter_free(skiplist_iter_t* iter) {
    if (iter) {
        kv_free(iter);
    }
}

int skiplist_iter_next(skiplist_iter_t* iter, char** key, size_t* klen, char** value, size_t* vlen) {
    if (!iter || !iter->current) return -1;
    
    skiplist_node_t* node = iter->current;
    iter->current = node->forward[0];
    
    while (node && node->deleted) {
        node = iter->current;
        if (!node) return -1;
        iter->current = node->forward[0];
    }
    
    *key = kv_malloc(node->key_len);
    if (!*key) return -1;
    memcpy(*key, node->key, node->key_len);
    *klen = node->key_len;
    
    *value = kv_malloc(node->value_len);
    if (!*value) {
        kv_free(*key);
        return -1;
    }
    memcpy(*value, node->value, node->value_len);
    *vlen = node->value_len;
    
    return 0;
}