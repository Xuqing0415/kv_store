#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <stddef.h>

#define SKIPLIST_MAX_LEVEL 12
#define SKIPLIST_P 0.5

typedef struct skiplist_node {
    char* key;
    size_t key_len;
    char* value;
    size_t value_len;
    int deleted;
    struct skiplist_node* forward[SKIPLIST_MAX_LEVEL];
} skiplist_node_t;

typedef struct skiplist_iter {
    skiplist_node_t* current;
} skiplist_iter_t;

typedef struct skiplist {
    skiplist_node_t* header;
    skiplist_node_t* tail;
    int level;
    size_t count;
    size_t memory_usage;
} skiplist_t;

skiplist_t* skiplist_new(void);
void skiplist_free(skiplist_t* sl);
int skiplist_insert(skiplist_t* sl, const char* key, size_t klen, const char* value, size_t vlen);
int skiplist_lookup(skiplist_t* sl, const char* key, size_t klen, char** out_value, size_t* out_vlen);
int skiplist_delete(skiplist_t* sl, const char* key, size_t klen);
size_t skiplist_count(skiplist_t* sl);
size_t skiplist_memory_usage(skiplist_t* sl);
skiplist_iter_t* skiplist_new_iterator(skiplist_t* sl);
void skiplist_iter_free(skiplist_iter_t* iter);
int skiplist_iter_next(skiplist_iter_t* iter, char** key, size_t* klen, char** value, size_t* vlen);

#endif