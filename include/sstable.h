#ifndef SSTABLE_H
#define SSTABLE_H

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include "compression.h"
#include "lru_cache.h"
#include "skiplist.h"

#ifndef SSTABLE_BLOCK_SIZE
#define SSTABLE_BLOCK_SIZE 4096
#endif
#define SSTABLE_FOOTER_SIZE 48
#define SSTABLE_RESTART_INTERVAL 16
#define SSTABLE_PREV_KEY_CAPACITY 4096

/* 默认压缩级别：1=快速压缩，兼顾速度和压缩比 */
#define SSTABLE_DEFAULT_COMPRESSION_LEVEL 1

typedef struct sstable_block {
    uint8_t* data;
    size_t size;
    size_t restart_count;
    uint32_t* restart_points;
} sstable_block_t;

typedef struct sstable {
    FILE* file;
    char* path;
    uint64_t file_id;
    size_t file_size;
    uint64_t index_offset;
    size_t index_size;
    uint64_t filter_offset;
    size_t filter_size;
    compression_type_t compression_type;  /* 压缩算法类型 */
    char* smallest_key;
    size_t smallest_key_len;
    char* largest_key;
    size_t largest_key_len;
    void* index_cache;
    void* filter_cache;
    uint8_t* cached_index_data;    /* 缓存的索引块原始数据 */
    size_t cached_index_data_len;
} sstable_t;

typedef struct sstable_iter {
    sstable_t* sst;
    sstable_block_t* current_block;
    size_t block_offset;
    size_t entry_offset;
    int eof;
    char prev_key[SSTABLE_PREV_KEY_CAPACITY];
    size_t prev_len;
} sstable_iter_t;

sstable_t* sstable_open(const char* path, uint64_t file_id);
void sstable_close(sstable_t* sst);
int sstable_write(const char* path, uint64_t file_id, skiplist_t* memtable, compression_type_t comp_type);
int sstable_lookup(sstable_t* sst, const char* key, size_t klen, char** out_value, size_t* out_vlen, lru_cache_t* block_cache);
sstable_iter_t* sstable_new_iterator(sstable_t* sst);
void sstable_iter_free(sstable_iter_t* iter);
int sstable_iter_next(sstable_iter_t* iter, char** key, size_t* klen, char** value, size_t* vlen);
const char* sstable_smallest_key(sstable_t* sst, size_t* out_len);
const char* sstable_largest_key(sstable_t* sst, size_t* out_len);

#endif