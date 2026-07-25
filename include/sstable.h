#ifndef SSTABLE_H
#define SSTABLE_H

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include "skiplist.h"

#define SSTABLE_BLOCK_SIZE 4096
#define SSTABLE_FOOTER_SIZE 48
#define SSTABLE_RESTART_INTERVAL 16

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
    char* smallest_key;
    size_t smallest_key_len;
    char* largest_key;
    size_t largest_key_len;
    void* index_cache;
    void* filter_cache;
} sstable_t;

typedef struct sstable_iter {
    sstable_t* sst;
    sstable_block_t* current_block;
    size_t block_offset;
    size_t entry_offset;
    int eof;
} sstable_iter_t;

sstable_t* sstable_open(const char* path, uint64_t file_id);
void sstable_close(sstable_t* sst);
int sstable_write(const char* path, uint64_t file_id, skiplist_t* memtable);
int sstable_lookup(sstable_t* sst, const char* key, size_t klen, char** out_value, size_t* out_vlen);
sstable_iter_t* sstable_new_iterator(sstable_t* sst);
void sstable_iter_free(sstable_iter_t* iter);
int sstable_iter_next(sstable_iter_t* iter, char** key, size_t* klen, char** value, size_t* vlen);
const char* sstable_smallest_key(sstable_t* sst, size_t* out_len);
const char* sstable_largest_key(sstable_t* sst, size_t* out_len);

#endif