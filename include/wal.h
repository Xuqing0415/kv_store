#ifndef WAL_H
#define WAL_H

#include <stddef.h>
#include <stdio.h>

#define WAL_HEADER_SIZE 4
#define WAL_CRC_SIZE 4

typedef enum {
    WAL_PUT = 1,
    WAL_DELETE = 2,
} wal_record_type_t;

typedef struct wal_record {
    wal_record_type_t type;
    char* key;
    size_t key_len;
    char* value;
    size_t value_len;
} wal_record_t;

typedef struct wal {
    FILE* file;
    char* path;
    size_t sync_interval;
    size_t bytes_since_sync;
} wal_t;

wal_t* wal_open(const char* path);
void wal_close(wal_t* w);
int wal_write(wal_t* w, wal_record_type_t type, const char* key, size_t klen, const char* value, size_t vlen);
int wal_sync(wal_t* w);
int wal_replay(wal_t* w, int (*callback)(wal_record_t* record, void* arg), void* arg);
int wal_truncate(wal_t* w, size_t offset);
size_t wal_size(wal_t* w);

#endif