#ifndef MANIFEST_H
#define MANIFEST_H

#include <stddef.h>
#include <stdint.h>

#define MANIFEST_FILE_NAME "MANIFEST"
#define MANIFEST_TMP_FILE_NAME "MANIFEST.tmp"

typedef struct manifest_file {
    uint64_t file_id;
    int level;
    char* smallest_key;
    size_t smallest_key_len;
    char* largest_key;
    size_t largest_key_len;
    size_t file_size;
    struct manifest_file* next;
} manifest_file_t;

typedef struct manifest {
    char* dir_path;
    manifest_file_t* files;
    uint64_t next_file_id;
    uint64_t version;
} manifest_t;

manifest_t* manifest_open(const char* dir_path);
void manifest_close(manifest_t* m);
int manifest_add_file(manifest_t* m, uint64_t file_id, int level, const char* smallest_key, size_t sklen, const char* largest_key, size_t lklen, size_t file_size);
int manifest_remove_file(manifest_t* m, uint64_t file_id);
int manifest_clear(manifest_t* m);
int manifest_list_files(manifest_t* m, int level, manifest_file_t*** out_files, size_t* out_count);
uint64_t manifest_next_file_id(manifest_t* m);
int manifest_sync(manifest_t* m);
int manifest_load(manifest_t* m);

#endif