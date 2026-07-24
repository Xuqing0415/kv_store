#include "manifest.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#define rename(old, new) MoveFileExA(old, new, MOVEFILE_REPLACE_EXISTING)
#else
#include <unistd.h>
#endif

static void manifest_file_free(manifest_file_t* file) {
    if (!file) return;
    kv_free(file->smallest_key);
    kv_free(file->largest_key);
    kv_free(file);
}

static void manifest_files_free(manifest_file_t* files) {
    manifest_file_t* file = files;
    while (file) {
        manifest_file_t* next = file->next;
        manifest_file_free(file);
        file = next;
    }
}

manifest_t* manifest_open(const char* dir_path) {
    if (!dir_path) return NULL;
    
    manifest_t* m = kv_malloc(sizeof(manifest_t));
    if (!m) return NULL;
    
    m->dir_path = kv_strdup(dir_path);
    if (!m->dir_path) {
        kv_free(m);
        return NULL;
    }
    
    m->files = NULL;
    m->next_file_id = 1;
    m->version = 0;
    
    manifest_load(m);
    
    return m;
}

void manifest_close(manifest_t* m) {
    if (!m) return;
    
    manifest_files_free(m->files);
    kv_free(m->dir_path);
    kv_free(m);
}

int manifest_add_file(manifest_t* m, uint64_t file_id, int level, const char* smallest_key, size_t sklen, const char* largest_key, size_t lklen, size_t file_size) {
    if (!m || !smallest_key || !largest_key) return -1;
    
    manifest_file_t* file = kv_malloc(sizeof(manifest_file_t));
    if (!file) return -1;
    
    file->file_id = file_id;
    file->level = level;
    file->file_size = file_size;
    
    file->smallest_key = kv_malloc(sklen);
    if (!file->smallest_key) {
        kv_free(file);
        return -1;
    }
    memcpy(file->smallest_key, smallest_key, sklen);
    file->smallest_key_len = sklen;
    
    file->largest_key = kv_malloc(lklen);
    if (!file->largest_key) {
        kv_free(file->smallest_key);
        kv_free(file);
        return -1;
    }
    memcpy(file->largest_key, largest_key, lklen);
    file->largest_key_len = lklen;
    
    file->next = m->files;
    m->files = file;
    
    if (file_id >= m->next_file_id) {
        m->next_file_id = file_id + 1;
    }
    
    return 0;
}

int manifest_remove_file(manifest_t* m, uint64_t file_id) {
    if (!m) return -1;
    
    manifest_file_t** prev = &m->files;
    manifest_file_t* file = m->files;
    
    while (file) {
        if (file->file_id == file_id) {
            *prev = file->next;
            manifest_file_free(file);
            return 0;
        }
        prev = &file->next;
        file = file->next;
    }
    
    return -1;
}

int manifest_list_files(manifest_t* m, int level, manifest_file_t*** out_files, size_t* out_count) {
    if (!m || !out_files || !out_count) return -1;
    
    size_t count = 0;
    manifest_file_t* file = m->files;
    
    while (file) {
        if (level < 0 || file->level == level) {
            count++;
        }
        file = file->next;
    }
    
    *out_files = kv_malloc(count * sizeof(manifest_file_t*));
    if (!*out_files) return -1;
    
    count = 0;
    file = m->files;
    
    while (file) {
        if (level < 0 || file->level == level) {
            (*out_files)[count++] = file;
        }
        file = file->next;
    }
    
    *out_count = count;
    return 0;
}

uint64_t manifest_next_file_id(manifest_t* m) {
    if (!m) return 1;
    return m->next_file_id++;
}

int manifest_sync(manifest_t* m) {
    if (!m) return -1;
    
    char tmp_path[512];
    char manifest_path[512];
    
    snprintf(tmp_path, sizeof(tmp_path), "%s/%s", m->dir_path, MANIFEST_TMP_FILE_NAME);
    snprintf(manifest_path, sizeof(manifest_path), "%s/%s", m->dir_path, MANIFEST_FILE_NAME);
    
    FILE* file = fopen(tmp_path, "wb");
    if (!file) return -1;
    
    fprintf(file, "version=%llu\n", (unsigned long long)m->version);
    fprintf(file, "next_file_id=%llu\n", (unsigned long long)m->next_file_id);
    
    manifest_file_t* f = m->files;
    while (f) {
        fprintf(file, "file=%llu,level=%d,size=%llu,smallest=", 
                (unsigned long long)f->file_id, f->level, (unsigned long long)f->file_size);
        
        for (size_t i = 0; i < f->smallest_key_len; i++) {
            fprintf(file, "%02x", (unsigned char)f->smallest_key[i]);
        }
        
        fprintf(file, ",largest=");
        for (size_t i = 0; i < f->largest_key_len; i++) {
            fprintf(file, "%02x", (unsigned char)f->largest_key[i]);
        }
        fprintf(file, "\n");
        
        f = f->next;
    }
    
    fclose(file);
    
    if (rename(tmp_path, manifest_path) != 0) {
        remove(tmp_path);
        return -1;
    }
    
    m->version++;
    
    return 0;
}

int manifest_load(manifest_t* m) {
    if (!m) return -1;
    
    char manifest_path[512];
    snprintf(manifest_path, sizeof(manifest_path), "%s/%s", m->dir_path, MANIFEST_FILE_NAME);
    
    FILE* file = fopen(manifest_path, "r");
    if (!file) return 0;
    
    char line[1024];
    while (fgets(line, sizeof(line), file)) {
        size_t len = strlen(line);
        if (len > 0 && line[len - 1] == '\n') {
            line[len - 1] = '\0';
        }
        
        if (strncmp(line, "version=", 8) == 0) {
            sscanf(line + 8, "%llu", (unsigned long long*)&m->version);
        } else if (strncmp(line, "next_file_id=", 13) == 0) {
            sscanf(line + 13, "%llu", (unsigned long long*)&m->next_file_id);
        } else if (strncmp(line, "file=", 5) == 0) {
            uint64_t file_id;
            int level;
            uint64_t file_size;
            char smallest_hex[512];
            char largest_hex[512];
            
            if (sscanf(line + 5, "%llu,level=%d,size=%llu,smallest=%511[^,],largest=%511s",
                       (unsigned long long*)&file_id, &level, (unsigned long long*)&file_size,
                       smallest_hex, largest_hex) == 5) {
                
                size_t sklen = strlen(smallest_hex) / 2;
                char* smallest_key = kv_malloc(sklen);
                if (!smallest_key) continue;
                
                for (size_t i = 0; i < sklen; i++) {
                    sscanf(smallest_hex + i * 2, "%2hhx", &smallest_key[i]);
                }
                
                size_t lklen = strlen(largest_hex) / 2;
                char* largest_key = kv_malloc(lklen);
                if (!largest_key) {
                    kv_free(smallest_key);
                    continue;
                }
                
                for (size_t i = 0; i < lklen; i++) {
                    sscanf(largest_hex + i * 2, "%2hhx", &largest_key[i]);
                }
                
                manifest_add_file(m, file_id, level, smallest_key, sklen, largest_key, lklen, file_size);
                
                kv_free(smallest_key);
                kv_free(largest_key);
            }
        }
    }
    
    fclose(file);
    
    return 0;
}