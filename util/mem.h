#ifndef MEM_H
#define MEM_H

#include <stddef.h>

void* kv_malloc(size_t size);
void* kv_calloc(size_t nmemb, size_t size);
void* kv_realloc(void* ptr, size_t size);
void kv_free(void* ptr);
char* kv_strdup(const char* s);

#endif