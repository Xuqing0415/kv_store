#include "mem.h"
#include <stdlib.h>
#include <string.h>

void* kv_malloc(size_t size) {
    return malloc(size);
}

void* kv_calloc(size_t nmemb, size_t size) {
    return calloc(nmemb, size);
}

void* kv_realloc(void* ptr, size_t size) {
    return realloc(ptr, size);
}

void kv_free(void* ptr) {
    if (ptr) free(ptr);
}

char* kv_strdup(const char* s) {
    if (!s) return NULL;
    size_t len = strlen(s) + 1;
    char* dup = kv_malloc(len);
    if (dup) memcpy(dup, s, len);
    return dup;
}