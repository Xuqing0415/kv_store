#include "compression.h"
#include "mem.h"
#include <stdlib.h>
#include <string.h>
#include <zstd.h>

int compression_compress(compression_type_t type, const uint8_t* src, size_t src_len,
                         uint8_t** dst, size_t* dst_len, int level) {
    if (!src || src_len == 0 || !dst || !dst_len) return -1;

    switch (type) {
    case COMPRESSION_NONE: {
        *dst = kv_malloc(src_len);
        if (!*dst) return -1;
        memcpy(*dst, src, src_len);
        *dst_len = src_len;
        return 0;
    }
    case COMPRESSION_ZSTD: {
        size_t bound = ZSTD_compressBound(src_len);
        *dst = kv_malloc(bound);
        if (!*dst) return -1;
        size_t compressed = ZSTD_compress(*dst, bound, src, src_len, level);
        if (ZSTD_isError(compressed)) {
            kv_free(*dst);
            *dst = NULL;
            return -1;
        }
        *dst_len = compressed;
        return 0;
    }
    default:
        return -1;
    }
}

int compression_decompress(compression_type_t type, const uint8_t* src, size_t src_len,
                           uint8_t** dst, size_t* dst_len) {
    if (!src || src_len == 0 || !dst || !dst_len) return -1;

    switch (type) {
    case COMPRESSION_NONE: {
        *dst = kv_malloc(src_len);
        if (!*dst) return -1;
        memcpy(*dst, src, src_len);
        *dst_len = src_len;
        return 0;
    }
    case COMPRESSION_ZSTD: {
        /* 获取解压后大小 */
        unsigned long long decompressed_size = ZSTD_getFrameContentSize(src, src_len);
        if (decompressed_size == ZSTD_CONTENTSIZE_ERROR) {
            return -1;
        }
        if (decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
            /* 未知大小，使用流式解压或估算 */
            decompressed_size = src_len * 4;
        }
        *dst = kv_malloc((size_t)decompressed_size);
        if (!*dst) return -1;
        size_t result = ZSTD_decompress(*dst, (size_t)decompressed_size, src, src_len);
        if (ZSTD_isError(result)) {
            kv_free(*dst);
            *dst = NULL;
            return -1;
        }
        *dst_len = result;
        return 0;
    }
    default:
        return -1;
    }
}

size_t compression_bound(compression_type_t type, size_t src_len) {
    switch (type) {
    case COMPRESSION_NONE:
        return src_len;
    case COMPRESSION_ZSTD:
        return ZSTD_compressBound(src_len);
    default:
        return src_len;
    }
}

const char* compression_type_name(compression_type_t type) {
    switch (type) {
    case COMPRESSION_NONE: return "none";
    case COMPRESSION_ZSTD: return "zstd";
    default: return "unknown";
    }
}