#include "compression.h"
#include "mem.h"
#include <stdlib.h>
#include <string.h>
#include <zstd.h>
#include <lz4.h>
#include <lz4hc.h>

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
    case COMPRESSION_LZ4: {
        int src_size = (int)src_len;
        if ((size_t)src_size != src_len) return -1; /* 溢出检查 */

        int bound = LZ4_compressBound(src_size);
        *dst = kv_malloc((size_t)bound + 4); /* +4 存储原始大小 */
        if (!*dst) return -1;

        /* 使用 LZ4_compress_HC 进行高压缩比压缩，level 控制压缩级别 (1-12) */
        int lz4_level = (level <= 0) ? LZ4HC_CLEVEL_DEFAULT : 
                        (level > LZ4HC_CLEVEL_MAX) ? LZ4HC_CLEVEL_MAX : level;
        int compressed = LZ4_compress_HC((const char*)src, (char*)(*dst + 4),
                                          src_size, bound, lz4_level);
        if (compressed <= 0) {
            kv_free(*dst);
            *dst = NULL;
            return -1;
        }

        /* 在压缩数据前存储原始大小，LZ4 解压时需要 */
        ((uint32_t*)*dst)[0] = (uint32_t)src_len;
        *dst_len = (size_t)compressed + 4;
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
    case COMPRESSION_LZ4: {
        if (src_len < 4) return -1;

        /* 读取原始大小（存储在压缩数据前 4 字节） */
        uint32_t orig_size = ((const uint32_t*)src)[0];
        if (orig_size == 0) return -1;

        *dst = kv_malloc(orig_size);
        if (!*dst) return -1;

        int decompressed = LZ4_decompress_safe((const char*)(src + 4), (char*)*dst,
                                                (int)(src_len - 4), (int)orig_size);
        if (decompressed < 0 || (size_t)decompressed != orig_size) {
            kv_free(*dst);
            *dst = NULL;
            return -1;
        }
        *dst_len = orig_size;
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
    case COMPRESSION_LZ4:
        return (size_t)LZ4_compressBound((int)src_len) + 4; /* +4 存储原始大小 */
    default:
        return src_len;
    }
}

const char* compression_type_name(compression_type_t type) {
    switch (type) {
    case COMPRESSION_NONE: return "none";
    case COMPRESSION_ZSTD: return "zstd";
    case COMPRESSION_LZ4:  return "lz4";
    default: return "unknown";
    }
}

compression_type_t compression_type_from_name(const char* name) {
    if (!name) return COMPRESSION_ZSTD;
    if (strcmp(name, "none") == 0) return COMPRESSION_NONE;
    if (strcmp(name, "zstd") == 0) return COMPRESSION_ZSTD;
    if (strcmp(name, "lz4") == 0)  return COMPRESSION_LZ4;
    return COMPRESSION_ZSTD; /* 默认 */
}