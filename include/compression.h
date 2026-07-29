#ifndef COMPRESSION_H
#define COMPRESSION_H

#include <stddef.h>
#include <stdint.h>

/* 压缩算法类型 */
typedef enum {
    COMPRESSION_NONE = 0,  /* 不压缩 */
    COMPRESSION_ZSTD = 1,  /* zstd 压缩（高压缩比） */
    COMPRESSION_LZ4  = 2,  /* LZ4 压缩（高速度） */
} compression_type_t;

/* 压缩数据块：compress(src, src_len) -> dst, dst_len
 * 返回 0 成功，-1 失败
 * 调用者负责释放 *dst */
int compression_compress(compression_type_t type, const uint8_t* src, size_t src_len,
                         uint8_t** dst, size_t* dst_len, int level);

/* 解压数据块：decompress(src, src_len) -> dst, dst_len
 * 返回 0 成功，-1 失败
 * 调用者负责释放 *dst */
int compression_decompress(compression_type_t type, const uint8_t* src, size_t src_len,
                           uint8_t** dst, size_t* dst_len);

/* 返回压缩后最大可能大小（用于分配缓冲区） */
size_t compression_bound(compression_type_t type, size_t src_len);

/* 返回压缩类型名称 */
const char* compression_type_name(compression_type_t type);

/* 从字符串解析压缩类型 */
compression_type_t compression_type_from_name(const char* name);

#endif /* COMPRESSION_H */