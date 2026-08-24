#include "sstable.h"
#include "bloom_filter.h"
#include "compression.h"
#include "crc32.h"
#include "encoding.h"
#include "lru_cache.h"
#include "mem.h"
#include "skiplist.h"
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

typedef struct sstable_entry {
    size_t shared_len;
    size_t unshared_len;
    size_t value_len;
    char* key;
    char* value;
} sstable_entry_t;

/* 前向声明 */
static int sstable_block_decode(sstable_block_t* block, const uint8_t* data, size_t size);

/* ===== 辅助函数：读取并解压一个数据块 ===== */
static int sstable_read_block(sstable_t* sst, size_t* out_offset, sstable_block_t* block, lru_cache_t* block_cache) {
    uint64_t block_offset = ftell(sst->file);

    /* 读取 4 字节块大小前缀（压缩后大小） */
    uint32_t compressed_size;
    if (fread(&compressed_size, 4, 1, sst->file) != 1) return -1;

    /* 尝试从缓存读取（缓存存储的是解压后的数据） */
    if (block_cache) {
        char cache_key[16];
        memcpy(cache_key, &sst->file_id, 8);
        memcpy(cache_key + 8, &block_offset, 8);
        void* cached_data = NULL;
        size_t cached_len = 0;
        if (lru_cache_lookup(block_cache, cache_key, 16, &cached_data, &cached_len) == 0) {
            int ret = sstable_block_decode(block, (uint8_t*)cached_data, cached_len);
            kv_free(cached_data);  /* lru_cache_lookup 分配了副本，需要释放 */
            return ret;
        }
    }

    /* 读取压缩数据 */
    uint8_t* compressed_data = kv_malloc(compressed_size);
    if (!compressed_data) return -1;

    size_t bytes_read = fread(compressed_data, 1, compressed_size, sst->file);
    if (bytes_read != compressed_size) {
        kv_free(compressed_data);
        return -1;
    }

    /* 解压 */
    uint8_t* decompressed_data = NULL;
    size_t decompressed_size = 0;

    if (sst->compression_type == COMPRESSION_NONE) {
        decompressed_data = compressed_data;
        decompressed_size = compressed_size;
    } else {
        if (compression_decompress(sst->compression_type, compressed_data, compressed_size,
                                   &decompressed_data, &decompressed_size) != 0) {
            kv_free(compressed_data);
            return -1;
        }
        kv_free(compressed_data);
    }

    /* 缓存解压后的数据 */
    if (block_cache) {
        char cache_key[16];
        memcpy(cache_key, &sst->file_id, 8);
        memcpy(cache_key + 8, &block_offset, 8);
        /* 缓存一份副本 */
        uint8_t* cache_copy = kv_malloc(decompressed_size);
        if (cache_copy) {
            memcpy(cache_copy, decompressed_data, decompressed_size);
            lru_cache_insert(block_cache, cache_key, 16, cache_copy, decompressed_size);
        }
    }

    int ret = sstable_block_decode(block, decompressed_data, decompressed_size);
    kv_free(decompressed_data);
    if (out_offset) *out_offset = block_offset;
    return ret;
}

static int sstable_block_build(sstable_block_t* block, skiplist_iter_t* iter, char* prev_key, size_t* prev_len,
    char** overflow_key, size_t* overflow_klen, char** overflow_value, size_t* overflow_vlen) {
    if (!block || !iter) return -1;
    
    block->data = kv_malloc(SSTABLE_BLOCK_SIZE);
    if (!block->data) return -1;
    
    size_t offset = 0;
    size_t restart_count = 0;
    /* 初始分配足够 1024 个重启点 */
    uint32_t* restart_points = kv_malloc(1024 * sizeof(uint32_t));
    if (!restart_points) {
        kv_free(block->data);
        block->data = NULL;
        return -1;
    }
    size_t restart_capacity = 1024;
    
    /* 第一个重启点总在 offset 0 */
    restart_points[restart_count++] = 0;
    
    char* key = NULL;
    size_t klen = 0;
    char* value = NULL;
    size_t vlen = 0;
    int first_entry = 1;
    int entries_in_block = 0;
    size_t last_key_len = 0;  /* 跟踪最后一个 key 的实际长度，不被重启点重置影响 */
    
    /* 检查是否有上次未写入的溢出条目（跨块边界被截断的记录） */
    if (*overflow_key != NULL) {
        key = *overflow_key;
        klen = *overflow_klen;
        value = *overflow_value;
        vlen = *overflow_vlen;
        *overflow_key = NULL;
        *overflow_value = NULL;
    }
    
    while (1) {
        /* 如果没有待处理的条目（非溢出），从迭代器获取下一个 */
        if (key == NULL) {
            if (skiplist_iter_next(iter, &key, &klen, &value, &vlen) != 0) {
                break;
            }
        }
        
        size_t shared_len = 0;
        if (!first_entry) {
            size_t min_len = *prev_len < klen ? *prev_len : klen;
            while (shared_len < min_len && key[shared_len] == prev_key[shared_len]) {
                shared_len++;
            }
        }
        first_entry = 0;
        
        size_t unshared_len = klen - shared_len;
        
        uint8_t header[12];
        size_t header_len = 0;
        header_len += encode_varint(shared_len, header + header_len);
        header_len += encode_varint(unshared_len, header + header_len);
        header_len += encode_varint(vlen, header + header_len);
        
        size_t entry_size = header_len + unshared_len + vlen;
        if (offset + entry_size + 8 > SSTABLE_BLOCK_SIZE) {
            /* 当前条目放不下，保存到溢出参数供下一个块作为首条记录 */
            /* 修复幽灵重启点：若最后一个重启点刚好记录在当前偏移（它指向本就不存在的
             * 后续条目，即块恰好在一个重启区间边界处填满），删除该重启点。
             * 否则二分查找会定位到这个空位置，导致该块尾部条目（含墓碑）全部查不到，
             * 表现为"数据丢失/已删除 key 复活"。 */
            if (restart_count > 1 && restart_points[restart_count - 1] == (uint32_t)offset) {
                restart_count--;
            }
            *overflow_key = key;
            *overflow_klen = klen;
            *overflow_value = value;
            *overflow_vlen = vlen;
            break;
        }
        
        memcpy(block->data + offset, header, header_len);
        offset += header_len;
        
        memcpy(block->data + offset, key + shared_len, unshared_len);
        offset += unshared_len;
        
        memcpy(block->data + offset, value, vlen);
        offset += vlen;
        
        uint32_t crc = crc32((uint8_t*)block->data + (offset - entry_size), entry_size);
        memcpy(block->data + offset, &crc, 4);
        offset += 4;
        
        /* 更新 prev_key */
        if (klen < SSTABLE_PREV_KEY_CAPACITY) {
            memcpy(prev_key, key, klen);
            *prev_len = klen;
        } else {
            /* key 太长，截断处理 */
            memcpy(prev_key, key, SSTABLE_PREV_KEY_CAPACITY - 1);
            *prev_len = SSTABLE_PREV_KEY_CAPACITY - 1;
        }
        last_key_len = *prev_len;  /* 保存实际 key 长度，用于索引条目 */
        
        entries_in_block++;
        
        /* 每隔 SSTABLE_RESTART_INTERVAL 条记录一个重启点 */
        if (entries_in_block % SSTABLE_RESTART_INTERVAL == 0) {
            if (restart_count >= restart_capacity) {
                size_t new_cap = restart_capacity + 1024;
                uint32_t* new_rp = kv_realloc(restart_points, new_cap * sizeof(uint32_t));
                if (!new_rp) {
                    kv_free(key);
                    kv_free(value);
                    kv_free(block->data);
                    block->data = NULL;
                    kv_free(restart_points);
                    return -1;
                }
                restart_points = new_rp;
                restart_capacity = new_cap;
            }
            restart_points[restart_count++] = (uint32_t)offset;
            /* 重置 prev_key，确保重启点后的第一条记录 shared_len=0 */
            *prev_len = 0;
        }
        
        kv_free(key);
        kv_free(value);
        key = NULL;
    }
    
    /* 如果没有写入任何条目，释放资源并返回 -1 */
    if (entries_in_block == 0) {
        kv_free(block->data);
        block->data = NULL;
        kv_free(restart_points);
        block->size = 0;
        block->restart_count = 0;
        return -1;
    }
    
    /* 确保重启数组不超出 block 大小 */
    size_t restart_array_size = restart_count * sizeof(uint32_t);
    while (offset + restart_array_size + 8 > SSTABLE_BLOCK_SIZE && restart_count > 1) {
        restart_count--;
        restart_array_size = restart_count * sizeof(uint32_t);
    }
    
    /* 写入重启点数组（密集的，每个索引都有有效值） */
    for (size_t i = 0; i < restart_count; i++) {
        memcpy(block->data + offset + i * 4, &restart_points[i], 4);
    }
    offset += restart_array_size;
    
    uint32_t rc_le = (uint32_t)restart_count;
    memcpy(block->data + offset, &rc_le, 4);
    offset += 4;
    
    uint32_t crc = crc32((uint8_t*)restart_points, restart_array_size + 4);
    memcpy(block->data + offset, &crc, 4);
    offset += 4;
    
    kv_free(restart_points);
    
    block->size = offset;
    block->restart_count = restart_count;
    
    /* 恢复 prev_len 为最后一个 key 的实际长度，供索引条目使用 */
    *prev_len = last_key_len;
    
    return 0;
}

static int sstable_block_decode(sstable_block_t* block, const uint8_t* data, size_t size) {
    if (!block || !data || size == 0) return -1;
    
    block->data = kv_malloc(size);
    if (!block->data) return -1;
    memcpy(block->data, data, size);
    block->size = size;
    
    block->restart_points = NULL;
    block->restart_count = 0;
    
    // Format: [entries][restart_points][restart_count(4B)][CRC(4B)]
    // Need at least 8 bytes for restart_count + CRC
    if (size > 8) {
        uint32_t rc;
        decode_fixed32(data + size - 8, &rc);
        block->restart_count = (size_t)rc;
        
        size_t restart_array_size = block->restart_count * sizeof(uint32_t);
        
        if (restart_array_size + 8 > size) {
            block->restart_count = 0;
            return -1;
        }
        
        block->restart_points = kv_malloc(restart_array_size);
        if (!block->restart_points) {
            kv_free(block->data);
            block->data = NULL;
            return -1;
        }
        
        size_t restart_offset = size - 8 - restart_array_size;
        for (size_t i = 0; i < block->restart_count; i++) {
            decode_fixed32(data + restart_offset + i * 4, &block->restart_points[i]);
        }
    }
    
    return 0;
}

static void sstable_block_free(sstable_block_t* block) {
    if (!block) return;
    kv_free(block->data);
    kv_free(block->restart_points);
}

static int sstable_entry_decode(sstable_block_t* block, size_t offset, sstable_entry_t* entry, char* prev_key, size_t* prev_len) {
    if (!block || !entry || !prev_key) return -1;
    
    uint8_t* ptr = block->data + offset;
    size_t remaining = block->size - offset - 4;
    
    uint64_t shared_len;
    size_t consumed = decode_varint(ptr, remaining, &shared_len);
    if (consumed == 0) return -1;
    ptr += consumed;
    remaining -= consumed;
    
    uint64_t unshared_len;
    consumed = decode_varint(ptr, remaining, &unshared_len);
    if (consumed == 0) return -1;
    ptr += consumed;
    remaining -= consumed;
    
    uint64_t value_len;
    consumed = decode_varint(ptr, remaining, &value_len);
    if (consumed == 0) return -1;
    ptr += consumed;
    remaining -= consumed;
    
    if (unshared_len + value_len > remaining) return -1;
    
    /* 安全检查：shared_len 不能超过已解码的 prev_key 长度 */
    if (shared_len > *prev_len) return -1;
    
    entry->shared_len = (size_t)shared_len;
    entry->unshared_len = (size_t)unshared_len;
    entry->value_len = (size_t)value_len;
    
    size_t key_len = shared_len + unshared_len;
    
    /* 安全检查：key_len 不能超过 prev_key 缓冲区大小 */
    if (key_len > SSTABLE_PREV_KEY_CAPACITY) return -1;
    entry->key = kv_malloc(key_len);
    if (!entry->key) return -1;
    
    memcpy(entry->key, prev_key, shared_len);
    memcpy(entry->key + shared_len, ptr, unshared_len);
    ptr += unshared_len;
    
    entry->value = NULL;
    if (value_len > 0) {
        entry->value = kv_malloc((size_t)value_len);
        if (!entry->value) {
            kv_free(entry->key);
            return -1;
        }
        memcpy(entry->value, ptr, (size_t)value_len);
    }
    
    memcpy(prev_key, entry->key, key_len);
    *prev_len = key_len;
    
    return 0;
}

static int sstable_block_lookup(sstable_block_t* block, const char* key, size_t klen, char** out_value, size_t* out_vlen) {
    if (!block || !key || klen == 0 || !out_value || !out_vlen) return -1;
    
    if (block->restart_count == 0) return -1;
    
    char prev_key[SSTABLE_PREV_KEY_CAPACITY];
    size_t prev_len = 0;
    
    int left = 0;
    int right = (int)block->restart_count - 1;
    
    while (left < right) {
        int mid = (left + right + 1) / 2;
        size_t offset = block->restart_points[mid];
        
        /* 每次二分探测都重置 prev_key，避免前缀解码状态污染 */
        memset(prev_key, 0, sizeof(prev_key));
        prev_len = 0;
        
        sstable_entry_t entry;
        if (sstable_entry_decode(block, offset, &entry, prev_key, &prev_len) != 0) {
            right = mid - 1;
            continue;
        }
        
        size_t entry_key_len = entry.shared_len + entry.unshared_len;
        size_t min_cmp = entry_key_len < klen ? entry_key_len : klen;
        int cmp = memcmp(entry.key, key, min_cmp);
        if (cmp == 0) {
            cmp = (entry_key_len < klen) ? -1 : (entry_key_len > klen) ? 1 : 0;
        }
        kv_free(entry.key);
        kv_free(entry.value);
        
        if (cmp < 0) {
            left = mid;
        } else {
            right = mid - 1;
        }
    }
    
    size_t offset = block->restart_points[left];
    /* 重置 prev_key 用于线性扫描 */
    memset(prev_key, 0, sizeof(prev_key));
    prev_len = 0;
    
    /* 计算条目区的结束位置（排除重启点数组和尾部 CRC） */
    size_t entries_end = block->size - block->restart_count * 4 - 8;
    
    while (offset < entries_end) {
        sstable_entry_t entry;
        if (sstable_entry_decode(block, offset, &entry, prev_key, &prev_len) != 0) {
            break;
        }
        
        size_t entry_key_len = entry.shared_len + entry.unshared_len;
        int cmp = memcmp(entry.key, key, entry_key_len < klen ? entry_key_len : klen);
        
        if (cmp == 0 && entry_key_len == klen) {
            /* tombstone: value_len == 0 表示已删除，返回 -2 */
            if (entry.value_len == 0) {
                kv_free(entry.key);
                kv_free(entry.value);
                return -2;
            }
            *out_value = entry.value;
            *out_vlen = entry.value_len;
            kv_free(entry.key);
            return 0;
        }
        
        if (cmp > 0) {
            kv_free(entry.key);
            kv_free(entry.value);
            break;
        }
        
        size_t entry_offset = offset;
        kv_free(entry.key);
        kv_free(entry.value);
        
        uint8_t* ptr = block->data + offset;
        size_t remaining = block->size - offset;
        
        uint64_t shared_len;
        size_t consumed = decode_varint(ptr, remaining, &shared_len);
        if (consumed == 0) break;
        ptr += consumed;
        remaining -= consumed;
        
        uint64_t unshared_len;
        consumed = decode_varint(ptr, remaining, &unshared_len);
        if (consumed == 0) break;
        ptr += consumed;
        remaining -= consumed;
        
        uint64_t value_len;
        consumed = decode_varint(ptr, remaining, &value_len);
        if (consumed == 0) break;
        ptr += consumed;
        remaining -= consumed;
        
        offset = entry_offset + (ptr - (block->data + entry_offset)) + (size_t)unshared_len + (size_t)value_len + 4;
    }
    
    return -1;
}

typedef struct index_entry {
    char* last_key;
    size_t key_len;
    uint64_t offset;
    size_t size;
    struct index_entry* next;
} index_entry_t;

static void index_entry_free(index_entry_t* entry) {
    while (entry) {
        index_entry_t* next = entry->next;
        kv_free(entry->last_key);
        kv_free(entry);
        entry = next;
    }
}

int sstable_write(const char* path, uint64_t file_id, skiplist_t* memtable, compression_type_t comp_type) {
    (void)file_id;
    if (!path || !memtable) return -1;
    
    FILE* file = fopen(path, "wb");
    if (!file) return -1;
    
    skiplist_iter_t* iter = skiplist_new_iterator(memtable);
    if (!iter) {
        fclose(file);
        return -1;
    }
    
    char prev_key[SSTABLE_PREV_KEY_CAPACITY] = {0};
    size_t prev_len = 0;
    
    index_entry_t* index_head = NULL;
    index_entry_t** index_tail = &index_head;
    
    sstable_block_t block;
    memset(&block, 0, sizeof(block));
    
    /* 溢出条目：当一个条目跨块边界放不下时，保存到此处供下一个块使用 */
    char* overflow_key = NULL;
    size_t overflow_klen = 0;
    char* overflow_value = NULL;
    size_t overflow_vlen = 0;

    size_t total_uncompressed = 0;
    size_t total_compressed = 0;
    compression_type_t effective_comp_type = comp_type;  /* 实际使用的压缩类型（可能因失败回退） */
    
    while (sstable_block_build(&block, iter, prev_key, &prev_len,
                                &overflow_key, &overflow_klen, &overflow_value, &overflow_vlen) == 0) {
        uint64_t block_offset = ftell(file);
        total_uncompressed += block.size;

        /* 压缩块数据 */
        uint8_t* compressed_data = NULL;
        size_t compressed_size = 0;
        if (compression_compress(effective_comp_type, block.data, block.size,
                                 &compressed_data, &compressed_size,
                                 SSTABLE_DEFAULT_COMPRESSION_LEVEL) != 0) {
            /* 压缩失败，回退到不压缩 */
            compressed_data = block.data;
            compressed_size = block.size;
            effective_comp_type = COMPRESSION_NONE;
        }
        total_compressed += compressed_size;
        
        /* 写入 4 字节块大小前缀（压缩后大小，小端序） */
        uint32_t data_size_le = (uint32_t)compressed_size;
        fwrite(&data_size_le, 4, 1, file);
        
        fwrite(compressed_data, 1, compressed_size, file);
        
        index_entry_t* entry = kv_malloc(sizeof(index_entry_t));
        if (entry) {
            entry->last_key = kv_malloc(prev_len);
            if (entry->last_key) {
                memcpy(entry->last_key, prev_key, prev_len);
                entry->key_len = prev_len;
                entry->offset = block_offset;
                entry->size = compressed_size + 4;  /* 总大小 = 4字节前缀 + 压缩数据 */
                entry->next = NULL;
                *index_tail = entry;
                index_tail = &entry->next;
            } else {
                kv_free(entry);
            }
        }
        
        if (compressed_data != block.data) {
            kv_free(compressed_data);
        }
        sstable_block_free(&block);
        memset(&block, 0, sizeof(block));
        /* 重置 prev_key 确保每个块独立：块首条记录 shared_len=0 */
        prev_len = 0;
    }
    
    /* 清理残留的溢出条目（正常情况不会残留，仅防御性编程） */
    if (overflow_key) kv_free(overflow_key);
    if (overflow_value) kv_free(overflow_value);
    
    skiplist_iter_free(iter);
    
    printf("[SSTABLE] Compression: %s, %zu -> %zu bytes (%.1f%%)\n",
           compression_type_name(effective_comp_type), total_uncompressed, total_compressed,
           total_uncompressed > 0 ? (100.0 * total_compressed / total_uncompressed) : 0.0);
    
    uint64_t index_offset = ftell(file);
    
    uint8_t index_buf[8192];
    size_t index_pos = 0;
    
    for (index_entry_t* e = index_head; e; e = e->next) {
        if (index_pos + 8 + 4 + 4 + e->key_len > sizeof(index_buf)) {
            fwrite(index_buf, 1, index_pos, file);
            index_pos = 0;
        }
        
        index_pos += encode_fixed64(e->offset, index_buf + index_pos);
        index_pos += encode_fixed32((uint32_t)e->size, index_buf + index_pos);
        index_pos += encode_fixed32((uint32_t)e->key_len, index_buf + index_pos);
        memcpy(index_buf + index_pos, e->last_key, e->key_len);
        index_pos += e->key_len;
    }
    
    if (index_pos > 0) {
        fwrite(index_buf, 1, index_pos, file);
    }
    
    uint64_t index_size = ftell(file) - index_offset;
    index_entry_free(index_head);
    
    uint64_t filter_offset = ftell(file);
    uint64_t filter_size = 0;
    
    /* Footer 格式:
     *  0-7:   index_offset
     *  8-15:  index_size
     *  16-23: filter_offset
     *  24-31: filter_size
     *  32:     compression_type (1 byte)
     *  33-47:  reserved (15 bytes)
     */
    uint8_t footer[SSTABLE_FOOTER_SIZE];
    memset(footer, 0, SSTABLE_FOOTER_SIZE);
    encode_fixed64(index_offset, footer);
    encode_fixed64(index_size, footer + 8);
    encode_fixed64(filter_offset, footer + 16);
    encode_fixed64(filter_size, footer + 24);
    footer[32] = (uint8_t)effective_comp_type;
    
    fwrite(footer, 1, SSTABLE_FOOTER_SIZE, file);
    
#ifndef _WIN32
    /* 通知内核释放该文件的页缓存（减少 APU 平台内存压力） */
    {
        int fd = fileno(file);
        if (fd >= 0) {
            posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
        }
    }
#endif
    
    fclose(file);
    
    return 0;
}

sstable_t* sstable_open(const char* path, uint64_t file_id) {
    if (!path) return NULL;
    
    sstable_t* sst = kv_malloc(sizeof(sstable_t));
    if (!sst) return NULL;
    
#ifdef USE_O_DIRECT
    /* 使用 O_DIRECT 绕过页缓存，减少 APU 平台内存压力。
     * 要求：块大小 4KB 对齐，stdio 缓冲区与块边界对齐。
     * 如果 O_DIRECT 打开失败（EINVAL：文件系统不支持），回退到普通模式。 */
    {
        int fd = open(path, O_RDONLY | O_DIRECT);
        if (fd >= 0) {
            sst->file = fdopen(fd, "rb");
            if (!sst->file) { close(fd); }
        }
        if (fd < 0 || !sst->file) {
            if (fd < 0 && errno == EINVAL) {
                printf("[SSTABLE] O_DIRECT not supported by filesystem, falling back to buffered I/O for %s\n", path);
            } else if (fd < 0) {
                printf("[SSTABLE] O_DIRECT open failed (errno=%d), falling back to buffered I/O for %s\n", errno, path);
            }
            sst->file = fopen(path, "rb");
        }
    }
#else
    sst->file = fopen(path, "rb");
#endif
    if (!sst->file) {
        kv_free(sst);
        return NULL;
    }
    
    sst->path = kv_strdup(path);
    if (!sst->path) {
        fclose(sst->file);
        kv_free(sst);
        return NULL;
    }
    
    sst->file_id = file_id;
    sst->compression_type = COMPRESSION_NONE;  /* 默认不压缩（兼容旧格式） */
    
    if (fseek(sst->file, 0, SEEK_END) != 0) {
        fclose(sst->file);
        kv_free(sst->path);
        kv_free(sst);
        return NULL;
    }
    sst->file_size = ftell(sst->file);
    
    if (sst->file_size >= SSTABLE_FOOTER_SIZE) {
        if (fseek(sst->file, sst->file_size - SSTABLE_FOOTER_SIZE, SEEK_SET) != 0) {
            fclose(sst->file);
            kv_free(sst->path);
            kv_free(sst);
            return NULL;
        }
        
        uint8_t footer[SSTABLE_FOOTER_SIZE];
        if (fread(footer, 1, SSTABLE_FOOTER_SIZE, sst->file) != SSTABLE_FOOTER_SIZE) {
            fclose(sst->file);
            kv_free(sst->path);
            kv_free(sst);
            return NULL;
        }
        
        decode_fixed64(footer, &sst->index_offset);
        decode_fixed64(footer + 8, (uint64_t*)&sst->index_size);
        decode_fixed64(footer + 16, &sst->filter_offset);
        decode_fixed64(footer + 24, (uint64_t*)&sst->filter_size);
        
        /* 读取压缩类型（向后兼容：旧格式 footer[32] 为 0，即 COMPRESSION_NONE） */
        sst->compression_type = (compression_type_t)footer[32];
    }
    
    sst->smallest_key = NULL;
    sst->smallest_key_len = 0;
    sst->largest_key = NULL;
    sst->largest_key_len = 0;
    sst->index_cache = NULL;
    sst->filter_cache = NULL;
    sst->cached_index_data = NULL;
    sst->cached_index_data_len = 0;
    
    return sst;
}

void sstable_close(sstable_t* sst) {
    if (!sst) return;
    
    if (sst->file) {
        fclose(sst->file);
    }
    kv_free(sst->path);
    kv_free(sst->smallest_key);
    kv_free(sst->largest_key);
    kv_free(sst->cached_index_data);
    kv_free(sst);
}

int sstable_lookup(sstable_t* sst, const char* key, size_t klen, char** out_value, size_t* out_vlen, lru_cache_t* block_cache) {
    if (!sst || !key || klen == 0 || !out_value || !out_vlen) return -1;
    
    if (sst->file_size < SSTABLE_FOOTER_SIZE) return -1;
    
    if (sst->index_size == 0) {
        /* 单块 SSTable：直接从文件开头读取 */
        if (fseek(sst->file, 0, SEEK_SET) != 0) return -1;
        
        sstable_block_t block;
        if (sstable_read_block(sst, NULL, &block, block_cache) != 0) return -1;
        
        int ret = sstable_block_lookup(&block, key, klen, out_value, out_vlen);
        sstable_block_free(&block);
        return ret;
    }
    
    /* 多块 SSTable：通过索引定位目标块 */
    uint8_t* index_data = NULL;
    
    if (sst->cached_index_data) {
        index_data = sst->cached_index_data;
    } else {
        index_data = kv_malloc((size_t)sst->index_size);
        if (!index_data) return -1;
        
        if (fseek(sst->file, (long)sst->index_offset, SEEK_SET) != 0) {
            kv_free(index_data);
            return -1;
        }
        
        if (fread(index_data, 1, (size_t)sst->index_size, sst->file) != (size_t)sst->index_size) {
            kv_free(index_data);
            return -1;
        }
        
        sst->cached_index_data = index_data;
        sst->cached_index_data_len = (size_t)sst->index_size;
    }
    
    uint64_t target_offset = 0;
    size_t pos = 0;
    
    while (pos < sst->index_size) {
        uint64_t offset;
        size_t consumed = decode_fixed64(index_data + pos, &offset);
        if (consumed != 8) break;
        pos += 8;
        
        uint32_t size;
        consumed = decode_fixed32(index_data + pos, &size);
        if (consumed != 4) break;
        pos += 4;
        
        uint32_t key_len;
        if (pos + 4 > sst->index_size) break;
        decode_fixed32(index_data + pos, &key_len);
        pos += 4;
        
        if (pos + key_len > sst->index_size) break;
        
        int cmp = memcmp(index_data + pos, key, key_len < klen ? key_len : klen);
        /* 前缀匹配时，较短的 key 更小 */
        if (cmp == 0) {
            cmp = (key_len < klen) ? -1 : (key_len > klen) ? 1 : 0;
        }
        if (cmp >= 0) {
            target_offset = offset;
            break;
        }
        
        target_offset = offset;
        pos += key_len;
    }
    
    if (index_data != sst->cached_index_data) {
        kv_free(index_data);
    }
    index_data = NULL;
    
    if (fseek(sst->file, (long)target_offset, SEEK_SET) != 0) {
        return -1;
    }
    
    sstable_block_t block;
    if (sstable_read_block(sst, NULL, &block, block_cache) != 0) return -1;
    
    int ret = sstable_block_lookup(&block, key, klen, out_value, out_vlen);
    sstable_block_free(&block);
    
    return ret;
}

sstable_iter_t* sstable_new_iterator(sstable_t* sst) {
    if (!sst) return NULL;
    
    sstable_iter_t* iter = kv_malloc(sizeof(sstable_iter_t));
    if (!iter) return NULL;
    
    iter->sst = sst;
    iter->current_block = NULL;
    iter->block_offset = 0;
    iter->entry_offset = 0;
    iter->eof = 0;
    memset(iter->prev_key, 0, sizeof(iter->prev_key));
    iter->prev_len = 0;
    
    if (fseek(sst->file, 0, SEEK_SET) != 0) {
        kv_free(iter);
        return NULL;
    }
    
    return iter;
}

void sstable_iter_free(sstable_iter_t* iter) {
    if (!iter) return;
    
    if (iter->current_block) {
        sstable_block_free(iter->current_block);
        kv_free(iter->current_block);
    }
    kv_free(iter);
}

int sstable_iter_next(sstable_iter_t* iter, char** key, size_t* klen, char** value, size_t* vlen) {
    if (!iter || !key || !klen || !value || !vlen) return -1;
    
    if (iter->eof) return -1;
    
    if (!iter->current_block) {
        /* 读取并解压第一个数据块 */
        iter->current_block = kv_malloc(sizeof(sstable_block_t));
        if (!iter->current_block) {
            iter->eof = 1;
            return -1;
        }
        
        if (sstable_read_block(iter->sst, NULL, iter->current_block, NULL) != 0) {
            kv_free(iter->current_block);
            iter->current_block = NULL;
            iter->eof = 1;
            return -1;
        }
        
        iter->entry_offset = 0;
    }
    
    if (iter->entry_offset >= iter->current_block->size - iter->current_block->restart_count * 4 - 8) {
        sstable_block_free(iter->current_block);
        kv_free(iter->current_block);
        iter->current_block = NULL;
        
        if (ftell(iter->sst->file) >= (long)iter->sst->index_offset) {
            iter->eof = 1;
            return -1;
        }
        
        /* 读取并解压下一个数据块 */
        iter->current_block = kv_malloc(sizeof(sstable_block_t));
        if (!iter->current_block) {
            iter->eof = 1;
            return -1;
        }
        
        if (sstable_read_block(iter->sst, NULL, iter->current_block, NULL) != 0) {
            kv_free(iter->current_block);
            iter->current_block = NULL;
            iter->eof = 1;
            return -1;
        }
        
        iter->entry_offset = 0;
        /* 不重置 prev_len：块间共享 prev_key，确保跨块前缀解码正确 */
    }
    
    sstable_entry_t entry;
    if (sstable_entry_decode(iter->current_block, iter->entry_offset, &entry, iter->prev_key, &iter->prev_len) != 0) {
        iter->eof = 1;
        return -1;
    }
    
    *key = entry.key;
    *klen = entry.shared_len + entry.unshared_len;
    *value = entry.value;
    *vlen = entry.value_len;
    
    uint8_t* ptr = iter->current_block->data + iter->entry_offset;
    size_t remaining = iter->current_block->size - iter->entry_offset;
    
    uint64_t shared_len;
    size_t consumed = decode_varint(ptr, remaining, &shared_len);
    ptr += consumed;
    remaining -= consumed;
    
    uint64_t unshared_len;
    consumed = decode_varint(ptr, remaining, &unshared_len);
    ptr += consumed;
    remaining -= consumed;
    
    uint64_t value_len;
    consumed = decode_varint(ptr, remaining, &value_len);
    ptr += consumed;
    remaining -= consumed;
    
    iter->entry_offset += (ptr - (iter->current_block->data + iter->entry_offset)) + (size_t)unshared_len + (size_t)value_len + 4;
    
    return 0;
}

const char* sstable_smallest_key(sstable_t* sst, size_t* out_len) {
    if (!sst || !out_len) return NULL;
    
    if (!sst->smallest_key) {
        sstable_iter_t* iter = sstable_new_iterator(sst);
        if (!iter) return NULL;
        
        char* key = NULL;
        size_t klen = 0;
        char* value = NULL;
        size_t vlen = 0;
        
        if (sstable_iter_next(iter, &key, &klen, &value, &vlen) == 0) {
            sst->smallest_key = key;
            sst->smallest_key_len = klen;
        }
        kv_free(value);
        sstable_iter_free(iter);
    }
    
    *out_len = sst->smallest_key_len;
    return sst->smallest_key;
}

const char* sstable_largest_key(sstable_t* sst, size_t* out_len) {
    if (!sst || !out_len) return NULL;
    
    if (!sst->largest_key) {
        sstable_iter_t* iter = sstable_new_iterator(sst);
        if (!iter) return NULL;
        
        char* key = NULL;
        size_t klen = 0;
        char* value = NULL;
        size_t vlen = 0;
        
        while (sstable_iter_next(iter, &key, &klen, &value, &vlen) == 0) {
            kv_free(sst->largest_key);
            sst->largest_key = key;
            sst->largest_key_len = klen;
            kv_free(value);
        }
        
        sstable_iter_free(iter);
    }
    
    *out_len = sst->largest_key_len;
    return sst->largest_key;
}