#include "wal.h"
#include "crc32.h"
#include "encoding.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#include <io.h>
static int fsync_win(int fd) {
    HANDLE h = (HANDLE)_get_osfhandle(fd);
    if (h == INVALID_HANDLE_VALUE) return -1;
    return FlushFileBuffers(h) ? 0 : -1;
}
#define fsync(fd) fsync_win(fd)

static int ftruncate_win(int fd, off_t length) {
    HANDLE h = (HANDLE)_get_osfhandle(fd);
    if (h == INVALID_HANDLE_VALUE) return -1;
    if (SetFilePointer(h, (LONG)length, NULL, FILE_BEGIN) == INVALID_SET_FILE_POINTER) return -1;
    return SetEndOfFile(h) ? 0 : -1;
}
#define ftruncate(fd, len) ftruncate_win(fd, len)
#else
#include <unistd.h>
#endif

wal_t* wal_open(const char* path) {
    if (!path) return NULL;
    
    wal_t* w = kv_malloc(sizeof(wal_t));
    if (!w) return NULL;
    
    w->path = kv_strdup(path);
    if (!w->path) {
        kv_free(w);
        return NULL;
    }
    
    w->file = fopen(path, "ab+");
    if (!w->file) {
        kv_free(w->path);
        kv_free(w);
        return NULL;
    }
    
    w->sync_interval = 1024 * 1024;
    w->bytes_since_sync = 0;
    
    return w;
}

void wal_close(wal_t* w) {
    if (!w) return;
    
    if (w->file) {
        fclose(w->file);
    }
    kv_free(w->path);
    kv_free(w);
}

int wal_write(wal_t* w, wal_record_type_t type, const char* key, size_t klen, const char* value, size_t vlen) {
    if (!w || !w->file || !key || klen == 0) return -1;
    
    size_t total_size = WAL_HEADER_SIZE + 1 + 4 + klen + 4 + vlen + WAL_CRC_SIZE;
    uint8_t* buf = kv_malloc(total_size);
    if (!buf) return -1;
    
    uint8_t* ptr = buf;
    
    ptr += encode_fixed32((uint32_t)(total_size - WAL_HEADER_SIZE), ptr);
    
    *ptr++ = (uint8_t)type;
    
    ptr += encode_fixed32((uint32_t)klen, ptr);
    memcpy(ptr, key, klen);
    ptr += klen;
    
    ptr += encode_fixed32((uint32_t)vlen, ptr);
    if (value) {
        memcpy(ptr, value, vlen);
    }
    ptr += vlen;
    
    uint32_t crc = crc32(buf + WAL_HEADER_SIZE, total_size - WAL_HEADER_SIZE - WAL_CRC_SIZE);
    ptr += encode_fixed32(crc, ptr);
    
    size_t written = fwrite(buf, 1, total_size, w->file);
    kv_free(buf);
    
    if (written != total_size) {
        return -1;
    }
    
    w->bytes_since_sync += total_size;
    if (w->bytes_since_sync >= w->sync_interval) {
        wal_sync(w);
    }
    
    return 0;
}

int wal_sync(wal_t* w) {
    if (!w || !w->file) return -1;
    
    if (fflush(w->file) != 0) return -1;
    if (fsync(fileno(w->file)) != 0) return -1;
    
    w->bytes_since_sync = 0;
    return 0;
}

int wal_replay(wal_t* w, int (*callback)(wal_record_t* record, void* arg), void* arg) {
    if (!w || !w->file || !callback) return -1;
    
    if (fseek(w->file, 0, SEEK_SET) != 0) return -1;
    
    while (1) {
        uint32_t record_len;
        if (fread(&record_len, 4, 1, w->file) != 1) {
            if (feof(w->file)) break;
            return -1;
        }
        
        size_t total_len = record_len + WAL_HEADER_SIZE;
        uint8_t* buf = kv_malloc(total_len);
        if (!buf) return -1;
        
        encode_fixed32(record_len, buf);
        
        if (fread(buf + WAL_HEADER_SIZE, 1, record_len, w->file) != record_len) {
            kv_free(buf);
            if (feof(w->file)) break;
            return -1;
        }
        
        uint32_t stored_crc;
        decode_fixed32(buf + total_len - WAL_CRC_SIZE, &stored_crc);
        
        uint32_t computed_crc = crc32(buf + WAL_HEADER_SIZE, record_len - WAL_CRC_SIZE);
        if (stored_crc != computed_crc) {
            kv_free(buf);
            break;
        }
        
        wal_record_t record;
        uint8_t* ptr = buf + WAL_HEADER_SIZE;
        
        record.type = (wal_record_type_t)*ptr++;
        
        uint32_t klen;
        ptr += decode_fixed32(ptr, &klen);
        record.key = kv_malloc(klen);
        memcpy(record.key, ptr, klen);
        record.key_len = klen;
        ptr += klen;
        
        uint32_t vlen;
        ptr += decode_fixed32(ptr, &vlen);
        if (vlen > 0) {
            record.value = kv_malloc(vlen);
            memcpy(record.value, ptr, vlen);
        } else {
            record.value = NULL;
        }
        record.value_len = vlen;
        
        int ret = callback(&record, arg);
        
        kv_free(record.key);
        kv_free(record.value);
        kv_free(buf);
        
        if (ret != 0) return ret;
    }
    
    return 0;
}

int wal_truncate(wal_t* w, size_t offset) {
    if (!w || !w->file) return -1;
    
    if (fseek(w->file, offset, SEEK_SET) != 0) return -1;
    if (ftruncate(fileno(w->file), offset) != 0) return -1;
    
    return 0;
}

size_t wal_size(wal_t* w) {
    if (!w || !w->file) return 0;
    
    if (fseek(w->file, 0, SEEK_END) != 0) return 0;
    long pos = ftell(w->file);
    return pos >= 0 ? (size_t)pos : 0;
}