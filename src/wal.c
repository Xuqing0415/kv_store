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
#include <dirent.h>
#endif

/* ===== WAL 管理器实现 ===== */

#define WAL_FILE_PREFIX "wal_"
#define WAL_FILE_SUFFIX ".log"
#define WAL_SEQ_DIGITS  6

struct wal_mgr {
    wal_t* active;          /* 当前活跃的 WAL 文件 */
    char*  dir_path;        /* WAL 文件目录 */
    int    seq;             /* 当前序列号 */
    size_t max_size;        /* 滚动阈值（字节） */
};

/* 构造 WAL 文件路径 */
static void wal_mgr_make_path(wal_mgr_t* wm, int seq, char* buf, size_t buf_size) {
    snprintf(buf, buf_size, "%s/" WAL_FILE_PREFIX "%0*d" WAL_FILE_SUFFIX,
             wm->dir_path, WAL_SEQ_DIGITS, seq);
}

/* 扫描目录，找到所有 WAL 文件的最大序列号，返回 -1 表示没有文件 */
static int wal_mgr_scan_dir(const char* dir_path) {
    int max_seq = -1;

#ifdef _WIN32
    char pattern[512];
    snprintf(pattern, sizeof(pattern), "%s/" WAL_FILE_PREFIX "*" WAL_FILE_SUFFIX, dir_path);

    WIN32_FIND_DATAA fd;
    HANDLE hFind = FindFirstFileA(pattern, &fd);
    if (hFind == INVALID_HANDLE_VALUE) return -1;

    do {
        /* 从文件名中提取序列号：wal_XXXXXX.log */
        const char* name = fd.cFileName;
        const char* prefix_pos = strstr(name, WAL_FILE_PREFIX);
        if (!prefix_pos) continue;
        prefix_pos += strlen(WAL_FILE_PREFIX);

        int seq = 0;
        int digits = 0;
        while (*prefix_pos >= '0' && *prefix_pos <= '9' && digits < WAL_SEQ_DIGITS) {
            seq = seq * 10 + (*prefix_pos - '0');
            prefix_pos++;
            digits++;
        }
        if (digits == WAL_SEQ_DIGITS && seq > max_seq) {
            max_seq = seq;
        }
    } while (FindNextFileA(hFind, &fd));

    FindClose(hFind);
#else
    DIR* dir = opendir(dir_path);
    if (!dir) return -1;

    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        const char* name = entry->d_name;
        if (strncmp(name, WAL_FILE_PREFIX, strlen(WAL_FILE_PREFIX)) != 0) continue;

        const char* p = name + strlen(WAL_FILE_PREFIX);
        /* 检查后缀 */
        const char* suffix = strstr(p, WAL_FILE_SUFFIX);
        if (!suffix) continue;

        int seq = 0;
        int digits = 0;
        while (p < suffix && *p >= '0' && *p <= '9' && digits < WAL_SEQ_DIGITS) {
            seq = seq * 10 + (*p - '0');
            p++;
            digits++;
        }
        if (digits == WAL_SEQ_DIGITS && seq > max_seq) {
            max_seq = seq;
        }
    }
    closedir(dir);
#endif

    return max_seq;
}

/* 删除所有旧 WAL 文件（不包括当前序列号） */
static void wal_mgr_delete_all_old(wal_mgr_t* wm) {
    int max_seq = wal_mgr_scan_dir(wm->dir_path);
    if (max_seq < 0) return;

    for (int seq = 0; seq < max_seq; seq++) {
        char path[512];
        wal_mgr_make_path(wm, seq, path, sizeof(path));

        /* 检查文件是否存在再删除 */
        FILE* f = fopen(path, "rb");
        if (f) {
            fclose(f);
            remove(path);
            printf("[WAL] GC archived WAL: %s\n", path);
        }
    }
}

wal_mgr_t* wal_mgr_open(const char* dir_path) {
    if (!dir_path) return NULL;

    wal_mgr_t* wm = kv_malloc(sizeof(wal_mgr_t));
    if (!wm) return NULL;

    wm->dir_path = kv_strdup(dir_path);
    if (!wm->dir_path) {
        kv_free(wm);
        return NULL;
    }

    wm->max_size = WAL_MAX_SIZE;
    wm->active = NULL;

    /* 扫描已有 WAL 文件 */
    int max_seq = wal_mgr_scan_dir(dir_path);

    if (max_seq >= 0) {
        /* 已有 WAL 文件，打开最新的继续追加 */
        wm->seq = max_seq;
        char path[512];
        wal_mgr_make_path(wm, wm->seq, path, sizeof(path));
        wm->active = wal_open(path);
        printf("[WAL] Found existing WAL: %s (seq=%d, size=%zu)\n",
               path, wm->seq, wm->active ? wal_size(wm->active) : 0);
    } else {
        /* 没有 WAL 文件，创建新文件 */
        wm->seq = 0;
        char path[512];
        wal_mgr_make_path(wm, wm->seq, path, sizeof(path));
        wm->active = wal_open(path);
        printf("[WAL] Created new WAL: %s (seq=%d)\n", path, wm->seq);
    }

    if (!wm->active) {
        kv_free(wm->dir_path);
        kv_free(wm);
        return NULL;
    }

    return wm;
}

void wal_mgr_close(wal_mgr_t* wm) {
    if (!wm) return;

    if (wm->active) {
        wal_close(wm->active);
        wm->active = NULL;
    }

    kv_free(wm->dir_path);
    kv_free(wm);
}

int wal_mgr_write(wal_mgr_t* wm, wal_record_type_t type, const char* key, size_t klen, const char* value, size_t vlen) {
    if (!wm || !wm->active) return -1;

    int ret = wal_write(wm->active, type, key, klen, value, vlen);
    if (ret != 0) return ret;

    /* 检查是否需要滚动 */
    size_t current_size = wal_size(wm->active);
    if (current_size >= wm->max_size) {
        printf("[WAL] Size %zu >= threshold %zu, rolling to new file\n",
               current_size, wm->max_size);

        wal_close(wm->active);
        wm->active = NULL;

        wm->seq++;
        char path[512];
        wal_mgr_make_path(wm, wm->seq, path, sizeof(path));
        wm->active = wal_open(path);

        if (!wm->active) return -1;

        printf("[WAL] Rolled to new WAL: %s (seq=%d)\n", path, wm->seq);
    }

    return 0;
}

int wal_mgr_sync(wal_mgr_t* wm) {
    if (!wm || !wm->active) return -1;
    return wal_sync(wm->active);
}

int wal_mgr_replay(wal_mgr_t* wm, int (*callback)(wal_record_t* record, void* arg), void* arg) {
    if (!wm || !callback) return -1;

    /* 扫描所有 WAL 文件，按序列号从小到大重放 */
    int max_seq = wal_mgr_scan_dir(wm->dir_path);
    if (max_seq < 0) {
        printf("[WAL] No WAL files found, skipping replay\n");
        return 0;
    }

    int total_records = 0;
    for (int seq = 0; seq <= max_seq; seq++) {
        char path[512];
        wal_mgr_make_path(wm, seq, path, sizeof(path));

        /* 检查文件是否存在 */
        FILE* test = fopen(path, "rb");
        if (!test) continue;
        fclose(test);

        wal_t* w = wal_open(path);
        if (!w) {
            printf("[WAL] Warning: Could not open %s for replay\n", path);
            continue;
        }

        printf("[WAL] Replaying: %s\n", path);
        int ret = wal_replay(w, callback, arg);
        wal_close(w);

        if (ret != 0) {
            printf("[WAL] Replay of %s failed\n", path);
            return ret;
        }
        total_records++;
    }

    printf("[WAL] Replay complete: %d WAL files processed\n", total_records);
    return 0;
}

int wal_mgr_archive(wal_mgr_t* wm) {
    if (!wm) return -1;

    /* 1. 关闭并删除当前活跃 WAL（数据已刷入 SSTable） */
    if (wm->active) {
        char old_path[512];
        wal_mgr_make_path(wm, wm->seq, old_path, sizeof(old_path));

        wal_close(wm->active);
        wm->active = NULL;

        if (remove(old_path) == 0) {
            printf("[WAL] Archived: %s (data flushed to SSTable)\n", old_path);
        } else {
            printf("[WAL] Warning: Could not remove %s\n", old_path);
        }
    }

    /* 2. 清理所有更旧的 WAL 文件 */
    wal_mgr_delete_all_old(wm);

    /* 3. 创建新的 WAL 文件 */
    wm->seq++;
    char new_path[512];
    wal_mgr_make_path(wm, wm->seq, new_path, sizeof(new_path));
    wm->active = wal_open(new_path);

    if (!wm->active) {
        printf("[WAL] Error: Failed to create new WAL: %s\n", new_path);
        return -1;
    }

    printf("[WAL] New WAL created: %s (seq=%d)\n", new_path, wm->seq);
    return 0;
}

size_t wal_mgr_active_size(wal_mgr_t* wm) {
    if (!wm || !wm->active) return 0;
    return wal_size(wm->active);
}

int wal_mgr_seq(wal_mgr_t* wm) {
    if (!wm) return -1;
    return wm->seq;
}

/* ===== 底层 WAL 文件操作（保持不变） ===== */

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