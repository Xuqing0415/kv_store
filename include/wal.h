#ifndef WAL_H
#define WAL_H

#include <stddef.h>
#include <stdio.h>

#define WAL_HEADER_SIZE 4
#define WAL_CRC_SIZE 4

/* WAL 滚动阈值：100MB */
#define WAL_MAX_SIZE (100 * 1024 * 1024)

typedef enum {
    WAL_PUT = 1,
    WAL_DELETE = 2,
} wal_record_type_t;

typedef struct wal_record {
    wal_record_type_t type;
    char* key;
    size_t key_len;
    char* value;
    size_t value_len;
} wal_record_t;

/* ===== 底层 WAL 文件操作（保留向后兼容） ===== */

typedef struct wal {
    FILE* file;
    char* path;
    size_t sync_interval;
    size_t bytes_since_sync;
} wal_t;

wal_t* wal_open(const char* path);
void wal_close(wal_t* w);
int wal_write(wal_t* w, wal_record_type_t type, const char* key, size_t klen, const char* value, size_t vlen);
int wal_sync(wal_t* w);
int wal_replay(wal_t* w, int (*callback)(wal_record_t* record, void* arg), void* arg);
int wal_truncate(wal_t* w, size_t offset);
size_t wal_size(wal_t* w);

/* ===== WAL 管理器：滚动与归档 ===== */

typedef struct wal_mgr wal_mgr_t;

/* 打开 WAL 管理器：扫描目录中已有 WAL 文件，打开最新一个（或创建新文件） */
wal_mgr_t* wal_mgr_open(const char* dir_path);

/* 关闭 WAL 管理器 */
void wal_mgr_close(wal_mgr_t* wm);

/* 写入 WAL 记录，超过阈值自动滚动到新文件 */
int wal_mgr_write(wal_mgr_t* wm, wal_record_type_t type, const char* key, size_t klen, const char* value, size_t vlen);

/* 同步当前 WAL 到磁盘 */
int wal_mgr_sync(wal_mgr_t* wm);

/* 重放所有 WAL 文件（按序号顺序），用于崩溃恢复 */
int wal_mgr_replay(wal_mgr_t* wm, int (*callback)(wal_record_t* record, void* arg), void* arg);

/* 归档：MemTable 刷盘后调用，删除旧 WAL 文件，创建新文件 */
int wal_mgr_archive(wal_mgr_t* wm);

/* 获取当前活跃 WAL 文件大小 */
size_t wal_mgr_active_size(wal_mgr_t* wm);

/* 获取当前 WAL 序列号 */
int wal_mgr_seq(wal_mgr_t* wm);

#endif