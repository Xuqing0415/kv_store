/**
 * concurrent_example.c — KV Store 多线程并发读写演示
 *
 * 编译: gcc -I../include -L../build_win concurrent_example.c -lkv_store -o concurrent_example
 * 运行: ./concurrent_example
 */

#include "kv_store.h"
#include "mem.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#include <process.h>
#define THREAD_RET unsigned __stdcall
#define THREAD_ARG void*
#define atomic_inc(p) InterlockedIncrement((LONG volatile*)(p))
#define atomic_dec(p) InterlockedDecrement((LONG volatile*)(p))
typedef HANDLE thread_t;
#else
#include <pthread.h>
#include <unistd.h>
#define THREAD_RET void*
#define THREAD_ARG void*
#define atomic_inc(p) __sync_fetch_and_add((p), 1)
#define atomic_dec(p) __sync_fetch_and_sub((p), 1)
typedef pthread_t thread_t;
#endif

#define NUM_THREADS  4
#define NUM_KEYS     500
#define OPS_PER_THREAD 2000

static volatile long g_put_ops = 0;
static volatile long g_get_ops = 0;
static volatile long g_del_ops = 0;
static volatile long g_get_hits = 0;
static volatile long g_get_misses = 0;
static volatile long g_running = 1;

static kv_store_t* g_db = NULL;

static THREAD_RET worker_thread(THREAD_ARG arg) {
    int thread_id = (int)(intptr_t)arg;
    char key[64], value[128];

    printf("  Worker %d started\n", thread_id);

    for (int i = 0; i < OPS_PER_THREAD; i++) {
        int op = rand() % 100;
        int key_idx = rand() % NUM_KEYS;
        snprintf(key, sizeof(key), "concurrent_key_%d", key_idx);

        if (op < 50) {
            /* 50% PUT */
            snprintf(value, sizeof(value), "concurrent_val_%d_from_thread_%d", key_idx, thread_id);
            kv_put(g_db, key, strlen(key), value, strlen(value));
            atomic_inc(&g_put_ops);
        } else if (op < 85) {
            /* 35% GET */
            char* val = NULL;
            size_t vlen = 0;
            if (kv_get(g_db, key, strlen(key), &val, &vlen) == 0) {
                atomic_inc(&g_get_hits);
                kv_free(val);
            } else {
                atomic_inc(&g_get_misses);
            }
            atomic_inc(&g_get_ops);
        } else {
            /* 15% DELETE */
            kv_delete(g_db, key, strlen(key));
            atomic_inc(&g_del_ops);
        }
    }

    printf("  Worker %d finished\n", thread_id);
#ifdef _WIN32
    _endthreadex(0);
#endif
    return 0;
}

int main(void) {
    const char* data_dir = "./example_concurrent_db";

    printf("========================================\n");
    printf("  KV Store — Concurrent Example\n");
    printf("========================================\n\n");

    printf("[1] Opening database...\n");
    g_db = kv_open(data_dir);
    if (!g_db) {
        fprintf(stderr, "ERROR: Failed to open database\n");
        return 1;
    }
    printf("    Database opened.\n\n");

    printf("[2] Launching %d worker threads (%d ops each)...\n",
           NUM_THREADS, OPS_PER_THREAD);

    thread_t threads[NUM_THREADS];

    for (int i = 0; i < NUM_THREADS; i++) {
#ifdef _WIN32
        threads[i] = (HANDLE)_beginthreadex(NULL, 0, worker_thread,
                                             (void*)(intptr_t)i, 0, NULL);
#else
        pthread_create(&threads[i], NULL, worker_thread, (void*)(intptr_t)i);
#endif
    }

    /* 等待所有线程完成 */
    for (int i = 0; i < NUM_THREADS; i++) {
#ifdef _WIN32
        WaitForSingleObject(threads[i], INFINITE);
        CloseHandle(threads[i]);
#else
        pthread_join(threads[i], NULL);
#endif
    }

    printf("\n[3] Results:\n");
    printf("    PUT ops:    %ld\n", g_put_ops);
    printf("    GET ops:    %ld (hits: %ld, misses: %ld)\n",
           g_get_ops, g_get_hits, g_get_misses);
    printf("    DELETE ops: %ld\n", g_del_ops);
    printf("    Total ops:  %ld\n\n",
           g_put_ops + g_get_ops + g_del_ops);

    /* 扫描验证 */
    printf("[4] Scanning all remaining keys...\n");
    kv_iter_t* iter = kv_scan(g_db, NULL, 0, NULL, 0);
    int remaining = 0;
    if (iter) {
        char* key = NULL, *value = NULL;
        size_t klen = 0, vlen = 0;
        while (kv_iter_next(iter, &key, &klen, &value, &vlen) == 0) {
            remaining++;
            kv_free(key);
            kv_free(value);
        }
        kv_iter_free(iter);
    }
    printf("    Remaining keys: %d\n\n", remaining);

    printf("[5] Closing database...\n");
    kv_close(g_db);
    printf("    Done.\n\n");

    printf("========================================\n");
    printf("  Concurrent test completed!\n");
    printf("========================================\n");

    return 0;
}