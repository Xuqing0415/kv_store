#include "resp_server.h"
#include "metrics_server.h"
#include "kv_store.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#include <process.h>
#else
#include <signal.h>
#include <pthread.h>
#endif

static resp_server_t* g_resp_server = NULL;
static metrics_server_t* g_metrics_server = NULL;

#ifdef _WIN32
static BOOL WINAPI signal_handler(DWORD ctrl_type) {
    (void)ctrl_type;
    if (g_resp_server) resp_server_stop(g_resp_server);
    if (g_metrics_server) metrics_server_stop(g_metrics_server);
    return TRUE;
}

static DWORD WINAPI metrics_thread_func(LPVOID arg) {
    metrics_server_t* server = (metrics_server_t*)arg;
    metrics_server_run(server);
    return 0;
}
#else
static void signal_handler(int sig) {
    (void)sig;
    if (g_resp_server) resp_server_stop(g_resp_server);
    if (g_metrics_server) metrics_server_stop(g_metrics_server);
}

static void* metrics_thread_func(void* arg) {
    metrics_server_t* server = (metrics_server_t*)arg;
    metrics_server_run(server);
    return NULL;
}
#endif

static void print_usage(const char* prog) {
    printf("Usage: %s [options]\n", prog);
    printf("Options:\n");
    printf("  -h <host>    Bind address (default: 127.0.0.1)\n");
    printf("  -p <port>    Listen port (default: 6379)\n");
    printf("  -m <port>    Metrics HTTP port (default: 9090, 0 to disable)\n");
    printf("  -d <dir>     Data directory (default: ./data)\n");
    printf("  --help       Show this help\n");
}

int main(int argc, char* argv[]) {
    const char* host = "127.0.0.1";
    int port = RESP_DEFAULT_PORT;
    int metrics_port = METRICS_DEFAULT_PORT;
    const char* data_dir = "./data";

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-h") == 0 && i + 1 < argc) {
            host = argv[++i];
        } else if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
            port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc) {
            metrics_port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-d") == 0 && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            print_usage(argv[0]);
            return 1;
        }
    }

    printf("========================================\n");
    printf("  KV Store Redis-compatible Server\n");
    printf("========================================\n");
    printf("Data directory: %s\n", data_dir);
    printf("Bind:          %s:%d\n", host, port);
    if (metrics_port > 0) {
        printf("Metrics:       http://%s:%d/metrics\n", host, metrics_port);
    } else {
        printf("Metrics:       disabled\n");
    }
    printf("\n");

    kv_store_t* db = kv_open(data_dir);
    if (!db) {
        fprintf(stderr, "Failed to open database at %s\n", data_dir);
        return 1;
    }

    /* 启动 RESP 服务器 */
    if (resp_server_start(&g_resp_server, host, port, db) != 0) {
        fprintf(stderr, "Failed to start RESP server\n");
        kv_close(db);
        return 1;
    }

    /* 启动 Metrics HTTP 服务器（后台线程） */
#ifdef _WIN32
    HANDLE metrics_thread = NULL;
#else
    pthread_t metrics_thread;
    int metrics_thread_started = 0;
#endif

    if (metrics_port > 0) {
        if (metrics_server_start(&g_metrics_server, host, metrics_port, db, NULL) != 0) {
            fprintf(stderr, "WARNING: Failed to start metrics server on port %d\n", metrics_port);
            g_metrics_server = NULL;
        } else {
#ifdef _WIN32
            metrics_thread = CreateThread(NULL, 0, metrics_thread_func, g_metrics_server, 0, NULL);
            if (!metrics_thread) {
                fprintf(stderr, "WARNING: Failed to create metrics thread\n");
                metrics_server_free(g_metrics_server);
                g_metrics_server = NULL;
            }
#else
            if (pthread_create(&metrics_thread, NULL, metrics_thread_func, g_metrics_server) == 0) {
                metrics_thread_started = 1;
            } else {
                fprintf(stderr, "WARNING: Failed to create metrics thread\n");
                metrics_server_free(g_metrics_server);
                g_metrics_server = NULL;
            }
#endif
        }
    }

#ifdef _WIN32
    SetConsoleCtrlHandler(signal_handler, TRUE);
#else
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
#endif

    printf("Connect with: redis-cli -h %s -p %d\n", host, port);
    if (metrics_port > 0 && g_metrics_server) {
        printf("Metrics at:   http://%s:%d/metrics\n", host, metrics_port);
    }
    printf("Press Ctrl+C to stop.\n\n");

    resp_server_run(g_resp_server);

    printf("\n[INFO] Shutting down...\n");

    /* 等待 metrics 线程退出 */
    if (g_metrics_server) {
#ifdef _WIN32
        if (metrics_thread) {
            WaitForSingleObject(metrics_thread, 5000);
            CloseHandle(metrics_thread);
        }
        metrics_server_free(g_metrics_server);
#else
        if (metrics_thread_started) {
            pthread_join(metrics_thread, NULL);
        }
        metrics_server_free(g_metrics_server);
#endif
    }

    resp_server_free(g_resp_server);
    kv_close(db);
    printf("[INFO] Server stopped.\n");

    return 0;
}