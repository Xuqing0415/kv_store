#include "resp_server.h"
#include "kv_store.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <signal.h>
#endif

static resp_server_t* g_server = NULL;

#ifdef _WIN32
static BOOL WINAPI signal_handler(DWORD ctrl_type) {
    (void)ctrl_type;
    if (g_server) resp_server_stop(g_server);
    return TRUE;
}
#else
static void signal_handler(int sig) {
    (void)sig;
    if (g_server) resp_server_stop(g_server);
}
#endif

static void print_usage(const char* prog) {
    printf("Usage: %s [options]\n", prog);
    printf("Options:\n");
    printf("  -h <host>   Bind address (default: 127.0.0.1)\n");
    printf("  -p <port>   Listen port (default: 6379)\n");
    printf("  -d <dir>    Data directory (default: ./data)\n");
    printf("  --help      Show this help\n");
}

int main(int argc, char* argv[]) {
    const char* host = "127.0.0.1";
    int port = RESP_DEFAULT_PORT;
    const char* data_dir = "./data";

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-h") == 0 && i + 1 < argc) {
            host = argv[++i];
        } else if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
            port = atoi(argv[++i]);
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
    printf("Bind:          %s:%d\n\n", host, port);

    kv_store_t* db = kv_open(data_dir);
    if (!db) {
        fprintf(stderr, "Failed to open database at %s\n", data_dir);
        return 1;
    }

    if (resp_server_start(&g_server, host, port, db) != 0) {
        fprintf(stderr, "Failed to start RESP server\n");
        kv_close(db);
        return 1;
    }

#ifdef _WIN32
    SetConsoleCtrlHandler(signal_handler, TRUE);
#else
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
#endif

    printf("Connect with: redis-cli -h %s -p %d\n", host, port);
    printf("Press Ctrl+C to stop.\n\n");

    resp_server_run(g_server);

    printf("\n[INFO] Shutting down...\n");
    resp_server_free(g_server);
    kv_close(db);
    printf("[INFO] Server stopped.\n");

    return 0;
}