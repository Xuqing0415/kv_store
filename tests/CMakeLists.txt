add_executable(test_skiplist test_skiplist.c)
target_link_libraries(test_skiplist kv_store)

add_executable(test_wal test_wal.c)
target_link_libraries(test_wal kv_store)

add_executable(test_sstable test_sstable.c)
target_link_libraries(test_sstable kv_store)

add_executable(test_kv_store test_kv_store.c)
target_link_libraries(test_kv_store kv_store)

add_executable(test_debug test_debug.c)
target_link_libraries(test_debug kv_store)

add_executable(test_minimal2 test_minimal2.c)
target_link_libraries(test_minimal2 kv_store)

add_executable(benchmark benchmark.c)
target_link_libraries(benchmark kv_store)

add_executable(test_crash test_crash.c)
target_link_libraries(test_crash kv_store)

add_executable(test_iter_debug test_iter_debug.c)
target_link_libraries(test_iter_debug kv_store)

add_executable(test_wal_crash test_wal_crash.c)
target_link_libraries(test_wal_crash kv_store)

add_executable(test_tombstone_scan test_tombstone_scan.c)
target_link_libraries(test_tombstone_scan kv_store)

add_executable(test_concurrent test_concurrent.c)
target_link_libraries(test_concurrent kv_store)

add_executable(test_crash_recovery test_crash_recovery.c)
target_link_libraries(test_crash_recovery kv_store)

add_executable(test_memcheck test_memcheck.c)
target_link_libraries(test_memcheck kv_store)

add_executable(test_debug_lookup test_debug_lookup.c)
target_link_libraries(test_debug_lookup kv_store)

# ============================================================
# ctest 注册（此前缺失导致 ctest 空跑）
# ============================================================

# 独立单元测试
add_test(NAME test_skiplist COMMAND test_skiplist)
add_test(NAME test_wal COMMAND test_wal)
add_test(NAME test_sstable COMMAND test_sstable)
add_test(NAME test_kv_store COMMAND test_kv_store)
add_test(NAME test_debug COMMAND test_debug)
add_test(NAME test_minimal2 COMMAND test_minimal2)
add_test(NAME test_crash COMMAND test_crash)
add_test(NAME test_iter_debug COMMAND test_iter_debug)
add_test(NAME test_wal_crash COMMAND test_wal_crash)
add_test(NAME test_tombstone_scan COMMAND test_tombstone_scan)
# test_concurrent 并发压力极大，与其它测试并行时可能触发竞态，改为串行执行
add_test(NAME test_concurrent COMMAND test_concurrent)
set_tests_properties(test_concurrent PROPERTIES RUN_SERIAL TRUE)
add_test(NAME test_crash_recovery COMMAND test_crash_recovery)
add_test(NAME test_memcheck COMMAND test_memcheck)

# 注意：test_debug_lookup 是调试遗留工具，硬编码打开 ./test_db/8.sst（依赖 kv_test
# 先运行且目录必须全新），无法作为独立单元测试，故不注册到 ctest。

# 基准测试：耗时较长，用 label 标记，可用 ctest -L benchmark 单独跑
add_test(NAME benchmark COMMAND benchmark)
set_tests_properties(benchmark PROPERTIES LABELS "benchmark")