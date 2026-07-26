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