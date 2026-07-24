#ifndef BLOOM_FILTER_H
#define BLOOM_FILTER_H

#include <stddef.h>
#include <stdint.h>

typedef struct bloom_filter {
    uint8_t* bits;
    size_t bits_len;
    size_t num_hash;
} bloom_filter_t;

bloom_filter_t* bloom_filter_new(size_t expected_keys, double false_positive_rate);
void bloom_filter_free(bloom_filter_t* bf);
void bloom_filter_add(bloom_filter_t* bf, const char* key, size_t klen);
int bloom_filter_may_contain(bloom_filter_t* bf, const char* key, size_t klen);
size_t bloom_filter_serialize(bloom_filter_t* bf, uint8_t* buf, size_t buf_len);
bloom_filter_t* bloom_filter_deserialize(const uint8_t* buf, size_t buf_len);
size_t bloom_filter_serialized_size(bloom_filter_t* bf);

#endif