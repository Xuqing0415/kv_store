#include "bloom_filter.h"
#include "mem.h"
#include <math.h>
#include <string.h>

static uint32_t hash1(const char* key, size_t len) {
    uint32_t h = 0x811c9dc5;
    for (size_t i = 0; i < len; i++) {
        h ^= key[i];
        h *= 0x01000193;
    }
    return h;
}

static uint32_t hash2(const char* key, size_t len) {
    uint32_t h = 0;
    for (size_t i = 0; i < len; i++) {
        h = h * 33 + key[i];
    }
    return h;
}

static size_t bloom_filter_compute_bits(size_t expected_keys, double false_positive_rate) {
    double ln2 = 0.69314718056;
    size_t bits = (size_t)(-expected_keys * log(false_positive_rate) / (ln2 * ln2));
    return (bits + 7) & ~7;
}

static size_t bloom_filter_compute_hashes(size_t bits, size_t expected_keys) {
    double ln2 = 0.69314718056;
    size_t hashes = (size_t)(bits * ln2 / expected_keys);
    return hashes < 1 ? 1 : hashes;
}

bloom_filter_t* bloom_filter_new(size_t expected_keys, double false_positive_rate) {
    if (expected_keys == 0 || false_positive_rate >= 1.0) return NULL;
    
    size_t bits_len = bloom_filter_compute_bits(expected_keys, false_positive_rate);
    size_t num_hash = bloom_filter_compute_hashes(bits_len, expected_keys);
    
    bloom_filter_t* bf = kv_malloc(sizeof(bloom_filter_t));
    if (!bf) return NULL;
    
    bf->bits_len = bits_len;
    bf->num_hash = num_hash;
    bf->bits = kv_calloc((bits_len + 7) / 8, 1);
    if (!bf->bits) {
        kv_free(bf);
        return NULL;
    }
    
    return bf;
}

void bloom_filter_free(bloom_filter_t* bf) {
    if (!bf) return;
    kv_free(bf->bits);
    kv_free(bf);
}

void bloom_filter_add(bloom_filter_t* bf, const char* key, size_t klen) {
    if (!bf || !key) return;
    
    uint32_t h1 = hash1(key, klen);
    uint32_t h2 = hash2(key, klen);
    
    for (size_t i = 0; i < bf->num_hash; i++) {
        uint32_t h = h1 + i * h2;
        size_t bit_pos = h % bf->bits_len;
        bf->bits[bit_pos / 8] |= (1 << (bit_pos % 8));
    }
}

int bloom_filter_may_contain(bloom_filter_t* bf, const char* key, size_t klen) {
    if (!bf || !key) return 0;
    
    uint32_t h1 = hash1(key, klen);
    uint32_t h2 = hash2(key, klen);
    
    for (size_t i = 0; i < bf->num_hash; i++) {
        uint32_t h = h1 + i * h2;
        size_t bit_pos = h % bf->bits_len;
        if (!(bf->bits[bit_pos / 8] & (1 << (bit_pos % 8)))) {
            return 0;
        }
    }
    return 1;
}

size_t bloom_filter_serialized_size(bloom_filter_t* bf) {
    if (!bf) return 0;
    size_t bytes = (bf->bits_len + 7) / 8;
    return 8 + 4 + bytes;
}

size_t bloom_filter_serialize(bloom_filter_t* bf, uint8_t* buf, size_t buf_len) {
    if (!bf || !buf) return 0;
    
    size_t expected = bloom_filter_serialized_size(bf);
    if (buf_len < expected) return 0;
    
    size_t bytes = (bf->bits_len + 7) / 8;
    
    for (int i = 0; i < 8; i++) {
        buf[i] = (bf->bits_len >> (8 * i)) & 0xff;
    }
    for (int i = 0; i < 4; i++) {
        buf[8 + i] = (bf->num_hash >> (8 * i)) & 0xff;
    }
    memcpy(buf + 12, bf->bits, bytes);
    
    return expected;
}

bloom_filter_t* bloom_filter_deserialize(const uint8_t* buf, size_t buf_len) {
    if (!buf || buf_len < 12) return NULL;
    
    bloom_filter_t* bf = kv_malloc(sizeof(bloom_filter_t));
    if (!bf) return NULL;
    
    bf->bits_len = 0;
    for (int i = 7; i >= 0; i--) {
        bf->bits_len = (bf->bits_len << 8) | buf[i];
    }
    
    bf->num_hash = 0;
    for (int i = 3; i >= 0; i--) {
        bf->num_hash = (bf->num_hash << 8) | buf[8 + i];
    }
    
    size_t bytes = (bf->bits_len + 7) / 8;
    if (buf_len < 12 + bytes) {
        kv_free(bf);
        return NULL;
    }
    
    bf->bits = kv_malloc(bytes);
    if (!bf->bits) {
        kv_free(bf);
        return NULL;
    }
    memcpy(bf->bits, buf + 12, bytes);
    
    return bf;
}