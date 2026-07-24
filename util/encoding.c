#include "encoding.h"

size_t encode_varint(uint64_t v, uint8_t* dst) {
    size_t i = 0;
    while (v >= 128) {
        dst[i++] = v | 0x80;
        v >>= 7;
    }
    dst[i++] = v;
    return i;
}

size_t decode_varint(const uint8_t* src, size_t len, uint64_t* v) {
    *v = 0;
    size_t i = 0;
    uint64_t shift = 0;
    while (i < len) {
        uint8_t b = src[i++];
        *v |= (uint64_t)(b & 0x7f) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
        if (shift >= 64) return 0;
    }
    return i;
}

size_t encode_fixed32(uint32_t v, uint8_t* dst) {
    dst[0] = v & 0xff;
    dst[1] = (v >> 8) & 0xff;
    dst[2] = (v >> 16) & 0xff;
    dst[3] = (v >> 24) & 0xff;
    return 4;
}

size_t decode_fixed32(const uint8_t* src, uint32_t* v) {
    *v = (uint32_t)src[0] |
         ((uint32_t)src[1] << 8) |
         ((uint32_t)src[2] << 16) |
         ((uint32_t)src[3] << 24);
    return 4;
}

size_t encode_fixed64(uint64_t v, uint8_t* dst) {
    for (int i = 0; i < 8; i++) {
        dst[i] = (v >> (8 * i)) & 0xff;
    }
    return 8;
}

size_t decode_fixed64(const uint8_t* src, uint64_t* v) {
    *v = 0;
    for (int i = 7; i >= 0; i--) {
        *v = (*v << 8) | src[i];
    }
    return 8;
}