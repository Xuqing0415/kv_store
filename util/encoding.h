#ifndef ENCODING_H
#define ENCODING_H

#include <stddef.h>
#include <stdint.h>

size_t encode_varint(uint64_t v, uint8_t* dst);
size_t decode_varint(const uint8_t* src, size_t len, uint64_t* v);
size_t encode_fixed32(uint32_t v, uint8_t* dst);
size_t decode_fixed32(const uint8_t* src, uint32_t* v);
size_t encode_fixed64(uint64_t v, uint8_t* dst);
size_t decode_fixed64(const uint8_t* src, uint64_t* v);

#endif