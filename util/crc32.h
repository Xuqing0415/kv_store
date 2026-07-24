#ifndef CRC32_H
#define CRC32_H

#include <stddef.h>
#include <stdint.h>

uint32_t crc32(const uint8_t* data, size_t len);
uint32_t crc32_combine(uint32_t crc1, uint32_t crc2, size_t len2);

#endif