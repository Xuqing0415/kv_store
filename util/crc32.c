#include "crc32.h"

static uint32_t crc32_table[256];

static void crc32_init(void) {
    static int initialized = 0;
    if (initialized) return;
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++) {
            c = (c & 1) ? (0xedb88320 ^ (c >> 1)) : (c >> 1);
        }
        crc32_table[i] = c;
    }
    initialized = 1;
}

uint32_t crc32(const uint8_t* data, size_t len) {
    crc32_init();
    uint32_t crc = 0xffffffff;
    for (size_t i = 0; i < len; i++) {
        crc = crc32_table[(crc ^ data[i]) & 0xff] ^ (crc >> 8);
    }
    return crc ^ 0xffffffff;
}

uint32_t crc32_combine(uint32_t crc1, uint32_t crc2, size_t len2) {
    uint32_t crc = crc1;
    for (size_t i = 0; i < len2; i++) {
        crc = crc32_table[(crc ^ 0) & 0xff] ^ (crc >> 8);
    }
    return crc ^ crc2;
}