#ifndef VSR_IO_CRC32C_H
#define VSR_IO_CRC32C_H

#include <stddef.h>
#include <stdint.h>

/*
 * CRC32C (Castagnoli): reflected polynomial 0x82F63B78, initial value and
 * final XOR 0xFFFFFFFF, as in iSCSI, ext4 and SCTP. crc is the value returned
 * for the preceding bytes, or 0 to start, so that
 * vsr_io_crc32c(vsr_io_crc32c(0, a, n), b, m) is the checksum of a followed by
 * b. data may be NULL only when size is zero. Pure and thread-safe.
 */
uint32_t vsr_io_crc32c(uint32_t crc, const void *data, size_t size);

/* The table-driven path vsr_io_crc32c takes on a CPU without a CRC32C
 * instruction; exposed so that tests compare both paths. */
uint32_t vsr_io_crc32c_portable(uint32_t crc, const void *data, size_t size);

#endif /* VSR_IO_CRC32C_H */
