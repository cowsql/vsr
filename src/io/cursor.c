#include "config.h"

#include "io/cursor.h"

#include "io/crc32c.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/*
 * Invariant kept by every mutator: when position < length, `piece` names
 * the piece holding the next byte and offset < that piece's length; empty
 * pieces and fully consumed pieces are stepped over eagerly. At the end the
 * cursor rests on the last piece it could reach.
 */

static const struct vsr_io_piece *piece_at(const struct vsr_io_cursor *cursor,
                                           uint32_t index)
{
    return cursor->pieces != NULL ? &cursor->pieces[index] : &cursor->one;
}

static void cursor_settle(struct vsr_io_cursor *cursor)
{
    while (cursor->piece + 1 < cursor->count &&
           cursor->offset == piece_at(cursor, cursor->piece)->length) {
        cursor->piece++;
        cursor->offset = 0;
    }
}

/* Advances by size bytes; size <= remaining. */
static void cursor_advance(struct vsr_io_cursor *cursor, size_t size)
{
    cursor->position += size;
    while (size > 0) {
        size_t available =
            piece_at(cursor, cursor->piece)->length - cursor->offset;
        size_t step = size < available ? size : available;

        cursor->offset += step;
        size -= step;
        cursor_settle(cursor);
    }
}

/* Visits the next size bytes piece by piece without moving; size <=
 * remaining. Copies into out when non-NULL, else chains a CRC. */
static uint32_t cursor_walk(const struct vsr_io_cursor *cursor,
                            unsigned char *out, size_t size, uint32_t crc)
{
    uint32_t piece = cursor->piece;
    size_t offset = cursor->offset;

    while (size > 0) {
        const struct vsr_io_piece *current = piece_at(cursor, piece);
        size_t available = current->length - offset;
        size_t step = size < available ? size : available;

        if (step > 0) {
            if (out != NULL) {
                memcpy(out, current->base + offset, step);
                out += step;
            } else {
                crc = vsr_io_crc32c(crc, current->base + offset, step);
            }
            size -= step;
        }
        piece++;
        offset = 0;
    }
    return crc;
}

void vsr_io_cursor_init(struct vsr_io_cursor *cursor,
                        const struct vsr_io_piece *pieces, uint32_t count)
{
    size_t length = 0;

    for (uint32_t i = 0; i < count; ++i) {
        length += pieces[i].length;
    }
    cursor->pieces = pieces;
    cursor->one.base = NULL;
    cursor->one.length = 0;
    cursor->count = count;
    cursor->piece = 0;
    cursor->offset = 0;
    cursor->position = 0;
    cursor->length = length;
    cursor_settle(cursor);
}

void vsr_io_cursor_init_one(struct vsr_io_cursor *cursor, const void *base,
                            size_t length)
{
    cursor->pieces = NULL;
    cursor->one.base = base;
    cursor->one.length = length;
    cursor->count = 1;
    cursor->piece = 0;
    cursor->offset = 0;
    cursor->position = 0;
    cursor->length = length;
}

bool vsr_io_cursor_read(struct vsr_io_cursor *cursor, void *out, size_t size)
{
    if (!vsr_io_cursor_peek(cursor, out, size)) {
        return false;
    }
    cursor_advance(cursor, size);
    return true;
}

bool vsr_io_cursor_peek(const struct vsr_io_cursor *cursor, void *out,
                        size_t size)
{
    if (size > vsr_io_cursor_remaining(cursor)) {
        return false;
    }
    if (size > 0) {
        (void)cursor_walk(cursor, out, size, 0);
    }
    return true;
}

bool vsr_io_cursor_skip(struct vsr_io_cursor *cursor, size_t size)
{
    if (size > vsr_io_cursor_remaining(cursor)) {
        return false;
    }
    cursor_advance(cursor, size);
    return true;
}

bool vsr_io_cursor_align(struct vsr_io_cursor *cursor, size_t alignment)
{
    size_t mask = alignment - 1;

    return vsr_io_cursor_skip(cursor,
                              (alignment - (cursor->position & mask)) & mask);
}

const void *vsr_io_cursor_span(struct vsr_io_cursor *cursor, size_t size,
                               bool *contiguous)
{
    const struct vsr_io_piece *current;
    const unsigned char *address;

    if (size > vsr_io_cursor_remaining(cursor)) {
        return NULL;
    }
    current = piece_at(cursor, cursor->piece);
    if (size == 0) {
        *contiguous = true;
        /* Any non-NULL address will do; it is never dereferenced. */
        if (current->base == NULL) {
            return current;
        }
        return current->base + cursor->offset;
    }
    /* size > 0 bytes remain, so the settled piece holds the next byte. */
    address = current->base + cursor->offset;
    *contiguous = size <= current->length - cursor->offset;
    cursor_advance(cursor, size);
    return address;
}

bool vsr_io_cursor_crc(const struct vsr_io_cursor *cursor, size_t size,
                       uint32_t *crc)
{
    if (size > vsr_io_cursor_remaining(cursor)) {
        return false;
    }
    *crc = cursor_walk(cursor, NULL, size, *crc);
    return true;
}

bool vsr_io_cursor_u32(struct vsr_io_cursor *cursor, uint32_t *value)
{
    unsigned char bytes[4];

    if (!vsr_io_cursor_read(cursor, bytes, sizeof(bytes))) {
        return false;
    }
    *value = (uint32_t)bytes[0] | (uint32_t)bytes[1] << 8 |
             (uint32_t)bytes[2] << 16 | (uint32_t)bytes[3] << 24;
    return true;
}

bool vsr_io_cursor_u64(struct vsr_io_cursor *cursor, uint64_t *value)
{
    unsigned char bytes[8];
    uint64_t result = 0;

    if (!vsr_io_cursor_read(cursor, bytes, sizeof(bytes))) {
        return false;
    }
    for (size_t i = sizeof(bytes); i > 0; --i) {
        result = result << 8 | bytes[i - 1];
    }
    *value = result;
    return true;
}

bool vsr_io_cursor_i32(struct vsr_io_cursor *cursor, int32_t *value)
{
    uint32_t raw;

    if (!vsr_io_cursor_u32(cursor, &raw)) {
        return false;
    }
    /* int32_t is two's complement without padding: copying the bits avoids
     * the implementation-defined conversion of values above INT32_MAX. */
    memcpy(value, &raw, sizeof(*value));
    return true;
}
