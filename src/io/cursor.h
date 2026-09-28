#ifndef VSR_IO_CURSOR_H
#define VSR_IO_CURSOR_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Boundary-aware byte reader shared by the frame decoder and the store record
 * reader (docs/io-implementation.md, "Cursor"). A cursor reads a logical byte
 * sequence made of one or more contiguous PIECES, so that a frame body or a
 * store record spread over two slabs decodes through the same code as one
 * that lies in a single slab. Today every frame and every record is delivered
 * in one piece (a straddling frame is copied into a fresh slab first, and a
 * record never crosses a segment or a tail-ring wrap); multi-piece bodies are
 * the deferred change of docs/io-design.md decision 24 and touch only the
 * callers that build piece arrays.
 *
 * Every accessor is bounded: it fails and leaves the cursor unchanged when
 * fewer bytes remain than requested. Nothing here allocates or checksums;
 * the caller runs CRC32C over the same pieces.
 */

#define VSR_IO_CURSOR_PIECES 4u

struct vsr_io_piece {
    const unsigned char *base;
    size_t length;
};

struct vsr_io_cursor {
    const struct vsr_io_piece *pieces; /* Borrowed for the cursor's life;
                                          NULL after init_one, which keeps
                                          its piece in `one` so that a copy
                                          of the cursor stays valid. */
    struct vsr_io_piece one;
    uint32_t count;
    uint32_t piece;  /* Index of the piece holding `position`. */
    size_t offset;   /* Offset of `position` inside that piece. */
    size_t position; /* Bytes consumed since init. */
    size_t length;   /* Sum of piece lengths. */
};

/* count is 1..VSR_IO_CURSOR_PIECES; every piece has a non-NULL base when
 * its length is nonzero. The sum of lengths must not overflow size_t. */
void vsr_io_cursor_init(struct vsr_io_cursor *cursor,
                        const struct vsr_io_piece *pieces, uint32_t count);
/* One contiguous piece, the common case; base may be NULL when length is
 * zero. */
void vsr_io_cursor_init_one(struct vsr_io_cursor *cursor, const void *base,
                            size_t length);

static inline size_t vsr_io_cursor_remaining(const struct vsr_io_cursor *cursor)
{
    return cursor->length - cursor->position;
}

/* Copies size bytes out and advances. false: too few bytes, nothing moved. */
bool vsr_io_cursor_read(struct vsr_io_cursor *cursor, void *out, size_t size);
/* Copies without advancing. */
bool vsr_io_cursor_peek(const struct vsr_io_cursor *cursor, void *out,
                        size_t size);
bool vsr_io_cursor_skip(struct vsr_io_cursor *cursor, size_t size);
/* Advances to the next multiple of alignment of the position; alignment is
 * a power of two. Skipped bytes are padding and are not inspected. */
bool vsr_io_cursor_align(struct vsr_io_cursor *cursor, size_t alignment);

/*
 * Claims the next size bytes as a SPAN and advances. When the span lies in
 * one piece, returns its address and sets *contiguous; a decoder then keeps
 * that pointer as the span of a blob, with no copy. When it crosses a piece
 * boundary, returns the address of its first byte with *contiguous false:
 * the caller copies it out through a copy of the cursor taken before the
 * call, or rejects the input, whichever the module's contract says. size 0
 * returns a non-NULL address and contiguous. NULL means too few bytes
 * remain, nothing moved.
 */
const void *vsr_io_cursor_span(struct vsr_io_cursor *cursor, size_t size,
                               bool *contiguous);

/* CRC32C of the next size bytes, chained from *crc (0 to start, as in
 * vsr_io_crc32c), stored back in *crc, without advancing. false: too few
 * bytes, *crc unchanged. */
bool vsr_io_cursor_crc(const struct vsr_io_cursor *cursor, size_t size,
                       uint32_t *crc);

/* Little-endian fixed-width reads through the cursor: a decoder reads every
 * wire header field with these, never through a struct cast, so unaligned
 * and cross-piece headers are legal. false: too few bytes, nothing moved. */
bool vsr_io_cursor_u16(struct vsr_io_cursor *cursor, uint16_t *value);
bool vsr_io_cursor_u32(struct vsr_io_cursor *cursor, uint32_t *value);
bool vsr_io_cursor_u64(struct vsr_io_cursor *cursor, uint64_t *value);
bool vsr_io_cursor_i32(struct vsr_io_cursor *cursor, int32_t *value);

#endif /* VSR_IO_CURSOR_H */
