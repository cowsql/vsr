#include "config.h"

#include "io/crc32c.h"
#include "io/cursor.h"
#include "lib/check.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define MAX_BYTES 64u

/* The logical sequence, and one private buffer per piece so that pieces are
 * never adjacent in memory by accident. */
static unsigned char data[MAX_BYTES];
static unsigned char buffers[VSR_IO_CURSOR_PIECES][MAX_BYTES];

struct layout {
    struct vsr_io_piece pieces[VSR_IO_CURSOR_PIECES];
    size_t start[VSR_IO_CURSOR_PIECES]; /* Logical offset of each piece. */
    uint32_t count;
    size_t length;
    /* Exhaustive checks at every position; otherwise shorter peeks, and
     * refusals only next to piece boundaries, to bound the run time of the
     * many long three-piece layouts. */
    bool full;
};

static void fill_data(void)
{
    for (size_t i = 0; i < MAX_BYTES; ++i) {
        data[i] = (unsigned char)(i * 37u + 11u);
    }
}

/* Splits data[0..length) at the given cut points into count pieces; an empty
 * piece gets a NULL base, which the cursor must tolerate. */
static void make_layout(struct layout *layout, size_t length,
                        const size_t *cuts, uint32_t count)
{
    size_t from = 0;

    memset(buffers, 0xA5, sizeof(buffers));
    layout->count = count;
    layout->length = length;
    for (uint32_t i = 0; i < count; ++i) {
        size_t to = i + 1 < count ? cuts[i] : length;

        layout->start[i] = from;
        layout->pieces[i].length = to - from;
        if (to > from) {
            memcpy(buffers[i], data + from, to - from);
            layout->pieces[i].base = buffers[i];
        } else {
            layout->pieces[i].base = NULL;
        }
        from = to;
    }
}

/* Piece holding logical byte k (k < length); empty pieces never do. */
static uint32_t piece_of(const struct layout *layout, size_t k)
{
    for (uint32_t i = 0; i < layout->count; ++i) {
        if (k >= layout->start[i] &&
            k < layout->start[i] + layout->pieces[i].length) {
            return i;
        }
    }
    CHECK(false);
    return 0;
}

static void open_at(struct vsr_io_cursor *cursor, const struct layout *layout,
                    size_t position)
{
    vsr_io_cursor_init(cursor, layout->pieces, layout->count);
    CHECK(cursor->length == layout->length);
    CHECK(cursor->position == 0);
    CHECK(vsr_io_cursor_skip(cursor, position));
    CHECK(cursor->position == position);
    CHECK(vsr_io_cursor_remaining(cursor) == layout->length - position);
}

static bool same_cursor(const struct vsr_io_cursor *a,
                        const struct vsr_io_cursor *b)
{
    return a->pieces == b->pieces && a->count == b->count &&
           a->piece == b->piece && a->offset == b->offset &&
           a->position == b->position && a->length == b->length;
}

static uint64_t little_endian(size_t at, size_t size)
{
    uint64_t value = 0;

    for (size_t i = size; i > 0; --i) {
        value = value << 8 | data[at + i - 1];
    }
    return value;
}

/* Every accessor fails without moving when size bytes are too many. */
static void check_refusals(const struct vsr_io_cursor *cursor)
{
    size_t remaining = vsr_io_cursor_remaining(cursor);
    struct vsr_io_cursor copy = *cursor;
    unsigned char out[MAX_BYTES + 1];
    bool contiguous = true;
    uint32_t crc = 0x1234u;
    uint64_t u64 = 7;
    uint32_t u32 = 7;
    int32_t i32 = 7;

    memset(out, 0x5A, sizeof(out));
    CHECK(!vsr_io_cursor_read(&copy, out, remaining + 1));
    CHECK(!vsr_io_cursor_peek(&copy, out, remaining + 1));
    CHECK(out[0] == 0x5A);
    CHECK(!vsr_io_cursor_skip(&copy, remaining + 1));
    CHECK(!vsr_io_cursor_skip(&copy, SIZE_MAX));
    CHECK(vsr_io_cursor_span(&copy, remaining + 1, &contiguous) == NULL);
    CHECK(vsr_io_cursor_span(&copy, SIZE_MAX, &contiguous) == NULL);
    CHECK(!vsr_io_cursor_crc(&copy, remaining + 1, &crc));
    CHECK(crc == 0x1234u);
    if (remaining < 4) {
        CHECK(!vsr_io_cursor_u32(&copy, &u32));
        CHECK(!vsr_io_cursor_i32(&copy, &i32));
        CHECK(u32 == 7 && i32 == 7);
    }
    if (remaining < 8) {
        CHECK(!vsr_io_cursor_u64(&copy, &u64));
        CHECK(u64 == 7);
    }
    CHECK(same_cursor(&copy, cursor));
}

static bool near_boundary(const struct layout *layout, size_t k)
{
    for (uint32_t i = 0; i < layout->count; ++i) {
        size_t start = layout->start[i];

        if (k + 1 >= start && k <= start + 1) {
            return true;
        }
    }
    return k + 1 >= layout->length;
}

/* Integer reads, byte reads, peeks and refusals at every position. */
static void check_reads(const struct layout *layout)
{
    size_t length = layout->length;
    struct vsr_io_cursor cursor;
    unsigned char out[MAX_BYTES];

    /* One read of everything, then the end refuses more. */
    open_at(&cursor, layout, 0);
    CHECK(vsr_io_cursor_read(&cursor, out, length));
    CHECK(memcmp(out, data, length) == 0);
    CHECK(vsr_io_cursor_remaining(&cursor) == 0);
    check_refusals(&cursor);

    /* Byte by byte, with a peek of the rest before each read. */
    open_at(&cursor, layout, 0);
    for (size_t k = 0; k < length; ++k) {
        struct vsr_io_cursor before = cursor;
        size_t peek = length - k;
        unsigned char byte;

        if (!layout->full && peek > 9) {
            peek = 9;
        }
        CHECK(vsr_io_cursor_peek(&cursor, out, peek));
        CHECK(memcmp(out, data + k, peek) == 0);
        CHECK(same_cursor(&cursor, &before));
        if (layout->full || near_boundary(layout, k)) {
            check_refusals(&cursor);
        }
        CHECK(vsr_io_cursor_read(&cursor, &byte, 1));
        CHECK(byte == data[k]);
        CHECK(cursor.position == k + 1);
    }
    check_refusals(&cursor);
    CHECK(vsr_io_cursor_read(&cursor, out, 0));
    CHECK(vsr_io_cursor_skip(&cursor, 0));

    /* Fixed-width little-endian reads starting at every position. */
    for (size_t k = 0; k <= length; ++k) {
        uint32_t u32;
        uint64_t u64;
        int32_t i32;

        open_at(&cursor, layout, k);
        if (length - k >= 4) {
            uint32_t expect = (uint32_t)little_endian(k, 4);

            CHECK(vsr_io_cursor_u32(&cursor, &u32));
            CHECK(u32 == expect);
            CHECK(cursor.position == k + 4);
            open_at(&cursor, layout, k);
            CHECK(vsr_io_cursor_i32(&cursor, &i32));
            CHECK((uint32_t)i32 == expect);
            open_at(&cursor, layout, k);
        }
        if (length - k >= 8) {
            CHECK(vsr_io_cursor_u64(&cursor, &u64));
            CHECK(u64 == little_endian(k, 8));
            CHECK(cursor.position == k + 8);
        }
    }
}

/* Whether to checksum this (position, size) pair: all of them on short
 * sequences, a boundary sample on long ones, where the checksums dominate
 * the run time and add nothing the short ones do not cover. */
static bool crc_sampled(size_t length, size_t k, size_t size)
{
    return length <= 24 || size <= 1 || size == 8 || size == length - k;
}

/* span for every (position, size) pair, crc for most. */
static void check_spans(const struct layout *layout)
{
    size_t length = layout->length;

    for (size_t k = 0; k <= length; ++k) {
        for (size_t size = 0; size <= length - k; ++size) {
            struct vsr_io_cursor cursor;
            struct vsr_io_cursor before;
            bool contiguous = false;
            const unsigned char *address;
            uint32_t crc = 0;
            uint32_t seed = 0x9E3779B9u;

            open_at(&cursor, layout, k);
            before = cursor;
            if (crc_sampled(length, k, size)) {
                CHECK(vsr_io_cursor_crc(&cursor, size, &crc));
                CHECK(crc == vsr_io_crc32c(0, data + k, size));
                CHECK(vsr_io_cursor_crc(&cursor, size, &seed));
                CHECK(seed == vsr_io_crc32c(0x9E3779B9u, data + k, size));
                CHECK(same_cursor(&cursor, &before));
            }

            address = vsr_io_cursor_span(&cursor, size, &contiguous);
            CHECK(address != NULL);
            CHECK(cursor.position == k + size);
            if (size == 0) {
                CHECK(contiguous);
                continue;
            }
            {
                uint32_t first = piece_of(layout, k);
                uint32_t last = piece_of(layout, k + size - 1);

                CHECK(contiguous == (first == last));
                CHECK(address ==
                      layout->pieces[first].base + (k - layout->start[first]));
                if (contiguous) {
                    CHECK(memcmp(address, data + k, size) == 0);
                }
            }
        }
    }
}

static void check_alignment(const struct layout *layout)
{
    static const size_t alignments[] = {1, 2, 4, 8, 16, 64};
    size_t length = layout->length;

    for (size_t k = 0; k <= length; ++k) {
        for (size_t a = 0; a < sizeof(alignments) / sizeof(alignments[0]);
             ++a) {
            size_t alignment = alignments[a];
            size_t target = (k + alignment - 1) / alignment * alignment;
            struct vsr_io_cursor cursor;
            struct vsr_io_cursor before;

            open_at(&cursor, layout, k);
            before = cursor;
            if (target > length) {
                CHECK(!vsr_io_cursor_align(&cursor, alignment));
                CHECK(same_cursor(&cursor, &before));
                continue;
            }
            CHECK(vsr_io_cursor_align(&cursor, alignment));
            CHECK(cursor.position == target);
            /* Aligning an aligned position is a no-op. */
            CHECK(vsr_io_cursor_align(&cursor, alignment));
            CHECK(cursor.position == target);
            if (target < length) {
                unsigned char byte;

                CHECK(vsr_io_cursor_read(&cursor, &byte, 1));
                CHECK(byte == data[target]);
            }
        }
    }
}

static void check_layout(size_t length, const size_t *cuts, uint32_t count,
                         bool full)
{
    struct layout layout;

    make_layout(&layout, length, cuts, count);
    layout.full = full;
    check_reads(&layout);
    if (full) {
        check_alignment(&layout);
        check_spans(&layout);
    }
}

/* Every split point of every length, in two and three pieces. */
static void test_splits(void)
{
    for (size_t length = 1; length <= MAX_BYTES; ++length) {
        size_t cuts[VSR_IO_CURSOR_PIECES - 1] = {0};

        check_layout(length, cuts, 1, true);
        for (size_t a = 0; a <= length; ++a) {
            cuts[0] = a;
            check_layout(length, cuts, 2, true);
            for (size_t b = a; b <= length; ++b) {
                cuts[1] = b;
                /* Exhaustive on the shorter three-piece layouts only:
                 * the span loop is cubic per layout. */
                check_layout(length, cuts, 3, length <= 24);
            }
        }
    }
}

/* Four pieces with empty ones at the front, the middle and the end. */
static void test_empty_pieces(void)
{
    static const size_t shapes[][3] = {
        {0, 0, 5}, {5, 5, 5}, {3, 3, 9}, {9, 20, 20}, {0, 20, 20}, {20, 20, 20},
    };

    for (size_t s = 0; s < sizeof(shapes) / sizeof(shapes[0]); ++s) {
        check_layout(20, shapes[s], 4, true);
    }
}

/* init_one, including an empty one without a base, and a copy by value. */
static void test_one_piece(void)
{
    struct vsr_io_cursor cursor;
    struct vsr_io_cursor copy;
    bool contiguous = false;
    const void *address;
    uint32_t value;
    uint64_t wide;
    uint32_t crc = 0;

    vsr_io_cursor_init_one(&cursor, NULL, 0);
    CHECK(vsr_io_cursor_remaining(&cursor) == 0);
    address = vsr_io_cursor_span(&cursor, 0, &contiguous);
    CHECK(address != NULL && contiguous);
    CHECK(vsr_io_cursor_crc(&cursor, 0, &crc) && crc == 0);
    CHECK(vsr_io_cursor_align(&cursor, 8));
    check_refusals(&cursor);

    vsr_io_cursor_init_one(&cursor, data, 12);
    CHECK(vsr_io_cursor_u32(&cursor, &value));
    CHECK(value == (uint32_t)little_endian(0, 4));
    copy = cursor;
    CHECK(vsr_io_cursor_u64(&copy, &wide));
    CHECK(wide == little_endian(4, 8));
    CHECK(cursor.position == 4);
    address = vsr_io_cursor_span(&cursor, 8, &contiguous);
    CHECK(address == data + 4 && contiguous);
    check_refusals(&cursor);
}

/* Sign handling of i32 at the extremes. */
static void test_signed(void)
{
    static const unsigned char bytes[] = {
        0xFF, 0xFF, 0xFF, 0xFF, /* -1 */
        0x00, 0x00, 0x00, 0x80, /* INT32_MIN */
        0xFF, 0xFF, 0xFF, 0x7F, /* INT32_MAX */
        0xFE, 0xFF, 0xFF, 0xFF, /* -2 */
    };
    struct vsr_io_cursor cursor;
    int32_t value;

    vsr_io_cursor_init_one(&cursor, bytes, sizeof(bytes));
    CHECK(vsr_io_cursor_i32(&cursor, &value) && value == -1);
    CHECK(vsr_io_cursor_i32(&cursor, &value) && value == INT32_MIN);
    CHECK(vsr_io_cursor_i32(&cursor, &value) && value == INT32_MAX);
    CHECK(vsr_io_cursor_i32(&cursor, &value) && value == -2);
    CHECK(!vsr_io_cursor_i32(&cursor, &value) && value == -2);
}

int main(void)
{
    fill_data();
    test_splits();
    test_empty_pieces();
    test_one_piece();
    test_signed();
    printf("cursor ok\n");
    return 0;
}
