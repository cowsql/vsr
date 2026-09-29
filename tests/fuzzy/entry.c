#include "config.h"

#include "io/codec.h"
#include "io/cursor.h"
#include "io/wire.h"

#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/*
 * Arbitrary bytes as the payload of an APPEND change through
 * vsr_io_codec_get_entry_at, under the limit set of tests/fuzzy/frame, with
 * the payload split at input-derived positions into 1..4 cursor pieces,
 * each copied into its own allocation so that a read past a piece is an
 * ASan report. Nothing may crash. The skipped entries are measured by an
 * independent walk over their body_length fields: the call fails exactly
 * when that walk leaves the payload. An accepted entry stays within the
 * limits, allocates at most the region one loaded entry is budgeted, and
 * decodes the same from a region of exactly that budget; its spans point
 * into the pieces; re-encoded as a one-entry APPEND it measures within the
 * record limit and yields exactly the bytes between the skipped prefix and
 * the cursor. Whenever vsr_io_codec_get_entries decodes the skip + 1
 * entries, get_entry_at succeeds and ends at the same position.
 *
 * Input: byte 0 is skip, bytes 1..3 the piece cuts, the rest the payload.
 */

#define MAX_INPUT 16384u
#define REGION_BYTES 65536u

static const struct vsr_limits limits = {5, 8, 16, 8,   8,   8,   8,    8,
                                         4, 4, 64, 256, 256, 256, 1024, 4096};

static alignas(16) unsigned char region_memory[REGION_BYTES];
static alignas(16) unsigned char tight_memory[REGION_BYTES];
static alignas(16) unsigned char batch_memory[REGION_BYTES];
static unsigned char encode_memory[2 * MAX_INPUT];
static size_t load_region_bytes;
static size_t body_region_bytes;
static uint64_t record_limit;

static void require(bool condition)
{
    if (!condition) {
        abort();
    }
}

static size_t max_size(size_t a, size_t b)
{
    return a > b ? a : b;
}

static void setup(void)
{
    static bool done;

    if (done) {
        return;
    }
    require(vsr_io_codec_load_region(&limits, &load_region_bytes) == VSR_OK);
    require(vsr_io_codec_record_limit(&limits, &record_limit) == VSR_OK);
    require(load_region_bytes <= REGION_BYTES);
    /* The body of one loaded entry, as vsr_io_codec_load_region budgets
     * it: a blob with one span, a membership of `members`, or a check. */
    body_region_bytes =
        max_size(max_size(sizeof(struct vsr_blob) + sizeof(struct vsr_span),
                          sizeof(struct vsr_membership) +
                              limits.members * sizeof(struct vsr_member)),
                 sizeof(struct vsr_check_epoch));
    done = true;
}

/* Splits [base, base + size) at the three cut bytes into 1..4 pieces, each
 * a separate heap copy in copies[] (NULL when empty). */
static uint32_t make_pieces(const unsigned char *base, size_t size,
                            const uint8_t *cuts, struct vsr_io_piece *pieces,
                            unsigned char **copies)
{
    size_t at[4];
    size_t from = 0;
    uint32_t count = 0;

    for (unsigned i = 0; i < 3; ++i) {
        at[i] = (size_t)cuts[i] * size / 255u;
    }
    for (unsigned i = 0; i < 3; ++i) {
        for (unsigned j = i + 1; j < 3; ++j) {
            if (at[j] < at[i]) {
                size_t swap = at[i];

                at[i] = at[j];
                at[j] = swap;
            }
        }
    }
    at[3] = size;
    for (unsigned i = 0; i < 4; ++i) {
        size_t length = at[i] - from;

        if (length == 0 && (i < 3 || count > 0)) {
            continue;
        }
        copies[count] = NULL;
        if (length > 0) {
            copies[count] = malloc(length);
            require(copies[count] != NULL);
            memcpy(copies[count], base + from, length);
        }
        pieces[count].base = copies[count];
        pieces[count].length = length;
        count++;
        from = at[i];
    }
    return count;
}

/* The payload offset after `skip` entries, from their body_length fields
 * alone; false when an entry header or body leaves the payload or a
 * body_length is not aligned. */
static bool skip_entries(const unsigned char *bytes, size_t size, uint32_t skip,
                         size_t *end)
{
    size_t at = 0;

    for (uint32_t i = 0; i < skip; ++i) {
        uint32_t body;

        if (size - at < sizeof(struct vsr_io_wire_entry)) {
            return false;
        }
        body = vsr_io_get_u32(bytes + at +
                              offsetof(struct vsr_io_wire_entry, body_length));
        at += sizeof(struct vsr_io_wire_entry);
        if (body % VSR_IO_WIRE_ALIGN != 0 || body > size - at) {
            return false;
        }
        at += body;
    }
    *end = at;
    return true;
}

static bool in_pieces(const struct vsr_io_piece *pieces, uint32_t count,
                      const void *data, size_t size)
{
    uintptr_t at = (uintptr_t)data;

    for (uint32_t i = 0; i < count; ++i) {
        uintptr_t base = (uintptr_t)pieces[i].base;

        if (pieces[i].base != NULL && at >= base && size <= pieces[i].length &&
            at - base <= pieces[i].length - size) {
            return true;
        }
    }
    return false;
}

/* The decoded graph within the limits, with its spans in the pieces. */
static void check_entry(const struct vsr_entry *entry,
                        const struct vsr_io_piece *pieces, uint32_t count)
{
    require(entry->reserved == 0);
    switch (entry->type) {
    case VSR_REQUEST_COMMAND: {
        const struct vsr_blob *blob = entry->body;

        require(blob != NULL && blob->reserved == 0);
        require(blob->size <= limits.command_bytes);
        if (blob->size == 0) {
            require(blob->count == 0 && blob->spans == NULL);
        } else {
            require(blob->count == 1 && blob->spans != NULL);
            require(blob->spans[0].size == blob->size);
            require(in_pieces(pieces, count, blob->spans[0].data,
                              blob->spans[0].size));
        }
        break;
    }
    case VSR_REQUEST_RECONFIGURE: {
        const struct vsr_membership *membership = entry->body;

        require(membership != NULL);
        require(membership->count <= limits.members);
        require((membership->count == 0) == (membership->members == NULL));
        for (uint32_t i = 0; i < membership->count; ++i) {
            require(membership->members[i].reserved == 0);
        }
        break;
    }
    case VSR_REQUEST_CHECK_EPOCH:
        require(entry->body != NULL);
        break;
    case VSR_REQUEST_NOOP:
        require(entry->body == NULL);
        break;
    default:
        require(false);
    }
}

/* Re-encodes the entry as a one-entry APPEND: the record measures within
 * the limits, and its payload is exactly bytes[start, end). */
static void check_encoding(const struct vsr_entry *entry,
                           const unsigned char *bytes, size_t start, size_t end)
{
    const unsigned char *descriptor =
        encode_memory + sizeof(struct vsr_io_wire_record);
    struct vsr_change change = {VSR_STORE_APPEND, 1, entry->op, entry};
    struct vsr_store store = {1, &change, 1, 0};
    size_t written;
    size_t measured;
    uint32_t offset;
    uint32_t length;

    require(vsr_io_codec_put_record(&store, 1, 1, 0, encode_memory,
                                    sizeof(encode_memory), &written) == VSR_OK);
    require(vsr_io_codec_record_bytes(&store, &limits, &measured) == VSR_OK);
    require(measured == written && written <= record_limit);
    offset = vsr_io_get_u32(descriptor +
                            offsetof(struct vsr_io_wire_change, offset));
    length = vsr_io_get_u32(descriptor +
                            offsetof(struct vsr_io_wire_change, length));
    require((size_t)offset + length <= written);
    require(start <= end && end - start == length);
    require(memcmp(encode_memory + offset, bytes + start, length) == 0);
}

static void entry_bytes(const unsigned char *bytes, size_t size, uint32_t skip,
                        const uint8_t *cuts)
{
    struct vsr_io_piece pieces[VSR_IO_CURSOR_PIECES];
    struct vsr_io_cursor cursor;
    struct vsr_io_cursor tight_cursor;
    struct vsr_io_bump region;
    struct vsr_io_bump tight;
    struct vsr_entry entry;
    struct vsr_entry tight_entry;
    unsigned char *copies[VSR_IO_CURSOR_PIECES];
    uint32_t count = make_pieces(bytes, size, cuts, pieces, copies);
    size_t start = 0;
    bool skipped = skip_entries(bytes, size, skip, &start);
    int rc;

    vsr_io_cursor_init(&cursor, pieces, count);
    vsr_io_bump_init(&region, region_memory, load_region_bytes);
    rc = vsr_io_codec_get_entry_at(&cursor, skip, &limits, &region, &entry);
    require(rc == VSR_OK || rc == VSR_EINVAL || rc == VSR_ELIMIT);
    require(cursor.position <= size);
    if (!skipped) {
        require(rc == VSR_EINVAL);
    }
    /* A region of exactly one entry's body budget decides the same. */
    vsr_io_cursor_init(&tight_cursor, pieces, count);
    vsr_io_bump_init(&tight, tight_memory, body_region_bytes);
    require(vsr_io_codec_get_entry_at(&tight_cursor, skip, &limits, &tight,
                                      &tight_entry) == rc);
    if (rc == VSR_OK) {
        require(region.used <= body_region_bytes);
        require(tight_cursor.position == cursor.position);
        check_entry(&entry, pieces, count);
        check_encoding(&entry, bytes, start, cursor.position);
    }
    /* The whole prefix decodes only if the skip reaches the same entry. */
    if (skip < limits.batch_entries) {
        struct vsr_io_cursor whole;
        struct vsr_io_bump batch;
        struct vsr_entry *entries = NULL;

        vsr_io_cursor_init(&whole, pieces, count);
        vsr_io_bump_init(&batch, batch_memory, load_region_bytes);
        if (vsr_io_codec_get_entries(&whole, skip + 1, &limits, &batch,
                                     &entries) == VSR_OK) {
            require(rc == VSR_OK);
            require(whole.position == cursor.position);
            check_encoding(&entries[skip], bytes, start, whole.position);
        }
    }
    for (uint32_t i = 0; i < count; ++i) {
        free(copies[i]);
    }
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    setup();
    if (size < 4 || size - 4 > MAX_INPUT) {
        return 0;
    }
    entry_bytes(data + 4, size - 4, data[0], data + 1);
    return 0;
}
