#include "config.h"

#include "io/codec.h"
#include "io/crc32c.h"
#include "io/cursor.h"
#include "io/wire.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/*
 * Arbitrary bytes through every decoder of the codec, under one fixed limit
 * set, with the input split at input-derived positions into 1..4 cursor
 * pieces. Nothing may crash; a decoder that accepts bytes must yield a graph
 * that re-encodes to the same bytes (where the encoding is canonical) and
 * within the sizes the sizing functions promise.
 *
 * Input: byte 0 selects the target, bytes 1..3 the piece cuts, the rest is
 * the bytes under test. Since a CRC guards every header, each target also
 * runs a copy with its CRCs recomputed, so the fuzzer explores the fields
 * behind them.
 */

#define TARGETS 6u
#define MAX_INPUT 16384u
#define REGION_BYTES 65536u
#define MAX_VECTORS 512u

static const struct vsr_limits limits = {5, 8, 16, 8,   8,   8,   8,    8,
                                         4, 4, 64, 256, 256, 256, 1024, 4096};

static unsigned char copy_memory[MAX_INPUT];
/* Re-encodings: a record whose descriptors overlap re-encodes every payload
 * once, so it can outgrow its input by the descriptor count. */
static unsigned char encode_memory[(VSR_MAX_STORE_CHANGES + 1) * MAX_INPUT];
static unsigned char writer_memory[MAX_INPUT];
static unsigned char region_memory[REGION_BYTES];
static unsigned char change_memory[VSR_MAX_STORE_CHANGES][REGION_BYTES / 4];
static struct vsr_io_vec vectors[MAX_VECTORS];
static size_t message_region_bytes;
static size_t load_region_bytes;
static uint64_t record_limit;
static uint64_t frame_limit;

static void require(bool condition)
{
    if (!condition) {
        abort();
    }
}

static void setup(void)
{
    static bool done;

    if (done) {
        return;
    }
    require(vsr_io_codec_message_region(&limits, &message_region_bytes) ==
            VSR_OK);
    require(vsr_io_codec_load_region(&limits, &load_region_bytes) == VSR_OK);
    require(vsr_io_codec_record_limit(&limits, &record_limit) == VSR_OK);
    require(vsr_io_codec_frame_limit(&limits, &frame_limit) == VSR_OK);
    require(message_region_bytes <= REGION_BYTES);
    require(load_region_bytes <= REGION_BYTES / 4);
    require(frame_limit <= MAX_INPUT);
    done = true;
}

/* Splits [base, base + size) at the three cut bytes into 1..4 pieces. */
static uint32_t make_pieces(const unsigned char *base, size_t size,
                            const uint8_t *cuts, struct vsr_io_piece *pieces)
{
    size_t at[3];
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
    for (unsigned i = 0; i < 3; ++i) {
        if (at[i] > from) {
            pieces[count].base = base + from;
            pieces[count].length = at[i] - from;
            count++;
            from = at[i];
        }
    }
    pieces[count].base = size > from ? base + from : NULL;
    pieces[count].length = size - from;
    return count + 1;
}

/* One message body: decode, then re-encode and compare. */
static void message_body(const unsigned char *body, size_t size,
                         const uint8_t *cuts)
{
    struct vsr_io_piece pieces[VSR_IO_CURSOR_PIECES];
    struct vsr_io_cursor cursor;
    struct vsr_io_bump region;
    struct vsr_message *message = NULL;
    struct vsr_io_encoder encoder;
    struct vsr_io_writer writer = {writer_memory, sizeof(writer_memory), 0};
    uint32_t count = make_pieces(body, size, cuts, pieces);
    uint32_t length;
    uint32_t crc;
    uint32_t vector_count = 0;
    bool done = false;
    size_t total = 0;

    vsr_io_cursor_init(&cursor, pieces, count);
    vsr_io_bump_init(&region, region_memory, message_region_bytes);
    if (vsr_io_codec_decode_message(&cursor, &limits, &region, &message) !=
        VSR_OK) {
        return;
    }
    require(message != NULL);
    require(vsr_io_cursor_remaining(&cursor) == 0);
    require(region.used <= message_region_bytes);
    require(vsr_io_codec_message_digest(message, &limits, &length, &crc) ==
            VSR_OK);
    require(length == size);
    require(crc == vsr_io_crc32c(0, body, size));
    require(VSR_IO_FRAME_HEADER_BYTES + length <= frame_limit);
    vsr_io_encoder_begin(&encoder, message, length, crc);
    while (!done) {
        require(vsr_io_encoder_emit(&encoder, &writer, vectors, MAX_VECTORS,
                                    &vector_count, 200, &done) == VSR_OK);
        for (uint32_t i = 0; i < vector_count; ++i) {
            require(total + vectors[i].length <= sizeof(encode_memory));
            memcpy(encode_memory + total, vectors[i].base, vectors[i].length);
            total += vectors[i].length;
        }
    }
    require(total == VSR_IO_FRAME_HEADER_BYTES + size);
    require(memcmp(encode_memory + VSR_IO_FRAME_HEADER_BYTES, body, size) == 0);
}

/* Re-encodes a stream frame body from its decoded form. */
static void stream_body(uint16_t kind, const unsigned char *body, size_t size,
                        const uint8_t *cuts)
{
    struct vsr_io_piece pieces[VSR_IO_CURSOR_PIECES];
    struct vsr_io_cursor cursor;
    struct vsr_span span = {NULL, 0};
    struct vsr_io_wire_stream_end end;
    uint64_t offset = 0;
    size_t header;
    uint32_t count = make_pieces(body, size, cuts, pieces);

    vsr_io_cursor_init(&cursor, pieces, count);
    switch (kind) {
    case VSR_IO_FRAME_STREAM_REQUEST:
        if (vsr_io_codec_get_stream_request(&cursor, &span) != VSR_OK) {
            return;
        }
        header = sizeof(struct vsr_io_wire_stream_request);
        vsr_io_codec_put_stream_request(encode_memory, (uint32_t)span.size);
        break;
    case VSR_IO_FRAME_STREAM_CHUNK:
        if (vsr_io_codec_get_stream_chunk(&cursor, &offset, &span) != VSR_OK) {
            return;
        }
        header = sizeof(struct vsr_io_wire_stream_chunk);
        vsr_io_codec_put_stream_chunk(encode_memory, offset,
                                      (uint32_t)span.size);
        break;
    default:
        if (vsr_io_codec_get_stream_end(&cursor, &end) != VSR_OK) {
            return;
        }
        require(size == sizeof(end));
        vsr_io_codec_put_stream_end(encode_memory, end.bytes, end.status);
        require(memcmp(encode_memory, body, size) == 0);
        return;
    }
    require(vsr_io_cursor_remaining(&cursor) == 0);
    require(size == header + VSR_IO_WIRE_PAD(span.size));
    if (span.size > 0) {
        memcpy(encode_memory + header, span.data, span.size);
    }
    memset(encode_memory + header + span.size, 0,
           VSR_IO_WIRE_PAD(span.size) - span.size);
    require(memcmp(encode_memory, body, size) == 0);
}

static void frame_bytes(const unsigned char *bytes, size_t size,
                        const uint8_t *cuts)
{
    struct vsr_io_piece pieces[VSR_IO_CURSOR_PIECES];
    struct vsr_io_cursor cursor;
    struct vsr_io_wire_frame frame;
    struct vsr_io_wire_hello hello;
    uint32_t count = make_pieces(bytes, size, cuts, pieces);
    uint32_t crc = 0;
    const unsigned char *body = bytes + VSR_IO_FRAME_HEADER_BYTES;

    vsr_io_cursor_init(&cursor, pieces, count);
    if (vsr_io_codec_get_frame(&cursor, (uint32_t)(MAX_INPUT - 24), &frame) !=
        VSR_OK) {
        return;
    }
    require(cursor.position == VSR_IO_FRAME_HEADER_BYTES);
    if (!vsr_io_cursor_crc(&cursor, frame.length, &crc)) {
        return;
    }
    /* The body is decoded whether or not its CRC holds: the property under
     * test is the decoder's, the link checks the CRC first. */
    switch (frame.kind) {
    case VSR_IO_FRAME_HELLO:
        vsr_io_cursor_init_one(&cursor, body, frame.length);
        if (vsr_io_codec_get_hello(&cursor, &hello) == VSR_OK) {
            vsr_io_codec_put_hello(encode_memory, hello.handshake,
                                   hello.purpose, hello.node, hello.nonce);
            require(memcmp(encode_memory, body, frame.length) == 0);
        }
        break;
    case VSR_IO_FRAME_MESSAGE:
        message_body(body, frame.length, cuts);
        break;
    default:
        stream_body(frame.kind, body, frame.length, cuts);
        break;
    }
}

static void fuzz_frame(const unsigned char *bytes, size_t size,
                       const uint8_t *cuts)
{
    frame_bytes(bytes, size, cuts);
    if (size >= VSR_IO_FRAME_HEADER_BYTES) {
        memcpy(copy_memory, bytes, size);
        vsr_io_put_u32(
            copy_memory + offsetof(struct vsr_io_wire_frame, header_crc),
            vsr_io_crc32c(0, copy_memory,
                          offsetof(struct vsr_io_wire_frame, header_crc)));
        frame_bytes(copy_memory, size, cuts);
    }
}

/* Decodes one change payload into its own region and fills the host
 * change; false when the payload is rejected. *consumed reports whether
 * the payload was used up exactly. */
static bool decode_change(const struct vsr_io_wire_change *wire,
                          const unsigned char *record, unsigned index,
                          struct vsr_change *change, bool *consumed)
{
    struct vsr_io_cursor cursor;
    struct vsr_io_bump region;
    int rc;

    vsr_io_bump_init(&region, change_memory[index], load_region_bytes);
    vsr_io_cursor_init_one(&cursor, record + wire->offset, wire->length);
    change->type = wire->type;
    change->count = wire->count;
    change->first = wire->first;
    change->data = NULL;
    switch (wire->type) {
    case VSR_STORE_APPEND: {
        struct vsr_entry *entries = NULL;

        rc = vsr_io_codec_get_entries(&cursor, wire->count, &limits, &region,
                                      &entries);
        change->data = entries;
        break;
    }
    case VSR_STORE_CLIENTS: {
        struct vsr_client_record *records;

        /* The store applies the batch rule; the record decoder takes one
         * record at a time. */
        if (wire->count > limits.batch_entries) {
            return false;
        }
        records = vsr_io_bump_alloc(&region, sizeof(*records) * wire->count, 8);
        if (records == NULL) {
            return false;
        }
        rc = VSR_OK;
        for (uint32_t i = 0; i < wire->count && rc == VSR_OK; ++i) {
            rc = vsr_io_codec_get_client_record(&cursor, &limits, &region,
                                                &records[i]);
        }
        change->data = records;
        break;
    }
    case VSR_STORE_HARD_STATE: {
        struct vsr_hard_state *hard =
            vsr_io_bump_alloc(&region, sizeof(*hard), 8);

        if (hard == NULL) {
            return false;
        }
        rc = vsr_io_codec_get_hard_state(&cursor, &limits, &region, hard);
        change->data = hard;
        break;
    }
    case VSR_STORE_PUBLISH_CHECKPOINT:
    case VSR_STORE_RESTORE_CHECKPOINT: {
        struct vsr_checkpoint *checkpoint = NULL;

        rc =
            vsr_io_codec_get_checkpoint(&cursor, &limits, &region, &checkpoint);
        change->data = checkpoint;
        break;
    }
    case VSR_STORE_IDENTITY: {
        struct vsr_store_identity *identity =
            vsr_io_bump_alloc(&region, sizeof(*identity), 8);

        if (identity == NULL) {
            return false;
        }
        rc = vsr_io_codec_get_identity(&cursor, identity);
        change->data = identity;
        break;
    }
    default:
        rc = VSR_OK;
        break;
    }
    *consumed = vsr_io_cursor_remaining(&cursor) == 0;
    return rc == VSR_OK;
}

static void record_bytes(const unsigned char *bytes, size_t size)
{
    struct vsr_io_cursor cursor;
    struct vsr_io_wire_record header;
    struct vsr_io_wire_change wires[VSR_MAX_STORE_CHANGES];
    struct vsr_change changes[VSR_MAX_STORE_CHANGES];
    struct vsr_store store;
    uint32_t kind = 0;
    bool canonical = true;
    size_t expected;
    size_t written;
    size_t measured;

    vsr_io_cursor_init_one(&cursor, bytes, size);
    if (vsr_io_codec_get_record(&cursor, record_limit, &header, &kind) !=
            VSR_OK ||
        kind != VSR_IO_SCAN_RECORD) {
        return;
    }
    if (!vsr_io_codec_check_record(&cursor, &header)) {
        return;
    }
    expected =
        sizeof(header) + header.count * sizeof(struct vsr_io_wire_change);
    for (uint32_t i = 0; i < header.count; ++i) {
        bool consumed = false;

        if (vsr_io_codec_get_change(&cursor, &wires[i]) != VSR_OK ||
            (uint64_t)wires[i].offset + wires[i].length > header.length) {
            return;
        }
        if (!decode_change(&wires[i], bytes, i, &changes[i], &consumed)) {
            return;
        }
        canonical = canonical && consumed && wires[i].offset == expected;
        expected += wires[i].length;
    }
    canonical = canonical && expected == header.length;
    store.sequence = header.sequence;
    store.changes = changes;
    store.count = header.count;
    store.reserved = 0;
    /* A decoded transaction re-encodes, a canonical one to the same bytes;
     * every per-object limit held while decoding, so the only limit the
     * measure can find exceeded is the record's total. */
    require(vsr_io_codec_put_record(&store, header.generation, header.run,
                                    header.flushed, encode_memory,
                                    sizeof(encode_memory), &written) == VSR_OK);
    if (vsr_io_codec_record_bytes(&store, &limits, &measured) == VSR_OK) {
        require(measured == written);
        require(written <= record_limit);
    } else {
        require(written > record_limit);
    }
    if (canonical) {
        require(written == header.length);
        require(memcmp(encode_memory, bytes, written) == 0);
    }
}

static void fuzz_record(const unsigned char *bytes, size_t size)
{
    uint32_t length;

    record_bytes(bytes, size);
    if (size < sizeof(struct vsr_io_wire_record)) {
        return;
    }
    memcpy(copy_memory, bytes, size);
    vsr_io_put_u32(copy_memory, VSR_IO_RECORD_MAGIC);
    length = vsr_io_get_u32(copy_memory + 4);
    if (length >= sizeof(struct vsr_io_wire_record) && length <= size) {
        vsr_io_put_u32(copy_memory +
                           offsetof(struct vsr_io_wire_record, payload_crc),
                       vsr_io_crc32c(0, copy_memory + 48, length - 48));
    }
    vsr_io_put_u32(copy_memory +
                       offsetof(struct vsr_io_wire_record, header_crc),
                   vsr_io_crc32c(0, copy_memory, 44));
    record_bytes(copy_memory, size);
}

static void superblock_bytes(const unsigned char *bytes, uint32_t size)
{
    struct vsr_io_wire_superblock superblock;

    if (vsr_io_codec_get_superblock(bytes, size, &superblock) != VSR_OK) {
        return;
    }
    vsr_io_codec_put_superblock(&superblock, encode_memory, size);
    require(memcmp(encode_memory, bytes, size) == 0);
}

static void fuzz_superblock(const unsigned char *bytes, size_t size)
{
    superblock_bytes(bytes, (uint32_t)size);
    if (size < sizeof(struct vsr_io_wire_superblock)) {
        return;
    }
    memcpy(copy_memory, bytes, size);
    vsr_io_put_u32(copy_memory, VSR_IO_SUPERBLOCK_MAGIC);
    vsr_io_put_u32(copy_memory + 4, VSR_IO_STORE_FORMAT);
    vsr_io_put_u32(copy_memory + offsetof(struct vsr_io_wire_superblock, crc),
                   vsr_io_crc32c(0, copy_memory,
                                 offsetof(struct vsr_io_wire_superblock, crc)));
    superblock_bytes(copy_memory, (uint32_t)size);
}

static void segment_bytes(const unsigned char *bytes, size_t size)
{
    struct vsr_io_bump region;
    struct vsr_io_wire_segment fixed;
    struct vsr_store_identity identity;
    struct vsr_hard_state hard;
    struct vsr_checkpoint *checkpoint = NULL;
    struct vsr_io_segment_state state;
    size_t written;

    vsr_io_bump_init(&region, region_memory, load_region_bytes);
    if (vsr_io_codec_get_segment(bytes, size, &limits, &region, &fixed,
                                 &identity, &hard, &checkpoint) != VSR_OK) {
        return;
    }
    require(region.used <= load_region_bytes);
    state.identity =
        (fixed.flags & VSR_IO_SEGMENT_STATE) != 0 ? &identity : NULL;
    state.hard = state.identity != NULL ? &hard : NULL;
    state.checkpoint = checkpoint;
    state.log_begin = fixed.log_begin;
    state.log_end = fixed.log_end;
    state.client_base = fixed.client_base;
    state.last_sequence = fixed.last_sequence;
    state.durable_floor = fixed.durable_floor;
    require(vsr_io_codec_put_segment(&fixed, &state, encode_memory, size,
                                     &written) == VSR_OK);
    require(written == fixed.length);
    require(memcmp(encode_memory, bytes, size) == 0);
}

static void fuzz_segment(const unsigned char *bytes, size_t size)
{
    uint32_t length;

    segment_bytes(bytes, size);
    if (size < sizeof(struct vsr_io_wire_segment) + sizeof(uint32_t)) {
        return;
    }
    memcpy(copy_memory, bytes, size);
    vsr_io_put_u32(copy_memory, VSR_IO_SEGMENT_MAGIC);
    vsr_io_put_u32(copy_memory + 4, VSR_IO_STORE_FORMAT);
    length = vsr_io_get_u32(copy_memory +
                            offsetof(struct vsr_io_wire_segment, length));
    if (length >= sizeof(struct vsr_io_wire_segment) + sizeof(uint32_t) &&
        length <= size) {
        vsr_io_put_u32(copy_memory + length - 4,
                       vsr_io_crc32c(0, copy_memory, length - 4));
    }
    segment_bytes(copy_memory, size);
}

static void clients_bytes(const unsigned char *bytes, size_t size,
                          const uint8_t *cuts)
{
    struct vsr_io_piece pieces[VSR_IO_CURSOR_PIECES];
    struct vsr_io_cursor cursor;
    struct vsr_io_bump region;
    struct vsr_io_wire_clients_header header;
    struct vsr_io_wire_client_record fixed;
    struct vsr_client_record record;
    uint32_t count = make_pieces(bytes, size, cuts, pieces);
    uint32_t seen = 0;
    uint32_t trailer;

    vsr_io_cursor_init(&cursor, pieces, count);
    if (vsr_io_codec_get_clients_header(&cursor, &header) != VSR_OK) {
        return;
    }
    vsr_io_codec_put_clients_header(&header, encode_memory);
    require(memcmp(encode_memory, bytes, sizeof(header)) == 0);
    while (seen < header.count) {
        struct vsr_io_cursor skip = cursor;
        size_t start = cursor.position;
        size_t bytes_of;

        vsr_io_bump_init(&region, region_memory, load_region_bytes);
        if (vsr_io_codec_get_clients_record(&cursor, &limits, &region,
                                            &record) != VSR_OK) {
            return;
        }
        require(region.used <= load_region_bytes);
        bytes_of = vsr_io_codec_clients_record_bytes(record.result.data.size);
        require(cursor.position - start == bytes_of);
        require(vsr_io_codec_skip_client_record(&skip, &fixed) == VSR_OK);
        require(skip.position + sizeof(uint32_t) == cursor.position);
        require(fixed.length == record.result.data.size);
        vsr_io_codec_put_clients_record(&record, encode_memory);
        require(memcmp(encode_memory, bytes + start, bytes_of) == 0);
        seen++;
    }
    if (vsr_io_codec_get_clients_trailer(&cursor, &trailer) == VSR_OK) {
        vsr_io_codec_put_clients_trailer(trailer, encode_memory);
        require(memcmp(encode_memory, bytes + cursor.position - 8, 8) == 0);
    }
}

static void fuzz_clients(const unsigned char *bytes, size_t size,
                         const uint8_t *cuts)
{
    size_t at = sizeof(struct vsr_io_wire_clients_header);
    uint32_t count;

    clients_bytes(bytes, size, cuts);
    if (size < at) {
        return;
    }
    memcpy(copy_memory, bytes, size);
    vsr_io_put_u32(copy_memory, VSR_IO_CLIENTS_MAGIC);
    vsr_io_put_u32(copy_memory + 4, VSR_IO_CLIENTS_FORMAT);
    vsr_io_put_u32(
        copy_memory + offsetof(struct vsr_io_wire_clients_header, crc),
        vsr_io_crc32c(0, copy_memory,
                      offsetof(struct vsr_io_wire_clients_header, crc)));
    count = vsr_io_get_u32(copy_memory +
                           offsetof(struct vsr_io_wire_clients_header, count));
    for (uint32_t i = 0; i < count; ++i) {
        size_t length;
        size_t total;

        if (size - at < sizeof(struct vsr_io_wire_client_record)) {
            break;
        }
        length =
            vsr_io_get_u32(copy_memory + at +
                           offsetof(struct vsr_io_wire_client_record, length));
        total =
            sizeof(struct vsr_io_wire_client_record) + VSR_IO_WIRE_PAD(length);
        if (total > size - at || size - at - total < sizeof(uint32_t)) {
            break;
        }
        vsr_io_put_u32(copy_memory + at + total,
                       vsr_io_crc32c(0, copy_memory + at, total));
        at += total + sizeof(uint32_t);
    }
    clients_bytes(copy_memory, size, cuts);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    const unsigned char *bytes;
    size_t length;

    setup();
    if (size < 4 || size - 4 > MAX_INPUT) {
        return 0;
    }
    bytes = data + 4;
    length = size - 4;
    switch (data[0] % TARGETS) {
    case 0:
        fuzz_frame(bytes, length, data + 1);
        break;
    case 1:
        fuzz_record(bytes, length);
        break;
    case 2:
        fuzz_superblock(bytes, length);
        break;
    case 3:
        fuzz_segment(bytes, length);
        break;
    case 4:
        fuzz_clients(bytes, length, data + 1);
        break;
    default:
        message_body(bytes, length, data + 1);
        break;
    }
    return 0;
}
