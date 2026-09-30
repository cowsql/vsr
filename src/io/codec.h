#ifndef VSR_IO_CODEC_H
#define VSR_IO_CODEC_H

#include "io/cursor.h"
#include "io/wire.h"
#include "vsr-io.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Codec: host structs <-> the byte layouts of wire.h
 * (docs/io-implementation.md, "Codec"). Pure functions over memory the
 * caller supplies; no I/O, no allocation, no clock.
 *
 * Decoding is in place. Fixed headers are read through a cursor into host
 * structs allocated from a DECODE REGION (a bump allocator over caller
 * memory sized by vsr_io_codec_region_bytes); payload bytes (command bodies,
 * results, manifests) are never copied and become one vsr_span each into the
 * bytes the cursor reads, so the decoded graph is valid exactly as long as
 * those bytes are pinned. Every decoder checks the core limits it is given
 * and returns VSR_ELIMIT when the input exceeds them and VSR_EINVAL when it
 * is malformed; the caller then rejects the frame or reports CORRUPT.
 *
 * Encoding for the network is VECTORED: header bytes go to a writer over
 * the link's send slab and payload spans are referenced from where they
 * live, so a send never copies a body (spans shorter than
 * VSR_IO_INLINE_BYTES are copied into the writer instead, since a vector
 * costs more than a small copy). Encoding for the store is a plain copy into
 * the tail ring. Both share the size and CRC computations.
 */

/* Little-endian field access; the only way headers are written or read. */
static inline void vsr_io_put_u32(unsigned char *out, uint32_t value)
{
    out[0] = (unsigned char)value;
    out[1] = (unsigned char)(value >> 8);
    out[2] = (unsigned char)(value >> 16);
    out[3] = (unsigned char)(value >> 24);
}

static inline void vsr_io_put_u64(unsigned char *out, uint64_t value)
{
    vsr_io_put_u32(out, (uint32_t)value);
    vsr_io_put_u32(out + 4, (uint32_t)(value >> 32));
}

static inline uint32_t vsr_io_get_u32(const unsigned char *in)
{
    return (uint32_t)in[0] | ((uint32_t)in[1] << 8) | ((uint32_t)in[2] << 16) |
           ((uint32_t)in[3] << 24);
}

static inline uint64_t vsr_io_get_u64(const unsigned char *in)
{
    return (uint64_t)vsr_io_get_u32(in) |
           ((uint64_t)vsr_io_get_u32(in + 4) << 32);
}

/* -------------------------------------------------------------------------
 * Regions and limits
 * ---------------------------------------------------------------------- */

/* Bump allocator over one region. Allocation never fails silently: NULL
 * means the region is exhausted, which a correctly sized region (below)
 * never is for input within the limits. */
struct vsr_io_bump {
    unsigned char *base;
    size_t size;
    size_t used;
};

void vsr_io_bump_init(struct vsr_io_bump *bump, void *base, size_t size);
void *vsr_io_bump_alloc(struct vsr_io_bump *bump, size_t size,
                        size_t alignment);

/*
 * Bytes of the host graph of the largest MESSAGE the limits allow: the
 * envelope, the largest body (a PREPARE of batch_entries entries, or a log
 * state with entries, a two-membership epoch and a checkpoint with its own
 * epoch and manifest span), with one span per blob. Checked arithmetic;
 * ELIMIT on overflow.
 */
int vsr_io_codec_message_region(const struct vsr_limits *limits, size_t *bytes);
/* Bytes of the host graph of the largest LOAD result: a vsr_loaded plus a
 * recovered row with hard state, epoch, anchor and manifest bytes, or
 * batch_entries entries,
 * or one client record, each with one span per blob. */
int vsr_io_codec_load_region(const struct vsr_limits *limits, size_t *bytes);
/* Largest encoded frame (header included) any message within the limits
 * produces; slab_bytes must be at least this. */
int vsr_io_codec_frame_limit(const struct vsr_limits *limits, uint64_t *bytes);
/*
 * Largest encoded store record any transaction within the limits produces
 * (docs/io-design.md decision 36): header, VSR_MAX_STORE_CHANGES
 * descriptors, an APPEND of batch_entries entries whose bodies total
 * message_bytes, a CLIENTS of batch_entries records of result_bytes, a
 * HARD_STATE with two memberships of `members`, a PUBLISH and a RESTORE
 * each with a two-membership epoch and manifest_bytes, and an IDENTITY.
 */
int vsr_io_codec_record_limit(const struct vsr_limits *limits, uint64_t *bytes);
/* Meaningful bytes of the largest segment header (state included). */
int vsr_io_codec_segment_limit(const struct vsr_limits *limits, size_t *bytes);

/* -------------------------------------------------------------------------
 * Frames
 * ---------------------------------------------------------------------- */

/* Writes a frame header for a body of `length` bytes with body CRC `crc`
 * into 24 bytes at out. */
void vsr_io_codec_put_frame(unsigned char *out, uint16_t kind, uint32_t length,
                            uint32_t crc);
/* Reads and validates a frame header: magic, version, kind, length within
 * limit and a multiple of 8, header CRC. The body CRC is checked by the
 * caller once the body is present (vsr_io_cursor_crc). OK or EINVAL. */
int vsr_io_codec_get_frame(struct vsr_io_cursor *cursor, uint32_t limit,
                           struct vsr_io_wire_frame *out);

void vsr_io_codec_put_hello(unsigned char *out, uint32_t handshake,
                            uint32_t purpose, uint64_t node, uint64_t nonce);
int vsr_io_codec_get_hello(struct vsr_io_cursor *cursor,
                           struct vsr_io_wire_hello *out);

/* Stream request, chunk and end frames; payload bytes follow the header in
 * the same body and are referenced as a span by the decoder. */
void vsr_io_codec_put_stream_request(unsigned char *out, uint32_t length);
int vsr_io_codec_get_stream_request(struct vsr_io_cursor *cursor,
                                    struct vsr_span *request);
void vsr_io_codec_put_stream_chunk(unsigned char *out, uint64_t offset,
                                   uint32_t length);
int vsr_io_codec_get_stream_chunk(struct vsr_io_cursor *cursor,
                                  uint64_t *offset, struct vsr_span *payload);
void vsr_io_codec_put_stream_end(unsigned char *out, uint64_t bytes,
                                 int32_t status);
int vsr_io_codec_get_stream_end(struct vsr_io_cursor *cursor,
                                struct vsr_io_wire_stream_end *out);
void vsr_io_codec_put_library_request(
    unsigned char *out, const struct vsr_io_wire_library_request *request);
/* EINVAL when the bytes do not start with VSR_IO_LIBRARY_MAGIC or are
 * malformed. */
int vsr_io_codec_get_library_request(const struct vsr_span *request,
                                     struct vsr_io_wire_library_request *out);

/* -------------------------------------------------------------------------
 * Messages
 * ---------------------------------------------------------------------- */

#define VSR_IO_INLINE_BYTES 256u

/*
 * Digest of a message about to be sent: the encoded body length (envelope
 * included) and its CRC32C, computed in one pass that is the only time the
 * codec touches payload bytes. The message must be structurally valid for
 * the limits (the core emitted it); ELIMIT when the body exceeds
 * frame_limit, EINVAL for a malformed graph.
 */
int vsr_io_codec_message_digest(const struct vsr_message *message,
                                const struct vsr_limits *limits,
                                uint32_t *length, uint32_t *crc);

/* Writer over a contiguous header area, typically half a send slab. */
struct vsr_io_writer {
    unsigned char *base;
    size_t capacity;
    size_t used;
};

/*
 * Incremental vectored encoder. begin binds a message whose digest is
 * known; emit produces the next vectors of its frame: header runs are
 * written to `writer` and referenced as vectors, payload spans are
 * referenced in place (or copied into the writer below VSR_IO_INLINE_BYTES),
 * padding comes from the writer. emit stops when the vector array, the
 * writer or `budget` bytes are exhausted, or when the message is complete
 * (*done). A message may therefore span several sends; the link resumes
 * with the same encoder. Vectors reference bytes that stay pinned by the
 * SEND op until its completion.
 */
struct vsr_io_encoder {
    const struct vsr_message *message;
    uint32_t length; /* Body length from the digest. */
    uint32_t crc;
    uint32_t stage; /* Private position: object, entry, span, offset. */
    uint32_t entry;
    uint32_t span;
    uint32_t reserved;
    uint64_t offset;
};

void vsr_io_encoder_begin(struct vsr_io_encoder *encoder,
                          const struct vsr_message *message, uint32_t length,
                          uint32_t crc);
int vsr_io_encoder_emit(struct vsr_io_encoder *encoder,
                        struct vsr_io_writer *writer, struct vsr_io_vec *vecs,
                        uint32_t capacity, uint32_t *count, uint64_t budget,
                        bool *done);

/*
 * Decodes one MESSAGE frame body into a host graph allocated from `region`.
 * The cursor covers exactly the body; every blob becomes a single span into
 * the cursor's piece (a span that would cross pieces is EINVAL, until
 * multi-piece bodies are delivered). Limits checked: members per
 * membership, entries per batch, command/result/manifest/message bytes.
 * Returns OK with *out in the region, EINVAL or ELIMIT.
 */
int vsr_io_codec_decode_message(struct vsr_io_cursor *cursor,
                                const struct vsr_limits *limits,
                                struct vsr_io_bump *region,
                                struct vsr_message **out);

/* -------------------------------------------------------------------------
 * Store records, superblocks, segment headers
 * ---------------------------------------------------------------------- */

/* Encoded record bytes for a transaction (header, descriptors, payload,
 * padding); ELIMIT above record_limit or on overflow. */
int vsr_io_codec_record_bytes(const struct vsr_store *transaction,
                              const struct vsr_limits *limits, size_t *bytes);
/* Copies the record into out, which holds at least record_bytes; every
 * payload span is copied. generation and run stamp the header as the store
 * requires; flushed is the durable sequence acknowledged to the core when
 * the record is packed (decision 50). Returns OK or EINVAL for a malformed
 * graph. */
int vsr_io_codec_put_record(const struct vsr_store *transaction,
                            uint64_t generation, uint32_t run, uint64_t flushed,
                            unsigned char *out, size_t capacity,
                            size_t *written);
void vsr_io_codec_put_pad(unsigned char *out, uint32_t length);

/*
 * Record scanning. get_record reads and validates a record header at the
 * cursor: magic, length within limit and a multiple of 8, header CRC. It
 * reports PAD and zero fill through *kind without consuming beyond the
 * header. The payload CRC is checked by vsr_io_codec_check_record over the
 * whole record. Decoders for change payloads take a cursor positioned at
 * the payload and allocate from a region (LOAD results) or decode into
 * caller structs (recovery state).
 */
enum vsr_io_scan_kind {
    VSR_IO_SCAN_RECORD,
    VSR_IO_SCAN_PAD,
    VSR_IO_SCAN_END /* Zero fill, foreign magic, or short. */
};

int vsr_io_codec_get_record(struct vsr_io_cursor *cursor, uint64_t limit,
                            struct vsr_io_wire_record *out, uint32_t *kind);
/* Payload CRC over a complete record at the cursor; false on mismatch. */
bool vsr_io_codec_check_record(const struct vsr_io_cursor *cursor,
                               const struct vsr_io_wire_record *header);
int vsr_io_codec_get_change(struct vsr_io_cursor *cursor,
                            struct vsr_io_wire_change *out);
/* Decodes count entries into an array from the region; entry bodies become
 * spans into the cursor's bytes. */
int vsr_io_codec_get_entries(struct vsr_io_cursor *cursor, uint32_t count,
                             const struct vsr_limits *limits,
                             struct vsr_io_bump *region,
                             struct vsr_entry **entries);
/* Skips `skip` entries then decodes one; the LOAD path uses it to reach an
 * entry index inside an APPEND without building the whole batch. */
int vsr_io_codec_get_entry_at(struct vsr_io_cursor *cursor, uint32_t skip,
                              const struct vsr_limits *limits,
                              struct vsr_io_bump *region,
                              struct vsr_entry *entry);
int vsr_io_codec_get_client_record(struct vsr_io_cursor *cursor,
                                   const struct vsr_limits *limits,
                                   struct vsr_io_bump *region,
                                   struct vsr_client_record *record);
/* Reads only the fixed part of a client record and skips its bytes; the
 * client index rebuild needs identity, number and op, not the result. */
int vsr_io_codec_skip_client_record(struct vsr_io_cursor *cursor,
                                    struct vsr_io_wire_client_record *out);
int vsr_io_codec_get_hard_state(struct vsr_io_cursor *cursor,
                                const struct vsr_limits *limits,
                                struct vsr_io_bump *region,
                                struct vsr_hard_state *hard);
int vsr_io_codec_get_checkpoint(struct vsr_io_cursor *cursor,
                                const struct vsr_limits *limits,
                                struct vsr_io_bump *region,
                                struct vsr_checkpoint **checkpoint);
int vsr_io_codec_get_identity(struct vsr_io_cursor *cursor,
                              struct vsr_store_identity *identity);

/* Superblock: one block. put fills the fixed part and zeroes the rest of
 * the block; get validates magic, format and CRC. */
void vsr_io_codec_put_superblock(const struct vsr_io_wire_superblock *in,
                                 unsigned char *block, uint32_t block_bytes);
int vsr_io_codec_get_superblock(const unsigned char *block,
                                uint32_t block_bytes,
                                struct vsr_io_wire_superblock *out);

/*
 * Segment header. The logical state is passed as host structs; NULL
 * identity means a fresh store (STATE clear), NULL checkpoint means no
 * anchor. put writes `header_bytes` bytes (zero after the CRC); get
 * validates and decodes the state into the region.
 */
struct vsr_io_segment_state {
    const struct vsr_store_identity *identity;
    const struct vsr_hard_state *hard;
    const struct vsr_checkpoint *checkpoint;
    uint64_t log_begin;
    uint64_t log_end;
    uint64_t client_base;
    uint64_t last_sequence;
    uint64_t durable_floor;
};

int vsr_io_codec_put_segment(const struct vsr_io_wire_segment *fixed,
                             const struct vsr_io_segment_state *state,
                             unsigned char *out, size_t header_bytes,
                             size_t *written);
int vsr_io_codec_get_segment(const unsigned char *header, size_t header_bytes,
                             const struct vsr_limits *limits,
                             struct vsr_io_bump *region,
                             struct vsr_io_wire_segment *fixed,
                             struct vsr_store_identity *identity,
                             struct vsr_hard_state *hard,
                             struct vsr_checkpoint **checkpoint);

/* -------------------------------------------------------------------------
 * Clients file
 * ---------------------------------------------------------------------- */

/* Formats "clients-<32 hex>" into name[VSR_IO_CLIENTS_NAME_BYTES]. */
void vsr_io_codec_clients_name(struct vsr_id snapshot, char *name);
void vsr_io_codec_put_clients_header(
    const struct vsr_io_wire_clients_header *in, unsigned char *out);
int vsr_io_codec_get_clients_header(struct vsr_io_cursor *cursor,
                                    struct vsr_io_wire_clients_header *out);
/* Bytes one record occupies in the file: header, padded result, CRC. */
size_t vsr_io_codec_clients_record_bytes(uint64_t result_bytes);
/* Writes the record header, copies the result spans, pads and appends the
 * record CRC; out holds at least clients_record_bytes. */
void vsr_io_codec_put_clients_record(const struct vsr_client_record *record,
                                     unsigned char *out);
/* Verifies the record CRC and decodes; the result span points into the
 * cursor's bytes. EINVAL on a bad CRC or shape. */
int vsr_io_codec_get_clients_record(struct vsr_io_cursor *cursor,
                                    const struct vsr_limits *limits,
                                    struct vsr_io_bump *region,
                                    struct vsr_client_record *record);
/* The trailer (16 bytes) of a file of `count` records; before is the
 * running CRC32C (vsr_io_crc32c's value) of the header and of every record
 * without its own CRC (wire.h), which the trailer's crc extends over its
 * magic and count. */
void vsr_io_codec_put_clients_trailer(uint32_t count, uint32_t before,
                                      unsigned char *out);
/* Decodes a trailer and checks it against `before` (as above): EINVAL for a
 * short cursor, another magic, a crc that is not the file's, or a nonzero
 * reserved field. */
int vsr_io_codec_get_clients_trailer(struct vsr_io_cursor *cursor,
                                     uint32_t before, uint32_t *count);

#endif /* VSR_IO_CODEC_H */
