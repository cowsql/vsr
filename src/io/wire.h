#ifndef VSR_IO_WIRE_H
#define VSR_IO_WIRE_H

#include "vsr-io.h"

#include <stdint.h>

/*
 * Byte layouts shared by the network codec, the store and the clients file
 * (docs/io-implementation.md, "Wire format" and "Store format"). Every
 * layout is little-endian with natural alignment and every structure here
 * is a multiple of 8 bytes, so that a sequence of them stays 8-aligned and
 * blobs, which follow their header and are padded to 8, keep the next header
 * aligned. The structs are the DOCUMENTATION of the layout and the source of
 * offsets and sizes; encoders write fields through the little-endian helpers
 * of codec.h and decoders read them through the cursor, never through a
 * struct cast over received bytes, because received bytes may be unaligned
 * or split across pieces.
 *
 * Presence of an optional object is a flag bit in the enclosing structure,
 * never a NULL encoding. Counts are 32-bit; byte lengths of blobs are 64-bit
 * as in vsr.h; lengths that describe wire structures (frames, records,
 * entries) are 32-bit because a frame fits one slab and a record fits one
 * segment.
 */

#define VSR_IO_WIRE_ALIGN 8u
#define VSR_IO_WIRE_PAD(size)                                                  \
    (((size) + (VSR_IO_WIRE_ALIGN - 1u)) & ~(size_t)(VSR_IO_WIRE_ALIGN - 1u))

/* -------------------------------------------------------------------------
 * Frames
 *
 * A connection begins with the 8-byte VSR_IO_WIRE_MAGIC preamble from the
 * dialer (vsr-io.h), then frames in both directions. A frame is a header, a
 * body of `length` bytes, and nothing else; length is a multiple of 8.
 * header_crc covers the 16 bytes before it; body_crc covers the body. The
 * decoder rejects a frame (and closes the link) on a bad magic, version,
 * kind, length above the link's frame limit, or either CRC.
 * ---------------------------------------------------------------------- */

#define VSR_IO_FRAME_MAGIC UINT32_C(0x314D5246) /* "FRM1" */
#define VSR_IO_FRAME_HEADER_BYTES 24u

enum vsr_io_frame_kind {
    VSR_IO_FRAME_HELLO = 1,          /* body: vsr_io_wire_hello */
    VSR_IO_FRAME_MESSAGE = 2,        /* body: vsr_io_wire_message + body */
    VSR_IO_FRAME_STREAM_REQUEST = 3, /* body: vsr_io_wire_stream_request */
    VSR_IO_FRAME_STREAM_CHUNK = 4,   /* body: vsr_io_wire_stream_chunk */
    VSR_IO_FRAME_STREAM_END = 5      /* body: vsr_io_wire_stream_end */
};

struct vsr_io_wire_frame {
    uint32_t magic;      /* VSR_IO_FRAME_MAGIC */
    uint16_t version;    /* VSR_IO_WIRE_VERSION */
    uint16_t kind;       /* enum vsr_io_frame_kind */
    uint32_t length;     /* Body bytes; multiple of 8. */
    uint32_t body_crc;   /* CRC32C of the body; 0 for an empty body. */
    uint32_t header_crc; /* CRC32C of bytes [0, 16). */
    uint32_t reserved;
};

/*
 * HELLO, the TRUSTED handshake. The dialer sends the preamble then HELLO;
 * the acceptor answers HELLO; the link is established on both sides once
 * each has the other's HELLO. purpose tells a peer link from a stream. nonce
 * comes from the executor's entropy and is reserved for the KEYED mode; in
 * TRUSTED mode it is echoed and otherwise ignored. handshake must equal the
 * receiver's configured mode and version its wire version, else the link is
 * refused by closing it.
 */
enum vsr_io_link_purpose { VSR_IO_PURPOSE_PEER = 1, VSR_IO_PURPOSE_STREAM = 2 };

struct vsr_io_wire_hello {
    uint32_t handshake; /* enum vsr_io_handshake_mode */
    uint32_t purpose;   /* enum vsr_io_link_purpose */
    uint64_t node;      /* Sender's node identity. */
    uint64_t nonce;
    uint64_t reserved;
};

/* MESSAGE: the core envelope, then the body encoding selected by type. */
struct vsr_io_wire_message {
    uint64_t cluster_hi;
    uint64_t cluster_lo;
    uint64_t epoch;
    uint64_t view;
    uint64_t from;
    uint32_t type; /* enum vsr_message_type */
    uint32_t flags;
    uint64_t number;
};

/*
 * Nested objects, in the order the decoder meets them. A BLOB is
 * vsr_io_wire_blob followed by size bytes and padding to 8; it decodes to a
 * vsr_blob with one span into the receiving slab (count 0 when size is 0).
 * A MEMBERSHIP is vsr_io_wire_membership followed by count members. An
 * EPOCH is vsr_io_wire_epoch, the current membership and, when
 * VSR_IO_WIRE_EPOCH_PREVIOUS is set, the previous one. A CHECKPOINT is
 * vsr_io_wire_checkpoint, its epoch and its manifest blob. An ENTRY is
 * vsr_io_wire_entry followed by its body: a blob for COMMAND, a membership
 * for RECONFIGURE, a vsr_io_wire_check_epoch for CHECK_EPOCH, nothing for
 * NOOP; body_length is the encoded body's byte count, so entries can be
 * skipped without decoding them. ENTRIES is vsr_io_wire_entries followed by
 * count entries. A LOG_STATE is vsr_io_wire_log_state, its epoch, its
 * entries and, when VSR_IO_WIRE_LOG_CHECKPOINT is set, its checkpoint.
 */
struct vsr_io_wire_blob {
    uint64_t size;
};

struct vsr_io_wire_member {
    uint64_t id;
    uint32_t role;
    uint32_t reserved;
};

struct vsr_io_wire_membership {
    uint64_t epoch;
    uint32_t count;
    uint32_t faults;
};

#define VSR_IO_WIRE_EPOCH_PREVIOUS 1u

struct vsr_io_wire_epoch {
    uint64_t boundary;
    uint32_t phase;
    uint32_t flags;
};

struct vsr_io_wire_checkpoint {
    uint64_t id_hi;
    uint64_t id_lo;
    uint64_t op;
    uint64_t view;
};

struct vsr_io_wire_check_epoch {
    uint64_t epoch;
};

struct vsr_io_wire_entry {
    uint64_t op;
    uint64_t epoch;
    uint64_t view;
    uint64_t client_hi;
    uint64_t client_lo;
    uint64_t number;
    uint32_t type;        /* enum vsr_request_type */
    uint32_t body_length; /* Encoded body bytes after this header. */
};

struct vsr_io_wire_entries {
    uint32_t count;
    uint32_t reserved;
};

struct vsr_io_wire_nonce {
    uint64_t incarnation_hi;
    uint64_t incarnation_lo;
    uint64_t counter;
};

struct vsr_io_wire_revision {
    uint64_t incarnation_hi;
    uint64_t incarnation_lo;
    uint64_t sequence;
};

#define VSR_IO_WIRE_LOG_CHECKPOINT 1u

struct vsr_io_wire_log_state {
    struct vsr_io_wire_revision revision;
    uint64_t view;
    uint64_t last_normal_view;
    uint64_t committed;
    uint64_t log_begin;
    uint64_t log_end;
    uint32_t flags;
    uint32_t reserved;
};

/*
 * Message bodies by type. NULL bodies encode as length 0.
 *   PREPARE:            vsr_io_wire_prepare, then ENTRIES
 *   DO_VIEW_CHANGE, START_VIEW: LOG_STATE
 *   RECOVERY, RECOVERY_RESPONSE: vsr_io_wire_recovery, then LOG_STATE when
 *                       VSR_IO_WIRE_RECOVERY_STATE is set
 *   GET_STATE, GET_LOG: vsr_io_wire_fetch
 *   NEW_STATE, LOG, STATE_UNAVAILABLE: vsr_io_wire_state_chunk, then
 *                       LOG_STATE
 *   START_EPOCH, NEW_EPOCH: EPOCH
 *   CHECKPOINT:         CHECKPOINT
 *   READ_PROBE, READ_ACK: vsr_io_wire_nonce
 */
struct vsr_io_wire_prepare {
    uint64_t committed;
};

#define VSR_IO_WIRE_RECOVERY_STATE 1u

struct vsr_io_wire_recovery {
    struct vsr_io_wire_nonce nonce;
    uint32_t flags;
    uint32_t reserved;
};

struct vsr_io_wire_fetch {
    struct vsr_io_wire_nonce nonce;
    struct vsr_io_wire_revision revision;
    uint64_t first;
    uint64_t end;
    uint64_t max_bytes;
    uint32_t max_entries;
    uint32_t reserved;
};

struct vsr_io_wire_state_chunk {
    struct vsr_io_wire_nonce nonce;
    uint64_t first;
    uint64_t next;
};

/*
 * Streams. The request frame carries the requester's bytes (at most
 * VSR_IO_STREAM_REQUEST_BYTES); a request beginning with
 * VSR_IO_LIBRARY_MAGIC is the engine's own (vsr_io_wire_library_request)
 * and is served by the engine rather than the caller. Chunks carry
 * `length` payload bytes at stream offset `offset`; the frame's body_crc
 * already covers them, so a chunk has no CRC of its own. END carries the
 * source's status and total byte count; a stream ends exactly once.
 */
struct vsr_io_wire_stream_request {
    uint32_t length; /* Request bytes following, padded to 8. */
    uint32_t reserved;
};

struct vsr_io_wire_stream_chunk {
    uint64_t offset;
    uint32_t length; /* Payload bytes following, padded to 8. */
    uint32_t reserved;
};

struct vsr_io_wire_stream_end {
    uint64_t bytes;
    int32_t status; /* enum vsr_io_status */
    uint32_t reserved;
};

/* VSR_IO_LIBRARY_MAGIC (vsr-io.h) is the reserved prefix. */
#define VSR_IO_LIBRARY_REQUEST_VERSION 1u

enum vsr_io_library_request_kind {
    VSR_IO_LIBRARY_CLIENTS = 1 /* The clients-<snapshot> file of a replica. */
};

struct vsr_io_wire_library_request {
    uint64_t magic; /* VSR_IO_LIBRARY_MAGIC */
    uint32_t version;
    uint32_t kind; /* enum vsr_io_library_request_kind */
    uint64_t cluster_hi;
    uint64_t cluster_lo;
    uint64_t replica; /* Source replica whose directory holds the file. */
    uint64_t snapshot_hi;
    uint64_t snapshot_lo;
};

/* -------------------------------------------------------------------------
 * Store file: superblocks, segment headers, records
 * ---------------------------------------------------------------------- */

#define VSR_IO_SUPERBLOCK_MAGIC UINT32_C(0x31425353) /* "SSB1" */
#define VSR_IO_SEGMENT_MAGIC UINT32_C(0x31474553)    /* "SEG1" */
#define VSR_IO_RECORD_MAGIC UINT32_C(0x31434552)     /* "REC1" */
#define VSR_IO_PAD_MAGIC UINT32_C(0x31444150)        /* "PAD1" */

/*
 * Superblock, one per block at file offsets 0 and block_bytes. The two
 * copies are written alternately; the valid one with the greater revision
 * wins. Written at creation, when the start segment changes, and when slots
 * grow; never per transaction. crc covers bytes [0, offsetof(crc)). The rest
 * of the block is zero.
 */
struct vsr_io_wire_superblock {
    uint32_t magic;  /* VSR_IO_SUPERBLOCK_MAGIC */
    uint32_t format; /* VSR_IO_STORE_FORMAT */
    uint64_t generation;
    uint64_t revision; /* Superblock write counter, from 1. */
    uint64_t cluster_hi;
    uint64_t cluster_lo;
    uint64_t replica;
    uint32_t durability;
    uint32_t block_bytes;
    uint64_t segment_bytes;
    uint32_t header_blocks; /* Blocks of every segment header. */
    uint32_t slots;         /* Segment slots allocated in the file. */
    uint64_t start_segment; /* Oldest live segment number; 0 when none. */
    uint32_t start_slot;    /* Its slot. */
    uint32_t run;           /* Opens of the store; every record carries the
                               run that wrote it (see records). */
    uint64_t durable_floor; /* Acknowledged durable sequence when written. */
    uint32_t crc;
    uint32_t reserved;
};

/*
 * Segment header: the first header_blocks blocks of a slot. `length` bytes
 * from the start are meaningful and end with the CRC; the rest of the header
 * blocks is zero. The state is the store's logical state as of the segment's
 * first record, that is after the last record of the previous segment:
 * identity, log bounds, hard state (with both memberships of its epoch), the
 * anchor checkpoint (id, op, view, epoch, manifest), the client base and
 * the last sequence written before this segment. A fresh store's first
 * segment has VSR_IO_SEGMENT_STATE clear: no identity, no state.
 *
 * Layout after the fixed part, when VSR_IO_SEGMENT_STATE is set:
 *   vsr_io_wire_identity
 *   vsr_io_wire_hard_state, then its EPOCH
 *   when VSR_IO_SEGMENT_ANCHOR is set: CHECKPOINT
 *   uint32_t crc over everything before it
 */
#define VSR_IO_SEGMENT_STATE 1u
#define VSR_IO_SEGMENT_ANCHOR 2u

struct vsr_io_wire_segment {
    uint32_t magic;  /* VSR_IO_SEGMENT_MAGIC */
    uint32_t format; /* VSR_IO_STORE_FORMAT */
    uint64_t generation;
    uint64_t segment; /* Segment number, from 1, never reused. */
    uint32_t length;  /* Meaningful bytes, CRC included. */
    uint32_t flags;   /* VSR_IO_SEGMENT_* */
    uint32_t run;     /* Run that wrote the header. */
    uint32_t reserved;
    uint64_t last_sequence; /* Last sequence before this segment; 0 none. */
    uint64_t durable_floor; /* Acknowledged durable sequence when written. */
    uint64_t client_base;   /* Sequence at which the anchor's clients file
                               was authoritative; 0 when none. */
    uint64_t log_begin;
    uint64_t log_end;
};

struct vsr_io_wire_identity {
    uint64_t cluster_hi;
    uint64_t cluster_lo;
    uint64_t replica;
    uint32_t durability;
    uint32_t reserved;
};

struct vsr_io_wire_hard_state {
    uint64_t view;
    uint64_t last_normal_view;
    uint64_t committed;
    uint32_t state; /* enum vsr_hard_state_type */
    uint32_t role;  /* enum vsr_member_role */
};

/*
 * Record: one store transaction. length counts the header, the change
 * descriptors and the payload, padded to 8. header_crc covers bytes
 * [0, 44); payload_crc covers bytes [48, length). flushed is the durable
 * sequence acknowledged to the core when the record was packed: recovery
 * takes the maximum of it over every valid record, with the superblock's
 * and the segment headers' durable_floor, as the prefix that must be
 * intact (docs/io-implementation.md, "Recovery"; decision 50). A PAD is a
 * vsr_io_wire_pad whose length runs to the next block boundary; when fewer
 * than 8 bytes remain in a block they are zero and the scanner skips to the
 * next block without a marker. The scanner stops at the first header whose
 * magic is neither RECORD nor PAD, whose CRC fails, whose generation differs,
 * whose sequence is not the expected next one, or whose run is below the
 * run of the record before it. The run rule is what rejects a persisted
 * block of a torn write once the block before it has been rewritten by a
 * later run (docs/io-implementation.md, "Recovery").
 *
 * Change payloads, located by descriptor offset (from the record start) and
 * length:
 *   APPEND:   count ENTRY objects (no ENTRIES header)
 *   TRUNCATE, TRIM: none
 *   CLIENTS:  count vsr_io_wire_client_record objects, each followed by its
 *             result bytes padded to 8
 *   HARD_STATE: vsr_io_wire_hard_state, then EPOCH
 *   PUBLISH_CHECKPOINT, RESTORE_CHECKPOINT: CHECKPOINT
 *   IDENTITY: vsr_io_wire_identity
 */
struct vsr_io_wire_record {
    uint32_t magic;  /* VSR_IO_RECORD_MAGIC */
    uint32_t length; /* Total bytes, multiple of 8. */
    uint64_t sequence;
    uint64_t generation;
    uint64_t flushed; /* Durable sequence acknowledged when packed. */
    uint32_t count;   /* Change descriptors following the header. */
    uint32_t run;     /* Superblock run at the time of writing. */
    uint32_t payload_crc;
    uint32_t header_crc;
};

struct vsr_io_wire_pad {
    uint32_t magic;  /* VSR_IO_PAD_MAGIC */
    uint32_t length; /* Bytes to the next block boundary, this header
                        included; at least 8. */
};

struct vsr_io_wire_change {
    uint32_t type; /* enum vsr_change_type */
    uint32_t count;
    uint64_t first;
    uint32_t offset; /* Payload offset from the record start. */
    uint32_t length; /* Payload bytes. */
};

struct vsr_io_wire_client_record {
    uint64_t client_hi;
    uint64_t client_lo;
    uint64_t number;
    uint64_t op;
    int32_t code;
    uint32_t length; /* Result bytes following, padded to 8. */
};

/* -------------------------------------------------------------------------
 * Clients file: clients-<snapshot id as 32 lowercase hex digits, hi then lo>
 *
 * Header, then count records, each a vsr_io_wire_client_record with its
 * result bytes and a trailing uint32_t CRC32C over the record and its bytes
 * (padding included), then a vsr_io_wire_clients_trailer. The file is read
 * sequentially; a reader verifies the header CRC, every record CRC, the
 * count and the trailer, and reports CORRUPT otherwise. The header's
 * sequence is the writer's store sequence at capture and is informational
 * once the file is fetched by another replica.
 * ---------------------------------------------------------------------- */

#define VSR_IO_CLIENTS_MAGIC UINT32_C(0x31544C43) /* "CLT1" */
#define VSR_IO_CLIENTS_FORMAT 1u
#define VSR_IO_CLIENTS_NAME_BYTES 41u /* "clients-" + 32 hex + NUL */

struct vsr_io_wire_clients_header {
    uint32_t magic;  /* VSR_IO_CLIENTS_MAGIC */
    uint32_t format; /* VSR_IO_CLIENTS_FORMAT */
    uint64_t cluster_hi;
    uint64_t cluster_lo;
    uint64_t snapshot_hi;
    uint64_t snapshot_lo;
    uint64_t op;
    uint64_t sequence;
    uint32_t count;
    uint32_t crc; /* CRC32C of bytes [0, offsetof(crc)). */
};

struct vsr_io_wire_clients_trailer {
    uint32_t magic; /* VSR_IO_CLIENTS_MAGIC */
    uint32_t count; /* Records written; equals the header's. */
};

/* The layouts above are the contract; these checks pin them on every ABI
 * the library supports (LP64 little-endian). */
_Static_assert(sizeof(struct vsr_io_wire_frame) == VSR_IO_FRAME_HEADER_BYTES,
               "frame header is 24 bytes");
_Static_assert(sizeof(struct vsr_io_wire_hello) == 32, "hello is 32 bytes");
_Static_assert(sizeof(struct vsr_io_wire_message) == 56,
               "message envelope is 56 bytes");
_Static_assert(sizeof(struct vsr_io_wire_blob) == 8, "blob header is 8");
_Static_assert(sizeof(struct vsr_io_wire_member) == 16, "member is 16");
_Static_assert(sizeof(struct vsr_io_wire_membership) == 16,
               "membership header is 16");
_Static_assert(sizeof(struct vsr_io_wire_epoch) == 16, "epoch header is 16");
_Static_assert(sizeof(struct vsr_io_wire_checkpoint) == 32,
               "checkpoint header is 32");
_Static_assert(sizeof(struct vsr_io_wire_entry) == 56, "entry header is 56");
_Static_assert(sizeof(struct vsr_io_wire_entries) == 8, "entries header is 8");
_Static_assert(sizeof(struct vsr_io_wire_nonce) == 24, "nonce is 24");
_Static_assert(sizeof(struct vsr_io_wire_revision) == 24, "revision is 24");
_Static_assert(sizeof(struct vsr_io_wire_log_state) == 72,
               "log state header is 72");
_Static_assert(sizeof(struct vsr_io_wire_prepare) == 8, "prepare is 8");
_Static_assert(sizeof(struct vsr_io_wire_recovery) == 32, "recovery is 32");
_Static_assert(sizeof(struct vsr_io_wire_fetch) == 80, "fetch is 80");
_Static_assert(sizeof(struct vsr_io_wire_state_chunk) == 40,
               "state chunk header is 40");
_Static_assert(sizeof(struct vsr_io_wire_stream_request) == 8,
               "stream request header is 8");
_Static_assert(sizeof(struct vsr_io_wire_stream_chunk) == 16,
               "stream chunk header is 16");
_Static_assert(sizeof(struct vsr_io_wire_stream_end) == 16, "stream end is 16");
_Static_assert(sizeof(struct vsr_io_wire_library_request) == 56,
               "library request is 56");
_Static_assert(sizeof(struct vsr_io_wire_superblock) == 104,
               "superblock is 104");
_Static_assert(sizeof(struct vsr_io_wire_segment) == 80,
               "segment header is 80");
_Static_assert(sizeof(struct vsr_io_wire_identity) == 32, "identity is 32");
_Static_assert(sizeof(struct vsr_io_wire_hard_state) == 32, "hard state is 32");
_Static_assert(sizeof(struct vsr_io_wire_record) == 48, "record header is 48");
_Static_assert(sizeof(struct vsr_io_wire_pad) == 8, "pad is 8");
_Static_assert(sizeof(struct vsr_io_wire_change) == 24, "change is 24");
_Static_assert(sizeof(struct vsr_io_wire_client_record) == 40,
               "client record is 40");
_Static_assert(sizeof(struct vsr_io_wire_clients_header) == 64,
               "clients header is 64");
_Static_assert(sizeof(struct vsr_io_wire_clients_trailer) == 8,
               "clients trailer is 8");

#endif /* VSR_IO_WIRE_H */
