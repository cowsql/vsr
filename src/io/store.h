#ifndef VSR_IO_STORE_H
#define VSR_IO_STORE_H

#include "io/codec.h"
#include "vsr-io.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Store: the indexed log of one replica (docs/io-implementation.md, "Store"
 * and "Recovery"). A sans-IO planner over the on-disk layout of wire.h: it
 * consumes the core's LOAD, STORE, SYNC and RECLAIM ops in emission order,
 * packs records into the TAIL RING, keeps every index, plans writes,
 * flushes and reads as executor records through prepare, consumes their
 * completions, and queues the completions the core must see. It never
 * calls the executor and never allocates.
 *
 * Physical rules (docs/io-design.md decisions 33 and 35):
 *   - a block that was written is never rewritten; every write starts at a
 *     block boundary, covers whole blocks, and pads its last block with a
 *     PAD marker; the next write starts at the next block boundary;
 *   - records pack back to back inside a write; a record never crosses a
 *     segment boundary or a ring wrap;
 *   - the tail ring mirrors the file byte for byte over the ranges it
 *     holds (an EXTENT maps a ring range to a file range), so writes are
 *     issued straight from the ring with FIXED_BUFFER and hot LOAD results
 *     point into it;
 *   - a STORE completes when its record is packed and indexed; it is held
 *     only when its record would overwrite PINNED ring bytes (referenced by
 *     a LOAD lease, or not yet written) or when unwritten bytes exceed
 *     write_behind_bytes. The layout rule cache_bytes >= write_behind_bytes
 *     + pinned_payload_bytes + 2 * max_record_bytes + 2 * header_bytes +
 *     block_bytes, checked at attach, guarantees this wait always ends (a
 *     seal packs a segment header at the next block boundary, possibly
 *     after a ring wrap, before the record; decision 69);
 *   - every record carries `flushed`, the durable sequence acknowledged to
 *     the core when it was packed, and every superblock its durable_floor;
 *     recovery's floor F is the maximum over all of them and a scan that
 *     ends at or below F is CORRUPT (decision 50). When `durable` passes
 *     the floor of the newest superblock and no record is packed within
 *     flush_interval_ns (100 ms when zero), an idle superblock write
 *     records the new floor.
 *
 * Index rules:
 *   - the OP RING maps every op in [retained_begin, log_end) to its record;
 *     retained_begin is the oldest op any unreclaimed revision can still
 *     name, which RECLAIM advances; the ring holds max_entries and a STORE
 *     that would exceed it fails with FAILED (fenced, an invariant);
 *   - a version removed by TRUNCATE moves to the VERSIONS table with its
 *     validity range [appended, truncated) until RECLAIM passes it;
 *   - the CLIENT TABLE holds one entry per incarnation the store knows:
 *     its latest completed record, its latest retained entry, its offset
 *     in the current base file, and an in-flight flag for admitted
 *     requests not yet indexed; entries count against max_clients
 *     together. One version suffices because the engine routes the LOADs
 *     of a core update before its STOREs and never queues a LOAD behind a
 *     held STORE, so a CLIENT or REQUEST load is processed with `readable`
 *     equal to the sequence it names (decision 51);
 *   - the SEGMENT TABLE maps slots to segment numbers and sequence ranges;
 *     a slot is freed when its last sequence is below every floor: the
 *     record of the oldest retained entry, the RECLAIM revision, the client
 *     base plus one, and any capture still reading from it.
 */

#define VSR_IO_TRIM_HISTORY 64u /* TRIM/RESTORE events kept for RECLAIM. */

enum vsr_io_store_state {
    VSR_IO_STORE_CLOSED,
    VSR_IO_STORE_OPENING,    /* OPENAT/STATX of the log file. */
    VSR_IO_STORE_CREATING,   /* NEW/JOIN: fallocate, superblocks, header. */
    VSR_IO_STORE_RECOVERING, /* Superblocks, headers, scan, clients file. */
    VSR_IO_STORE_READY,
    VSR_IO_STORE_FAILED, /* Fenced; completes everything with FAILED. */
    VSR_IO_STORE_CLOSING /* Waiting for in-flight writes, then close. */
};

enum vsr_io_segment_phase {
    VSR_IO_SEGMENT_FREE,
    VSR_IO_SEGMENT_HEADER,  /* Header packed; its write not yet complete. */
    VSR_IO_SEGMENT_OPEN,    /* Records may be packed into it. */
    VSR_IO_SEGMENT_SEALED,  /* Full; records only read. */
    VSR_IO_SEGMENT_FREEING, /* Freed; reused only once the superblock
                               naming the new start segment is on disk. */
    VSR_IO_SEGMENT_FLUSHING /* That superblock write completed; in
                               FDATASYNC mode a flush issued since must
                               complete before the slot is reused
                               (decision S15). */
};

struct vsr_io_segment {
    uint64_t number; /* 0: free slot. */
    uint64_t first_sequence;
    uint64_t last_sequence;
    uint64_t used;         /* Bytes packed, header included. */
    uint32_t phase;        /* enum vsr_io_segment_phase */
    uint32_t header_write; /* Write index while HEADER, else NONE. */
    uint32_t run;          /* Run that wrote its header (the successor
                              rule of recovery, decision 48). */
    uint32_t reserved;
};

/* Op ring entry: the current version of one op. */
struct vsr_io_op_ref {
    uint64_t op;       /* 0: empty slot. */
    uint64_t sequence; /* Record that appended it. */
    uint64_t offset;   /* File offset of that record. */
    uint32_t length;   /* Record bytes. */
    uint32_t change;   /* Change index inside the record. */
    uint32_t index;    /* Entry index inside the APPEND. */
    uint32_t reserved;
    struct vsr_id client; /* Zero for NOOP entries. */
    uint64_t previous;    /* Previous retained op of the same client; 0. */
};

/* A truncated version kept for older revisions. */
struct vsr_io_version {
    uint64_t op;
    uint64_t appended;  /* Valid at revisions >= appended... */
    uint64_t truncated; /* ...and < truncated. */
    uint64_t offset;
    uint32_t length;
    uint32_t change;
    uint32_t index;
    uint32_t reserved;
    struct vsr_id client;
};

/* Location of a completed client record. */
struct vsr_io_client_version {
    uint64_t number; /* Request number; 0: none. */
    uint64_t op;
    uint64_t sequence; /* CLIENTS record; 0 when only the base file has it. */
    uint64_t offset;   /* Record offset in the log, or in the base file. */
    uint32_t length;
    uint32_t change;
    uint32_t index; /* Client record index inside the change. */
    uint32_t reserved;
};

struct vsr_io_client {
    struct vsr_id id; /* Zero: empty; VSR_IO_CLIENT_TOMBSTONE: deleted. */
    struct vsr_io_client_version current;
    uint64_t base_offset;    /* Record offset in the current base file, or
                                UINT64_MAX; valid when current.sequence is at
                                or below the client base. */
    uint64_t capture_offset; /* Offset written by the latest capture, or
                                UINT64_MAX (reset by snapshot_clients). */
    uint64_t next_offset;    /* Offset in the base file being loaded
                                (base_begin to base_end), or UINT64_MAX. */
    uint64_t retained;       /* Latest retained entry op; 0 none. */
    uint8_t inflight;        /* Admitted REQUEST, not yet indexed. */
    uint8_t reserved[7];
};

#define VSR_IO_CLIENT_TOMBSTONE_HI UINT64_MAX

/* Ring <-> file mapping; a FIFO of extents in packing order. An extent
 * never crosses a segment boundary or a ring wrap, so its file and ring
 * ranges are both contiguous. */
struct vsr_io_extent {
    uint64_t file_offset;
    uint64_t ring_offset;   /* Unwrapped: monotonic; wrapped = mod size. */
    uint64_t length;        /* Bytes mirrored; grows while newest. */
    uint64_t last_sequence; /* Last record ending inside it; 0 none. */
    uint32_t segment;       /* Slot the range belongs to. */
    uint32_t header;        /* 1 when it begins with the slot's header. */
};

/* One physical write of whole blocks from the ring. */
struct vsr_io_write {
    uint32_t slot;       /* Slot table index or NONE. */
    uint32_t state;      /* 0 free, 1 pending issue, 2 in flight, 3 complete. */
    uint64_t ring_begin; /* Unwrapped ring range. */
    uint64_t ring_end;
    uint64_t file_offset;
    uint64_t last_sequence; /* Last record ending inside it; 0 none. */
    uint32_t segment;       /* Slot written. */
    uint32_t header;        /* 1 when it carries a segment header. */
};

/* A held STORE completion. */
struct vsr_io_pending_store {
    uint64_t op;
    const struct vsr_store *transaction;
    uint64_t sequence;
    size_t bytes; /* Encoded record bytes. */
};

struct vsr_io_pending_sync {
    uint64_t op;
    uint64_t sequence;
};

/* One record a LOAD reads: resolved against the indexes when the LOAD is
 * accepted (decision 51), decoded when the bytes are at hand. */
struct vsr_io_load_ref {
    uint64_t offset;   /* Record offset in the log, or in the base file. */
    uint64_t sequence; /* Expected record sequence; 0 for a base record. */
    uint32_t length;   /* Record bytes; 0 for a base record (unknown). */
    uint32_t change;
    uint32_t index;
    uint32_t reserved;
};

/* A LOAD not yet completed: its records are in load_refs; hot ones wait
 * only for a lease, cold ones for the one read in flight per replica. */
struct vsr_io_pending_load {
    uint64_t op;
    struct vsr_store_read read;
    uint32_t state; /* 0 queued, 1 reading, 2 read, waiting for a lease. */
    uint32_t count; /* Records resolved (load_refs). */
    uint32_t slab;  /* Cold read slab or NONE. */
    uint32_t slot;  /* Its executor slot while reading. */
    uint32_t base;  /* 1: the record is read from the base file. */
    uint32_t reserved;
    uint64_t offset; /* Cold read: file range read into the slab. */
    uint64_t length;
    uint64_t got; /* Bytes the read returned. */
};

/* A ring range pinned by a LOAD lease. */
struct vsr_io_ring_pin {
    uint64_t begin; /* Unwrapped; UINT64_MAX when free. */
    uint64_t end;
};

/* Completion the engine turns into a COMPLETE event for the core. */
struct vsr_io_completion {
    uint64_t op;
    int32_t status;
    uint32_t lease; /* Engine lease index or NONE. */
    const void *data;
};

struct vsr_io_trim_event {
    uint64_t sequence;
    uint64_t log_begin;
};

/* Recovery state (section 6.4): one step at a time through prepare and
 * complete; the reads go into a pool slab, the superblocks into their
 * tail blocks, and the ring is scratch until the scan is over. */
struct vsr_io_recovery {
    uint32_t stage;         /* Private stage enumeration. */
    uint32_t slot;          /* Segment slot whose header is read, then
                               the one being scanned. */
    uint32_t slab;          /* Pool slab of the reads, or NONE. */
    uint32_t io_slot;       /* Executor slot of the read in flight, NONE. */
    uint32_t copy;          /* Superblock copy recovered from. */
    uint32_t last_slot;     /* Slot holding the last valid record, else
                               the start slot. */
    uint32_t mode;          /* Private: replaying the chain, or sweeping
                               for floors only (decision 50). */
    uint32_t record_run;    /* Run of the last valid record (the start
                            header's before one): a record of a later run
                            carries flushed >= its predecessor's sequence
                            (decision S20). */
    uint64_t offset;        /* File offset of the chunk in the slab. */
    uint64_t position;      /* File offset of the next record to judge. */
    uint64_t sequence;      /* Last valid record replayed. */
    uint32_t run;           /* Run of the last valid record (or header). */
    uint32_t retried;       /* 1 once the range at retry_offset was read
                               a second time (decision 50). */
    uint64_t retry_offset;  /* File offset of that range. */
    uint64_t durable_floor; /* F: max of the superblock's and headers'
                               floors and every valid record's flushed. */
    uint64_t resume;        /* File offset of the block after the last
                               valid record: where writing resumes. */
    uint64_t chain_resume;  /* While sweeping the dead tail of a block
                               (decision S14): the block boundary at which
                               the chain resumes; 0 when not. */
    uint64_t load_op;       /* The RECOVERY load op to complete. */
};

struct vsr_io_replica;

struct vsr_io_store {
    struct vsr_io_replica *replica; /* The replica embedding the store (set
                                       by init): LOAD results take its
                                       leases and its engine's pool. */
    struct vsr_io_store_options options;
    struct vsr_limits limits;
    uint32_t state;      /* enum vsr_io_store_state */
    uint32_t start_mode; /* enum vsr_start_mode */
    int32_t log_slot;    /* Registered file slot of the log; -1 none. */
    int32_t dir_fd;      /* AT_FDCWD or VSR_SIM_ROOT; paths are relative. */
    const char *path;    /* Directory, borrowed from attach. */
    uint64_t generation;
    uint32_t run;
    uint32_t header_blocks;
    uint64_t header_bytes;
    uint64_t max_record_bytes;
    uint64_t data_bytes; /* segment_bytes - header_bytes. */
    /* Logical state, as the segment header records it. */
    struct vsr_store_identity identity;
    bool identity_set;
    struct vsr_hard_state hard;   /* epoch points into hard_region. */
    struct vsr_checkpoint anchor; /* id zero when none. */
    unsigned char *state_region;  /* Copies of hard.epoch and the anchor's
                                    epoch/manifest; sized from limits. */
    uint64_t log_begin;
    uint64_t log_end;
    uint64_t retained_begin;
    uint64_t readable;    /* Last packed sequence. */
    uint64_t written;     /* Last sequence whose write completed, in order. */
    uint64_t durable;     /* Last sequence acknowledged by SYNC. */
    uint64_t flushed;     /* Last sequence covered by a completed flush. */
    uint64_t reclaim;     /* RECLAIM floor: oldest revision still needed. */
    uint64_t reclaimed;   /* The part of it applied: a crash brings back
                             any revision from the sequence on media on,
                             so RECLAIM applies as far as that sequence
                             (decision S16). */
    uint64_t client_base; /* Sequence of the current base file. */
    uint64_t client_base_floor;   /* The client base as of the sequence
                                     on media: the floor's base term. */
    uint64_t client_base_pending; /* Revision that set client_base while
                                     above the media sequence; 0 none. */
    struct vsr_id client_base_id;
    uint64_t clients_sequence;  /* Last CLIENTS change applied. */
    struct vsr_id last_capture; /* Snapshot whose offsets capture_offset
                                   holds. */
    uint64_t last_capture_sequence;
    uint64_t capture_floor; /* Lowest record a capture still reads; NONE. */
    uint64_t reindexed;     /* Sequence of the latest TRUNCATE, TRIM or
                               RESTORE: a CLIENT or REQUEST load naming an
                               older sequence completes with RETRY. */
    uint64_t restored;      /* Sequence of the latest RESTORE. */
    /* A RESTORE, or a PUBLISH of a snapshot other than the latest capture,
     * is held before it is packed until its clients file was loaded
     * (decision 43): base_wanted reports it, base_begin/record/end load
     * it, base_resume packs it or fails it. */
    uint32_t base_state; /* Private stage. */
    uint32_t base_kind;  /* 1 when the held transaction is a RESTORE. */
    uint64_t base_op;
    struct vsr_id base_id;
    uint64_t base_sequence;
    int32_t base_slot; /* Registered file slot of the current base file,
                          kept open by the snapshot module, or -1: a CLIENT
                          load of a record at or below the client base
                          reads the file. */
    int32_t reserved_base;
    /* Segments. */
    struct vsr_io_segment *segments; /* [max_segments] */
    uint32_t slots;                  /* Allocated in the file. */
    uint32_t current;                /* Slot being filled or NONE. */
    uint64_t next_segment;
    uint64_t start_segment;
    uint32_t start_slot;
    uint32_t superblock_pending; /* 1 while a superblock write is out. */
    uint32_t superblock_next;    /* Copy (0 or 1) to write next. */
    uint32_t superblock_slot;
    uint64_t superblock_revision;
    /* Tail ring. */
    unsigned char *ring;
    uint64_t ring_size;         /* cache_bytes */
    uint64_t head;              /* Unwrapped offset of the next packed byte. */
    uint64_t file_head;         /* File offset of the next packed byte. */
    uint64_t issued;            /* Unwrapped offset up to which writes were
                                planned (block aligned). */
    uint64_t retained_floor;    /* Oldest unwrapped byte still mirrored. */
    unsigned char *superblocks; /* Two blocks after the ring. */
    uint32_t region_index;      /* Executor region of the tail. */
    uint32_t extents_head;
    uint32_t extents_count;
    uint32_t extents_capacity;
    struct vsr_io_extent *extents;
    /* Write pipeline. */
    struct vsr_io_write *writes; /* [inflight_writes], issue order ring. */
    uint32_t writes_head;
    uint32_t writes_count;
    uint32_t flush_slot;       /* Outstanding fdatasync or NONE. */
    uint32_t flush_pending;    /* 1 when a flush must be issued; 2 when a
                                  SYNC asked for one and poll has yet to
                                  apply sync_delay_ns to it. */
    uint32_t freeing_flush;    /* FLUSHING slots wait for a flush: 1 one
                                  is wanted, 2 one issued since is out. */
    uint32_t flush_own;        /* 1 when the store wants a flush of its own
                                  (decisions S15, S16), issued without
                                  waiting for flush_target. */
    uint64_t flush_target;     /* Sequence the pending flush must cover. */
    uint64_t flush_deadline;   /* sync_delay / flush_interval expiry. */
    uint64_t superblock_floor; /* durable_floor of the newest superblock. */
    uint64_t idle_deadline;    /* Idle superblock write: armed on the FLUSH
                                  deadline handle when durable passes
                                  superblock_floor, for flush_interval_ns
                                  (100 ms when zero); fires only if no
                                  record was packed meanwhile. */
    uint32_t packed_since_durable;
    int32_t error;
    uint32_t superblock_dirty; /* 1 when the superblock must be rewritten. */
    uint32_t growth;           /* Private stage of a slot growth in flight. */
    char *log_path;            /* "<path>/log" for OPENAT; NULL: too long. */
    /* Indexes. */
    struct vsr_io_op_ref *ops;       /* [max_entries] */
    struct vsr_io_version *versions; /* [max_entries] */
    uint32_t versions_count;
    uint32_t clients_capacity; /* Power of two >= 2 * max_clients. */
    struct vsr_io_client *clients;
    uint32_t clients_count; /* Live entries, in-flight included. */
    uint32_t clients_tombstones;
    struct vsr_io_trim_event trims[VSR_IO_TRIM_HISTORY];
    uint32_t trims_head;
    uint32_t trims_count;
    /* Queues of core ops, in emission order. */
    struct vsr_io_pending_store *stores; /* [operations] ring */
    uint32_t stores_head;
    uint32_t stores_count;
    struct vsr_io_pending_sync *syncs; /* [operations] ring */
    uint32_t syncs_head;
    uint32_t syncs_count;
    struct vsr_io_pending_load *loads; /* [operations] ring */
    struct vsr_io_load_ref *load_refs; /* [operations * batch_entries] */
    uint32_t loads_head;
    uint32_t loads_count;
    uint32_t cold_active;         /* 1 while a cold read is in flight. */
    uint32_t cold_slab;           /* Its slab: released at its completion
                                     when no lease took it (the store was
                                     fenced meanwhile). */
    struct vsr_io_ring_pin *pins; /* [regions] by lease index. */
    uint32_t pins_count;
    struct vsr_io_completion *completions; /* [3 * operations] ring */
    uint32_t completions_head;
    uint32_t completions_count;
    struct vsr_io_recovery recovery;
    /* Pending file operations (open, statx, fallocate, close). */
    uint32_t file_slot;
    uint32_t file_op; /* Private enumeration of the step in flight. */
    uint64_t file_size;
    struct vsr_io_store_status stats;
};

/*
 * Layout checks and sizes. check applies the attach-time rules: block_bytes
 * a power of two >= 512; segment_bytes a multiple of block_bytes;
 * max_record_bytes (vsr_io_codec_record_limit) <= slab_bytes - 2 *
 * block_bytes and <= segment_bytes - header_bytes; cache_bytes a multiple of
 * block_bytes and >= write_behind_bytes + limits->pinned_payload_bytes +
 * 2 * max_record_bytes + 2 * header_bytes + block_bytes (decision 69);
 * segments >= 2, max_segments >= segments; inflight_writes >= 1;
 * max_entries >= batch_entries; max_clients >= 1; segment_bytes + 2 *
 * block_bytes and write_behind_bytes + max_record_bytes + block_bytes (the
 * largest single write) fit the executor's 32-bit lengths (decision 70).
 * Returns OK, EINVAL (a malformed value) or ELIMIT (a capacity rule).
 */
int vsr_io_store_check(const struct vsr_io_store_options *options,
                       const struct vsr_limits *limits, uint32_t slab_bytes);
/* Bookkeeping bytes in the replica's metadata region and the tail region
 * (cache_bytes + 2 blocks, block aligned). regions is the count of decode
 * regions, which bounds ring pins. */
int vsr_io_store_size(const struct vsr_io_store_options *options,
                      const struct vsr_limits *limits, uint32_t regions,
                      size_t *metadata_bytes, size_t *metadata_alignment,
                      size_t *tail_bytes, size_t *tail_alignment);
/* `store` must be the `store` member of a vsr_io_replica, which init
 * records in store->replica. */
void vsr_io_store_init(struct vsr_io_store *store, void *metadata,
                       size_t metadata_size, void *tail, size_t tail_size,
                       const struct vsr_io_store_options *options,
                       const struct vsr_limits *limits, uint32_t regions,
                       uint32_t region_index, const char *path, int dir_fd);

/*
 * Starts the asynchronous open: RECOVER opens and recovers, NEW and JOIN
 * create (an existing log is recovered under every mode: one with records
 * is reported to the RECOVERY load as a recovered row, which makes the
 * core fail NEW/JOIN as vsr.h requires; an empty one, like a missing log
 * under RECOVER, is NOT_FOUND and the store is READY over it, decision
 * 71). load_op is the core's RECOVERY LOAD, whose completion carries the
 * recovered row in a lease region, or the status: CORRUPT for a log the
 * scan cannot trust (section 6.4), FAILED for an I/O error. The recovery
 * wants the anchor's clients file through base_wanted (a FULL replica
 * with an anchor) before it completes; `read` carries nothing recovery
 * needs and is ignored.
 */
void vsr_io_store_open(struct vsr_io_store *store, uint32_t start_mode,
                       uint64_t load_op, const struct vsr_store_read *read);

/* Core ops. Within one core update the engine calls load for every LOAD
 * first, then store for every STORE, then sync and reclaim (decision 51);
 * a LOAD is answered against the current indexes at once, never behind a
 * held STORE. Each returns OK when accepted; the completion is queued
 * later through next_completion. EINVAL for a malformed op (a caller
 * bug). */
int vsr_io_store_load(struct vsr_io_store *store, uint64_t op,
                      const struct vsr_store_read *read);
int vsr_io_store_store(struct vsr_io_store *store, uint64_t op,
                       const struct vsr_store *transaction);
int vsr_io_store_sync(struct vsr_io_store *store, uint64_t op,
                      uint64_t sequence);
int vsr_io_store_reclaim(struct vsr_io_store *store, uint64_t op,
                         uint64_t oldest);
/* RELEASE of a LOAD lease: unpins its ring range; the engine frees the
 * region and drops the slab reference of a cold load, which the lease
 * holds. A LOAD completion carries the engine lease index holding the
 * vsr_loaded graph (an empty result too) and the pin or slab. */
void vsr_io_store_release(struct vsr_io_store *store, uint32_t lease);
/* Drains queued completions in order; false when none. */
bool vsr_io_store_next_completion(struct vsr_io_store *store,
                                  struct vsr_io_completion *out);

/*
 * Client admission (docs/io-design.md decision 27 and 43): true when the
 * incarnation is known (any field set) or a free entry exists within
 * max_clients, in which case an in-flight entry is created. replied clears
 * the in-flight flag; an entry with nothing else set is deleted.
 */
bool vsr_io_store_admit(struct vsr_io_store *store, struct vsr_id client);
void vsr_io_store_replied(struct vsr_io_store *store, struct vsr_id client);

/*
 * Capture support. snapshot copies the completed record of every entry
 * into `out` (id, number, op, record location; `capacity` should hold
 * max_clients, the count is returned regardless; a record only the base
 * file holds, because it never was in this log or because its slot was
 * freed under the base, has sequence 0 and the file offset), resets every
 * entry's capture_offset and sets capture_floor so their segments stay; the
 * snapshot module reads result bytes hot or cold, calls capture_offset
 * with each entry's offset in the file it writes and, at the end,
 * capture_end with the snapshot id and the sequence captured (a zero id
 * only clears the floor: an abandoned capture). base_set makes snapshot
 * `id` the client base at `sequence`: entries point at the file through
 * the latest capture's offsets when `id` is its id, else through the
 * offsets of the file loaded last (base_begin to base_end), and
 * client_base is `sequence` lowered below the record of any entry the
 * file does not cover. A PUBLISH of the latest capture calls it itself.
 */
struct vsr_io_client_snapshot {
    struct vsr_id id;
    struct vsr_io_client_version record;
};

uint32_t vsr_io_store_snapshot_clients(struct vsr_io_store *store,
                                       struct vsr_io_client_snapshot *out,
                                       uint32_t capacity);
void vsr_io_store_capture_offset(struct vsr_io_store *store,
                                 struct vsr_id client, uint64_t offset);
void vsr_io_store_capture_end(struct vsr_io_store *store, struct vsr_id id,
                              uint64_t sequence);
void vsr_io_store_base_set(struct vsr_io_store *store, struct vsr_id id,
                           uint64_t sequence);
/* A base file load, by the snapshot module: base_begin, then base_record
 * per file record (merged by request number: a greater one replaces the
 * entry's record with the file's, sequence 0; an equal one must name the
 * same op, else EINVAL; a lower one is ignored; a full table is ELIMIT),
 * then base_end. When a held RESTORE or PUBLISH waits for it (below) the
 * merge applies once base_resume packs the transaction; otherwise
 * (recovery) at once, as base_set. */
void vsr_io_store_base_begin(struct vsr_io_store *store);
int vsr_io_store_base_record(struct vsr_io_store *store,
                             const struct vsr_io_wire_client_record *record,
                             uint64_t file_offset);
void vsr_io_store_base_end(struct vsr_io_store *store, struct vsr_id id,
                           uint64_t sequence);
/* The RESTORE, or PUBLISH of a snapshot that is neither the latest
 * capture nor the current base, held before packing until its clients
 * file is read (decision 43; FULL role only): true while one waits for
 * the snapshot module to start the load, with its id and the
 * transaction's sequence. base_resume(OK) packs it (with the merged
 * table; a RESTORE then drops the entries the file lacks and rebuilds the
 * retained index); any other status fails it with that status and fences
 * the store. */
bool vsr_io_store_base_wanted(const struct vsr_io_store *store,
                              struct vsr_id *id, uint64_t *sequence);
void vsr_io_store_base_resume(struct vsr_io_store *store, int32_t status);

/*
 * Record access for LOAD results and the capture writer: locates the bytes
 * of a record; hot when the ring still holds them (a cursor over the ring,
 * pinned by `pin` until unpinned), cold when a read is needed (the caller
 * issues it into a slab and calls read_done). Records are never split
 * across a ring wrap, so a hot record is one piece.
 */
bool vsr_io_store_hot(const struct vsr_io_store *store, uint64_t offset,
                      uint32_t length, struct vsr_io_piece *piece);
uint32_t vsr_io_store_pin(struct vsr_io_store *store, uint64_t offset,
                          uint32_t length, uint32_t lease);
void vsr_io_store_unpin(struct vsr_io_store *store, uint32_t pin);

/* Floors and freeing (section 6.5): the floor is the lowest sequence any
 * reader still needs; free_segments frees every SEALED slot whose records
 * are all below it and completely written, marks the freed slots FREEING
 * until the superblock naming the new start segment completed, and never
 * frees a slot a queued cold load reads from. */
uint64_t vsr_io_store_free_floor(const struct vsr_io_store *store);
void vsr_io_store_free_segments(struct vsr_io_store *store);

/* Poll: due flush interval or sync delay (deadline kinds FLUSH, SYNC),
 * held STOREs that can now proceed, cold loads to start. Prepare: file
 * setup, header writes, record writes, superblock writes, flushes, cold
 * reads. Complete: dispatch by slot kind. */
void vsr_io_store_poll(struct vsr_io *io, uint32_t replica, uint64_t now);
void vsr_io_store_prepare(struct vsr_io *io, uint32_t replica,
                          struct vsr_io_sqe *sqes, uint32_t capacity,
                          uint32_t *count);
void vsr_io_store_complete(struct vsr_io *io, uint32_t replica, uint32_t slot,
                           const struct vsr_io_cqe *cqe);
uint64_t vsr_io_store_deadline(const struct vsr_io_store *store);

/* Detach: EBUSY while writes or a flush are in flight; otherwise clears
 * the file slot and marks the store closed. */
int vsr_io_store_close(struct vsr_io_store *store);
void vsr_io_store_status(const struct vsr_io_store *store,
                         struct vsr_io_store_status *status);

#endif /* VSR_IO_STORE_H */
