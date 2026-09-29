#ifndef VSR_IO_SNAPSHOT_H
#define VSR_IO_SNAPSHOT_H

#include "io/store.h"
#include "vsr-io.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Joint snapshot operations: the library's half of CAPTURE, SNAPSHOT_SYNC,
 * FETCH and DROP (docs/io-implementation.md, "Snapshots"; docs/io-design.md
 * decision 43). The engine intercepts these core ops; this module does the
 * clients-file work, forwards the op to the caller, and completes to the
 * core only when both halves are done. INSTALL is forwarded untouched.
 *
 *   CAPTURE  Generates the snapshot id from executor entropy, snapshots the
 *            client table at the task's sequence (before any later STORE),
 *            starts writing clients-<id> (header, records with result bytes
 *            read from the tail ring or cold records through a staging
 *            slab, trailer), and forwards the op with a template whose id
 *            is the generated one. Completion to the core once the file is
 *            written and the caller completed; on either failure the file
 *            is unlinked and the caller's status (or FAILED) reported.
 *   SYNC     fdatasync of clients-<id> then of the directory; forwards; the
 *            core sees OK when both halves are durable.
 *   FETCH    Opens a library stream to task.peer's node for the file
 *            (vsr_io_wire_library_request CLIENTS), receives it into
 *            clients-<id>.tmp, verifies every record CRC, count and trailer
 *            as chunks arrive, renames it to clients-<id> and only then
 *            forwards FETCH to the caller. A lost stream completes the core
 *            op with RETRY without involving the caller.
 *   DROP     Forwards; on the caller's completion unlinks clients-<id>
 *            (not while a served stream still reads it) and completes.
 *
 * The module also serves the source side of library streams (opening the
 * file and feeding FILE writes to the stream module) and loads base files
 * for the store (RECOVERY anchor, RESTORE, PUBLISH of an id that is not the
 * latest capture), record by record through vsr_io_store_base_record. The
 * module's poll starts a base load itself whenever the store reports one
 * wanted (vsr_io_store_base_wanted), so the engine has nothing to call.
 *
 * Registry: one entry per snapshot id the replica holds locally, mirroring
 * the core's local holds (core.limits.transfers + 4 entries); an entry is
 * created by CAPTURE, by FETCH, or by the base load of a recovered anchor
 * (the core holds it after RECOVERY), and freed by DROP.
 *
 * File slots (vsr_io_limits.file_slots): per replica the module keeps open
 * the directory (fsync at SYNC), the current base file (store.base_slot:
 * cold CLIENT loads at or below the client base read it) and the latest
 * capture's file (a PUBLISH of it makes it the base at once, with no open
 * in between), plus one transient file: a capture or fetch being written,
 * or a base file being loaded. A served stream opens the file once more
 * into the slot budgeted per stream.
 *
 * Executor records: every record the module submits uses slot kind
 * VSR_IO_SLOT_CLIENTS with owner = replica index, so the engine routes
 * CLIENTS completions to vsr_io_snapshots_complete(io, slot->owner, ...)
 * and FILE stays the store's. At most VSR_IO_SNAPSHOT_FILEOPS records are
 * in flight per replica. A step that finds no slab, slot or file slot free
 * arms the replica's CAPTURE deadline and retries at the poll that follows
 * it; the popped deadline needs no call.
 */

#define VSR_IO_SNAPSHOT_FILEOPS 4u
#define VSR_IO_SNAPSHOT_PATH_BYTES 4096u
#define VSR_IO_SNAPSHOT_EXTRA 4u /* Registry entries beyond transfers. */

enum vsr_io_snapshot_state {
    VSR_IO_SNAPSHOT_FREE,
    VSR_IO_SNAPSHOT_WRITING, /* CAPTURE: file being written. */
    VSR_IO_SNAPSHOT_WRITTEN, /* File complete, not durable. */
    VSR_IO_SNAPSHOT_SYNCING, /* fdatasync file, then directory. */
    VSR_IO_SNAPSHOT_DURABLE,
    VSR_IO_SNAPSHOT_FETCHING, /* Library stream in progress into .tmp. */
    VSR_IO_SNAPSHOT_DROPPING  /* Unlink pending or waiting for readers. */
};

/* The file work an entry is doing; the step within it is private. */
enum vsr_io_snapshot_job {
    VSR_IO_SNAPSHOT_JOB_NONE,
    VSR_IO_SNAPSHOT_JOB_WRITE,   /* CAPTURE: open, chunks, trailer. */
    VSR_IO_SNAPSHOT_JOB_DISCARD, /* Close and unlink a failed file. */
    VSR_IO_SNAPSHOT_JOB_LOAD,    /* Base load: open, statx, reads. */
    VSR_IO_SNAPSHOT_JOB_FETCH,   /* Open .tmp, chunk writes, rename. */
    VSR_IO_SNAPSHOT_JOB_SYNC,    /* (Open,) fdatasync, (close,) fsync dir. */
    VSR_IO_SNAPSHOT_JOB_DROP,    /* Close the kept slot, unlink. */
    VSR_IO_SNAPSHOT_JOB_RELEASE  /* Close a kept slot no longer needed. */
};

struct vsr_io_snapshot {
    struct vsr_id id;      /* Zero: free. */
    uint32_t state;        /* enum vsr_io_snapshot_state */
    uint32_t readers;      /* Served streams reading the file. */
    uint64_t sequence;     /* Local sequence at which the file is authoritative:
                          the capture task's, or the RESTORE that adopted a
                          fetched one; 0 until known. */
    uint64_t bytes;        /* File size once complete (served as one write). */
    uint64_t op;           /* Core op in progress on it, or 0. */
    uint64_t forwarded;    /* Op id forwarded to the caller (the core's own
                           id), or 0. */
    uint32_t op_type;      /* VSR_OP_SNAPSHOT_* of `op`. */
    uint32_t forward_due;  /* The op is still to be forwarded (poll). */
    int32_t caller_status; /* Caller's half; -1 while outstanding. */
    int32_t library_status; /* Library's half; -1 while outstanding. */
    int32_t file_slot;      /* Engine file slot the entry keeps open (the
                               latest capture, the base), or -1. */
    uint32_t job;           /* enum vsr_io_snapshot_job */
    uint32_t step;          /* Private step of the job. */
    uint32_t fileop;        /* File operation in flight, or NONE. */
    uint32_t tmp_slot;      /* Engine file slot the job opened, or NONE. */
    uint32_t unlink_due;    /* DROP done at the caller; unlink once no
                               reader remains. */
    uint32_t on_disk;       /* clients-<id> exists (created, renamed or
                               found by a load); 0 once unlinked. */
    uint32_t sync_failed;   /* SYNC: the fdatasync failed; reported once
                               the transient slot is closed. */
    uint32_t job_next;      /* JOB_SYNC to start once the RELEASE whose
                               CLOSE is in flight completes, or NONE. */
    uint32_t lease;         /* CAPTURE, FETCH: the engine lease reserved
                               when the op was taken; the caller's
                               checkpoint is copied into its region and the
                               OK completion carries it. NONE otherwise. */
    uint32_t reserved;
    const struct vsr_snapshot_task *task; /* The core op's task, pinned
                                             until the op completes. */
    const struct vsr_checkpoint *result;  /* The caller's checkpoint copied
                                             into the lease region, or NULL. */
};

/* One executor record of the module in flight: its slot-table index and
 * the path bytes the record borrows until it completes. */
struct vsr_io_snapshot_fileop {
    uint32_t used;
    uint32_t slot; /* Slot-table index. */
    char path[VSR_IO_SNAPSHOT_PATH_BYTES];
    char path2[VSR_IO_SNAPSHOT_PATH_BYTES]; /* RENAMEAT's new name. */
};

/* Streaming writer of a clients file (CAPTURE). */
struct vsr_io_clients_writer {
    uint32_t snapshot;    /* Registry index, or NONE when idle. */
    uint32_t slab;        /* Staging slab; chunks are written from it. */
    uint32_t staged;      /* Bytes staged in the slab. */
    uint32_t next;        /* Next table entry to write. */
    uint32_t count;       /* Entries in the table. */
    uint32_t cold_slab;   /* Slab holding a cold record, or NONE. */
    uint32_t cold_valid;  /* The cold slab holds entry `next`'s bytes. */
    uint32_t cold_bytes;  /* Bytes the cold read returned. */
    uint64_t cold_offset; /* File offset the cold read started at. */
    uint64_t file_offset; /* Bytes written to the file so far. */
    uint64_t header_op;
    uint64_t header_sequence;
    uint32_t trailer_staged;
    uint32_t aborted; /* Stop staging; close and unlink once idle. */
    struct vsr_io_client_snapshot *table; /* [max_clients] */
};

/* Streaming reader of a clients file (base loads, FETCH verification). */
struct vsr_io_clients_reader {
    uint32_t snapshot;    /* Registry index, or NONE when idle. */
    uint32_t slab;        /* Staging slab: the bytes not yet parsed. */
    uint32_t filled;      /* Bytes in the slab. */
    uint32_t consumed;    /* Bytes of the slab already parsed. */
    uint32_t stage;       /* header, records, trailer, done, failed */
    uint32_t purpose;     /* 0 base load, 1 fetch verification */
    uint64_t file_offset; /* File offset of the slab's first byte. */
    uint64_t file_size;   /* UINT64_MAX until known. */
    uint32_t expected;    /* Records announced by the header. */
    uint32_t seen;
    uint64_t sequence; /* Base sequence to install at the end. */
    int32_t status;    /* Parse failure status, or OK. */
    uint32_t stream;   /* Library stream index for FETCH, or NONE. */
    uint32_t reads;    /* Reads in flight (0 or 1). */
    uint64_t stat[32]; /* struct statx scratch. */
};

/* A chunk of the fetch stream held until its write completed. */
struct vsr_io_snapshot_chunk {
    uint64_t op; /* DATA op id. */
    uint64_t offset;
    const unsigned char *data;
    uint32_t length;
    uint32_t slab;
};

/* The source side of one served library stream. */
struct vsr_io_snapshot_serve {
    uint32_t state;    /* 0 free, 1 opening, 2 open, 3 ended: closing */
    uint32_t snapshot; /* Registry index. */
    uint32_t slot;     /* Engine file slot, or NONE. */
    uint32_t fileop;   /* File operation in flight, or NONE. */
    uint32_t ended;    /* The stream ended; close the slot. */
    uint32_t reserved;
};

struct vsr_io_snapshots {
    struct vsr_io_snapshot *entries; /* [transfers + 4] */
    uint32_t count;
    uint32_t capture; /* Registry index of the CAPTURE in progress or NONE. */
    struct vsr_io_clients_writer writer;
    struct vsr_io_clients_reader reader;
    struct vsr_io_snapshot_fileop *fileops; /* [VSR_IO_SNAPSHOT_FILEOPS] */
    struct vsr_io_snapshot_chunk *chunks;   /* [stream_window] ring */
    uint32_t chunks_head;
    uint32_t chunks_count;
    uint32_t chunks_capacity;
    uint32_t chunk_writing; /* 1 while the head chunk's write is out. */
    struct vsr_io_snapshot_serve *serves; /* [limits.streams] by stream */
    uint32_t serves_count;
    uint32_t dir_state;  /* 0 closed, 1 opening, 2 open, 3 failed */
    uint32_t dir_slot;   /* Engine file slot of the directory, or NONE. */
    uint32_t dir_fileop; /* Its open in flight, or NONE. */
    uint32_t retry;      /* 1 when a step waited for a resource since the
                            last poll: the CAPTURE deadline is armed. */
    uint32_t closed;
    uint32_t max_clients;
    int32_t error;         /* Last errno of the module's I/O, or zero. */
    uint32_t pending_base; /* Registry index of a file loaded for a held
                              RESTORE or PUBLISH that the store has not
                              packed yet (its slot stays open), or NONE. */
    struct vsr_snapshot_task task;  /* Forwarded CAPTURE task copy. */
    struct vsr_checkpoint template; /* Its checkpoint with the new id. */
    unsigned char *template_region; /* Epoch and memberships copy. */
    size_t region_bytes;            /* Of a checkpoint copy. */
    unsigned char request[56];      /* The fetch's wire request, pinned
                                       until its stream ends. */
};

/* Bytes and alignment of the module's memory for the core limits, the
 * engine limits (stream_window, streams) and the store's max_clients;
 * ELIMIT on overflow. region_bytes is what a deep copy of a checkpoint
 * needs: the engine's lease regions (vsr_io_codec_load_region, which
 * holds a recovered row's checkpoint) must hold it. */
int vsr_io_snapshots_region_bytes(const struct vsr_limits *limits,
                                  size_t *bytes);
int vsr_io_snapshots_size(const struct vsr_limits *limits,
                          const struct vsr_io_limits *io_limits,
                          uint32_t max_clients, size_t *bytes,
                          size_t *alignment);
void vsr_io_snapshots_init(struct vsr_io_snapshots *snapshots, void *memory,
                           size_t size, const struct vsr_limits *limits,
                           const struct vsr_io_limits *io_limits,
                           uint32_t max_clients);

/* Core ops intercepted by the engine, in emission order. Returns OK when
 * taken, or a status to complete the core op with at once (RETRY when a
 * registry entry, a slab, a file slot, an engine lease or a stream cannot
 * be obtained now, or another CAPTURE or FETCH still uses the writer or
 * reader; FAILED for a task that contradicts the registry). A taken op is
 * forwarded to the caller from the module's poll (with the same op id) and
 * completed to the core through vsr_io_engine_complete_core when both
 * halves are done. An OK CAPTURE or FETCH completion carries the caller's
 * checkpoint deep-copied into an engine lease region (the core retains it
 * under that lease until its RELEASE op); every other completion carries
 * no data and no lease. A DROP of an id the registry does not hold is
 * forwarded like any other and unlinks clients-<id> if it exists. */
int vsr_io_snapshots_capture(struct vsr_io *io, uint32_t replica, uint64_t op,
                             const struct vsr_snapshot_task *task);
int vsr_io_snapshots_sync(struct vsr_io *io, uint32_t replica, uint64_t op,
                          const struct vsr_snapshot_task *task);
int vsr_io_snapshots_fetch(struct vsr_io *io, uint32_t replica, uint64_t op,
                           const struct vsr_snapshot_task *task);
int vsr_io_snapshots_drop(struct vsr_io *io, uint32_t replica, uint64_t op,
                          const struct vsr_snapshot_task *task);
/* The caller completed the forwarded half (a CORE COMPLETE event whose op
 * id was forwarded by this module). data is the caller's vsr_checkpoint
 * for CAPTURE and FETCH, copied into the op's lease region at once (the
 * engine may release the caller's lease right after); EINVAL when the id
 * is not outstanding. */
int vsr_io_snapshots_forwarded_done(struct vsr_io *io, uint32_t replica,
                                    uint64_t op, int32_t status,
                                    const void *data);
bool vsr_io_snapshots_owns(const struct vsr_io_snapshots *snapshots,
                           uint64_t forwarded_op);

/* Source side of a library stream (called by the stream module at the
 * request frame, before any file is open): resolves the request to a local
 * file (complete, with no CAPTURE outstanding on it; NOT_FOUND otherwise)
 * and returns OK to serve it, setting streams[stream].replica; the
 * module then opens the file into a slot and feeds one FILE write of the
 * whole file and CLOSE OK through vsr_io_streams_write / close, using
 * vsr_io_streams_handle(&io->streams, stream). Any other status refuses:
 * the stream sends END with that status and closes. */
int vsr_io_snapshots_serve(struct vsr_io *io, uint32_t stream,
                           const struct vsr_io_wire_library_request *request);
/* Requester side: chunks of the library stream owned by `snapshot`, each
 * completed with vsr_io_streams_data_done(op) once its bytes were used
 * (the slab stays pinned until then). The end is reported for both sides
 * of a library stream (a served one closes its file slot then). */
void vsr_io_snapshots_stream_data(struct vsr_io *io, uint32_t replica,
                                  uint32_t stream, uint64_t op, uint64_t offset,
                                  const struct vsr_span *bytes, uint32_t slab);
void vsr_io_snapshots_stream_end(struct vsr_io *io, uint32_t replica,
                                 uint32_t stream, int32_t status);

/* Base file loads for the store: starts reading clients-<id>; each record
 * goes to vsr_io_store_base_record and the end to base_end/base_resume.
 * RETRY when the reader is busy or no slab is free (poll retries by
 * itself); the engine need not call it. */
int vsr_io_snapshots_load_base(struct vsr_io *io, uint32_t replica,
                               struct vsr_id id, uint64_t sequence);

void vsr_io_snapshots_poll(struct vsr_io *io, uint32_t replica, uint64_t now);
void vsr_io_snapshots_prepare(struct vsr_io *io, uint32_t replica,
                              struct vsr_io_sqe *sqes, uint32_t capacity,
                              uint32_t *count);
void vsr_io_snapshots_complete(struct vsr_io *io, uint32_t replica,
                               uint32_t slot, const struct vsr_io_cqe *cqe);
/* Detach: EBUSY while a file operation is in flight or a served stream
 * reads a file; otherwise closes every file slot the module holds
 * (update_file -1 and the engine slot freed) and clears store.base_slot. */
int vsr_io_snapshots_close(struct vsr_io *io, uint32_t replica);

#endif /* VSR_IO_SNAPSHOT_H */
