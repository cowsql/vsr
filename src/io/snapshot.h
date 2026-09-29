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
 * decision 42). The engine intercepts these core ops; this module does the
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
 * latest capture), record by record through vsr_io_store_base_record.
 *
 * Registry: one entry per snapshot id the replica holds locally, mirroring
 * the core's local holds (core.limits.transfers + 4 entries); an entry is
 * created by CAPTURE or FETCH and freed by DROP.
 */

enum vsr_io_snapshot_state {
    VSR_IO_SNAPSHOT_FREE,
    VSR_IO_SNAPSHOT_WRITING, /* CAPTURE: file being written. */
    VSR_IO_SNAPSHOT_WRITTEN, /* File complete, not durable. */
    VSR_IO_SNAPSHOT_SYNCING, /* fdatasync file, then directory. */
    VSR_IO_SNAPSHOT_DURABLE,
    VSR_IO_SNAPSHOT_FETCHING, /* Library stream in progress into .tmp. */
    VSR_IO_SNAPSHOT_DROPPING  /* Unlink pending or waiting for readers. */
};

struct vsr_io_snapshot {
    struct vsr_id id;      /* Zero: free. */
    uint32_t state;        /* enum vsr_io_snapshot_state */
    uint32_t readers;      /* Served streams and base loads reading the file. */
    uint64_t sequence;     /* Local sequence at which the file is authoritative:
                          the capture task's, or the RESTORE that adopted a
                          fetched one; 0 until known. */
    uint64_t op;           /* Core op in progress on it, or 0. */
    uint64_t forwarded;    /* Op id forwarded to the caller, or 0. */
    int32_t caller_status; /* Caller's half; -1 while outstanding. */
    int32_t library_status; /* Library's half; -1 while outstanding. */
    int32_t file_slot;      /* Registered slot while open; -1. */
    uint32_t reserved;
};

/* Streaming writer of a clients file (CAPTURE). */
struct vsr_io_clients_writer {
    uint32_t snapshot; /* Registry index. */
    uint32_t slab;     /* Staging slab; chunks are written from it. */
    uint32_t staged;   /* Bytes staged in the slab. */
    uint32_t slot;     /* Write or read slot in flight or NONE. */
    uint64_t file_offset;
    uint32_t next;      /* Next table entry to write. */
    uint32_t count;     /* Entries in the table. */
    uint32_t cold_slab; /* Slab holding a cold record, or NONE. */
    uint32_t pin;       /* Ring pin of a hot record, or NONE. */
    struct vsr_io_client_snapshot *table; /* [max_clients] */
};

/* Streaming reader of a clients file (recovery, RESTORE, FETCH verify). */
struct vsr_io_clients_reader {
    uint32_t snapshot;
    uint32_t slab;
    uint32_t slot;
    uint32_t stage; /* header, records, trailer, done */
    uint64_t file_offset;
    uint64_t file_size;
    uint32_t consumed; /* Bytes of the slab already parsed. */
    uint32_t available;
    uint32_t expected; /* Records announced by the header. */
    uint32_t seen;
    uint64_t sequence; /* Base sequence to install at the end. */
    uint32_t purpose;  /* 0 base load, 1 fetch verification */
    uint32_t stream;   /* Library stream index for FETCH. */
};

struct vsr_io_snapshots {
    struct vsr_io_snapshot *entries; /* [transfers + 4] */
    uint32_t count;
    uint32_t capture; /* Registry index of the CAPTURE in progress or NONE. */
    struct vsr_io_clients_writer writer;
    struct vsr_io_clients_reader reader;
    uint64_t pending_capture;      /* Core op id awaiting the store snapshot. */
    struct vsr_snapshot_task task; /* Forwarded CAPTURE task copy. */
    struct vsr_checkpoint template; /* Its checkpoint with the new id. */
    unsigned char *template_region; /* Epoch and memberships copy. */
    struct vsr_checkpoint result;   /* Caller's CAPTURE/FETCH result copy
                                       returned to the core. */
    unsigned char *result_region;
    uint32_t deadline; /* Retry deadline handle. */
    uint32_t reserved;
};

int vsr_io_snapshots_size(const struct vsr_limits *limits, uint32_t max_clients,
                          size_t *bytes, size_t *alignment);
void vsr_io_snapshots_init(struct vsr_io_snapshots *snapshots, void *memory,
                           size_t size, const struct vsr_limits *limits,
                           uint32_t max_clients);

/* Core ops intercepted by the engine, in emission order. Returns OK when
 * taken, or a status to complete the core op with at once (RETRY when a
 * registry entry or slab cannot be obtained now, FAILED for a task that
 * contradicts the registry). */
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
 * for CAPTURE and FETCH, copied into result_region. */
int vsr_io_snapshots_forwarded_done(struct vsr_io *io, uint32_t replica,
                                    uint64_t op, int32_t status,
                                    const void *data);
bool vsr_io_snapshots_owns(const struct vsr_io_snapshots *snapshots,
                           uint64_t forwarded_op);

/* Source side of a library stream (called by the stream module at the
 * request frame, before any file is open): resolves the request to a local
 * file and returns OK to serve it, setting streams[stream].replica; the
 * module then opens the file into a slot and feeds one FILE write of the
 * whole file and CLOSE OK through vsr_io_streams_write / close, using
 * vsr_io_streams_handle(&io->streams, stream). Any other status refuses:
 * the stream sends END with that status and closes. stream.c carries weak
 * stubs of these three functions until snapshot.c defines them. */
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
 * goes to vsr_io_store_base_record and the end to base_end/base_resume. */
int vsr_io_snapshots_load_base(struct vsr_io *io, uint32_t replica,
                               struct vsr_id id, uint64_t sequence);

void vsr_io_snapshots_poll(struct vsr_io *io, uint32_t replica, uint64_t now);
void vsr_io_snapshots_prepare(struct vsr_io *io, uint32_t replica,
                              struct vsr_io_sqe *sqes, uint32_t capacity,
                              uint32_t *count);
void vsr_io_snapshots_complete(struct vsr_io *io, uint32_t replica,
                               uint32_t slot, const struct vsr_io_cqe *cqe);
/* Detach: EBUSY while a file operation is in flight. */
int vsr_io_snapshots_close(struct vsr_io *io, uint32_t replica);

#endif /* VSR_IO_SNAPSHOT_H */
