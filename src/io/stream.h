#ifndef VSR_IO_STREAM_H
#define VSR_IO_STREAM_H

#include "io/cursor.h"
#include "vsr-io.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Bulk streams, both roles (docs/io-implementation.md, "Streams"). A stream
 * is a link of STREAM purpose plus this state machine. The requester dials,
 * sends the request frame and receives chunks; the source accepts, answers
 * STREAM_SERVE or serves the engine's own library request, and sends chunks
 * from caller buffers or from file ranges. There is no splice and no pipe
 * (docs/io-design.md decision 39): a FILE write is served by reading the
 * range into pool slabs, stream_chunk_bytes at a time, with fixed-buffer
 * reads, and sending each slab as a chunk frame exactly like a BUFFERS
 * write. Flow control is credit: at most stream_window DATA ops outstanding
 * at the requester, and at most stream_window chunk reads or chunk sends
 * in flight at the source, plus at most stream_window WRITE events queued.
 *
 * Units and writes: every chunk is a UNIT of the stream's window, a ring
 * in stream order. At the requester a unit is a DATA op at the caller (or
 * the library) holding one slab reference; at the source it is one chunk
 * frame from creation (a FILE read into a slab, or a slice of the caller's
 * buffers) until the link's notified_offset passes its frame. The source's
 * WRITE events queue in a second ring of stream_window entries, chunked in
 * order; a write's STREAM_WRITTEN follows the release of its last unit, in
 * write order, and END goes out after the last chunk of the last write.
 *
 * Handles and op ids: the source's stream handle (STREAM_SERVE.stream,
 * named by WRITE and CLOSE) is generation << 32 | index, nonzero and never
 * reused for another stream. SERVE and DATA op ids carry their kind in the
 * top two bits (VSR_IO_STREAM_OP_*), so the engine routes a rail COMPLETE
 * by id alone: HANDSHAKE ids have those bits clear.
 *
 * Caller leases: the engine emits no RELEASE op for its own event kinds.
 * The lease of a STREAM_OPEN ends with the stream's STREAM_END, the lease
 * of a STREAM_WRITE with its STREAM_WRITTEN (emitted for every queued
 * write, in order, before STREAM_END, also when the stream ends early).
 *
 * Library streams: a request beginning with VSR_IO_LIBRARY_MAGIC is served
 * by the engine itself (vsr_io_snapshots_serve opens the file and feeds a
 * FILE write and CLOSE through the write and close calls below), and a
 * stream the engine requests (the clients half of SNAPSHOT_FETCH) is
 * opened with owner LIBRARY by snapshot.h, which receives its chunks and
 * end through vsr_io_snapshots_stream_data / stream_end instead of ops and
 * completes each chunk with vsr_io_streams_data_done. A source-side library
 * stream hears its end through vsr_io_snapshots_stream_end too, so the
 * file slot can be closed; it gets no WRITTEN. Until snapshot.c exists,
 * stream.c carries weak stubs of those three functions.
 *
 * Timeouts: a stream that makes no progress (no frame received, no send
 * progress, no completion, no caller call) for handshake_timeout_ns ends
 * with RETRY on both sides (decision 56); the requester's clock also covers
 * the dial, the source's the SERVE at the caller and the drain of its
 * sends after CLOSE. Waiting for the caller's DATA completions after END
 * arrived is not timed.
 */

/* Spans of one BUFFERS chunk: bounded so a chunk always fits the link's
 * vector array once the build is empty. */
#define VSR_IO_STREAM_CHUNK_VECTORS 64u

/* Kind of a SERVE or DATA op id: the top two bits. */
#define VSR_IO_STREAM_OP_MASK (UINT64_C(3) << 62)
#define VSR_IO_STREAM_OP_SERVE (UINT64_C(1) << 62)
#define VSR_IO_STREAM_OP_DATA (UINT64_C(2) << 62)

static inline uint64_t vsr_io_streams_op_kind(uint64_t op)
{
    return op & VSR_IO_STREAM_OP_MASK;
}

enum vsr_io_stream_state {
    VSR_IO_STREAM_FREE,
    VSR_IO_STREAM_DIALING,   /* Requester: link pending. */
    VSR_IO_STREAM_REQUESTED, /* Requester: request sent, chunks expected. */
    VSR_IO_STREAM_SERVING,   /* Source: STREAM_SERVE at the caller. */
    VSR_IO_STREAM_OPEN,      /* Source: accepted; writes flow. */
    VSR_IO_STREAM_ENDING     /* END sent, received or decided; draining
                                units, writes and the link's sends before
                                the END op. */
};

enum vsr_io_stream_owner {
    VSR_IO_STREAM_CALLER, /* Cookie and DATA/SERVE ops are the caller's. */
    VSR_IO_STREAM_LIBRARY /* snapshot.h owns it. */
};

enum vsr_io_stream_unit_state {
    VSR_IO_UNIT_FREE,
    VSR_IO_UNIT_READING, /* Source FILE chunk: read to issue (slot NONE) or
                            in flight; `filled` bytes are in the slab. */
    VSR_IO_UNIT_READY,   /* Source: chunk bytes ready to send, in order. */
    VSR_IO_UNIT_SENT,    /* Source: frame queued; freed once notified. */
    VSR_IO_UNIT_DATA     /* Requester: DATA op at the caller or library. */
};

/* One unit of the window: a chunk. */
struct vsr_io_stream_unit {
    uint32_t state;  /* enum vsr_io_stream_unit_state */
    uint32_t slab;   /* Pool slab holding the bytes, or NONE for caller
                        buffers. */
    uint32_t slot;   /* Read slot in flight, or NONE. */
    uint32_t length; /* Chunk bytes. */
    uint32_t filled; /* FILE: bytes read so far (a short read resumes). */
    uint32_t write;  /* Source: index of its write in the stream's ring. */
    uint32_t span;   /* BUFFERS: first span of the chunk... */
    /* Requester: the chunk's number in the stream; its low 16 bits are the
     * unit field of the DATA op id (decision B2). */
    uint32_t sequence;
    uint64_t span_offset; /* ...and the offset within it. */
    uint64_t offset;      /* Stream offset of the chunk. */
    uint64_t file_offset; /* FILE: byte offset of the chunk in the file. */
    uint64_t send_end;    /* Link stream offset after this chunk's frame. */
};

/* One queued STREAM_WRITE at the source. */
struct vsr_io_stream_queued_write {
    uint32_t kind;                /* enum vsr_io_write_kind */
    uint32_t slot;                /* FILE: the caller's registered slot. */
    uint64_t write;               /* The caller's write id. */
    uint64_t lease;               /* The caller's lease, 0 for a file range. */
    uint64_t offset;              /* FILE: byte offset of the range. */
    uint64_t length;              /* Bytes of the write. */
    uint64_t chunked;             /* Bytes assigned to units so far. */
    const struct vsr_span *spans; /* BUFFERS: the caller's spans... */
    uint32_t count;               /* ...and their count. */
    uint32_t span;                /* Chunking position: span index... */
    uint64_t span_offset;         /* ...and offset within it. */
    uint32_t units;               /* Units of this write not yet freed. */
    uint32_t reserved;
};

struct vsr_io_stream {
    uint32_t state;      /* enum vsr_io_stream_state */
    uint32_t owner;      /* enum vsr_io_stream_owner */
    uint32_t direction;  /* INBOUND: source; OUTBOUND: requester. */
    uint32_t link;       /* Link index or INDEX_NONE; kept after the link
                            was lost, to watch its teardown. */
    uint32_t generation; /* In handles and op ids; nonzero once used. */
    uint32_t deadline;   /* Deadline handle (inactivity). */
    uint64_t cookie;     /* Requester: caller's cookie; source: handle. */
    uint64_t node;
    uint64_t offset;        /* Bytes delivered (requester) or assigned to chunks
                         (source) so far. */
    uint64_t replica;       /* Library streams: replica index (snapshot.h). */
    uint64_t request_lease; /* Requester: caller's lease on the request. */
    struct vsr_span request; /* Requester: the caller's bytes; source: the
                                slab bytes, pinned by the SERVE op until
                                completed. */
    uint32_t request_slab;
    uint32_t request_pending; /* Requester: request frame still to queue. */
    uint64_t serve_op;        /* Op id of the outstanding SERVE, or 0. */
    uint32_t accepted;   /* Source: SERVE completed OK: an END op is owed. */
    uint32_t link_gone;  /* The link was lost or closed by the stream. */
    uint32_t end_due;    /* Source: CLOSE (or refusal, failure) decided; the
                           END frame goes after the last chunk. */
    uint32_t end_sent;   /* Source: END frame queued; requester: received. */
    uint32_t ended;      /* END op emitted (or the library told); a source
                           then lingers on its link until the requester's
                           close or the inactivity timer. */
    uint32_t closed;     /* Source: the caller's CLOSE was taken; a second
                            one is EINVAL (decision B4). */
    uint64_t end_offset; /* Link stream offset after the END frame. */
    uint64_t send_end;   /* Link stream offset after the last queued frame:
                            the kernel reads nothing of the stream's once
                            notified_offset reaches it. */
    int32_t status;      /* Status to report at END. */
    uint32_t units_used; /* Ring span from units_head, holes included. */
    uint32_t units_head;
    uint32_t writes_head;
    uint32_t writes_count;
    uint32_t aborted; /* Source: chunking stopped (loss, failure, shutdown);
                         a read in flight is dropped at its completion. */
    uint32_t chunks;  /* Requester: chunks taken so far (the next unit's
                         sequence); the head unit's is chunks - units_used. */
    struct vsr_io_stream_unit *units;          /* [stream_window] */
    struct vsr_io_stream_queued_write *writes; /* [stream_window] */
};

struct vsr_io_streams {
    struct vsr_io_stream *streams;             /* [limits.streams] */
    struct vsr_io_stream_unit *units;          /* [streams * stream_window] */
    struct vsr_io_stream_queued_write *writes; /* [streams * stream_window] */
    uint32_t count;
    uint32_t window;
    uint32_t chunk_bytes; /* options.stream_chunk_bytes */
    uint32_t active;
    uint32_t closing; /* 1 after vsr_io_streams_shutdown. */
    uint32_t reserved;
};

/* ELIMIT beyond 65535 streams or a window of 65535 (the op id fields) or
 * on overflow. */
int vsr_io_streams_size(const struct vsr_io_limits *limits, size_t *bytes,
                        size_t *alignment);
void vsr_io_streams_init(struct vsr_io_streams *streams, void *memory,
                         size_t size, const struct vsr_io_limits *limits,
                         uint32_t chunk_bytes);

/* The source's handle of stream `index` (its current generation). */
uint64_t vsr_io_streams_handle(const struct vsr_io_streams *streams,
                               uint32_t index);

/* Caller events. open returns ELIMIT when no stream or link is free, EINVAL
 * for a malformed request, a caller request with the library prefix, an
 * unknown, caller-dialed or own node, a cookie already in use by a caller
 * stream, or a closing engine; *index is the stream's index (the library
 * sets streams[index].replica). write is EINVAL for a handle that is not
 * an OPEN source stream or malformed buffers, AGAIN while stream_window
 * writes are queued (resubmit after a WRITTEN). close is OK for an OPEN
 * stream, and for one lost or cancelled under the caller whose END op is
 * still to come; EINVAL once the END op went out, after a refusal, a
 * read failure or an earlier close (those decided the END already). */
int vsr_io_streams_open(struct vsr_io *io, uint64_t cookie,
                        const struct vsr_io_stream_open *open, uint64_t lease,
                        uint32_t owner, uint32_t *index);
int vsr_io_streams_write(struct vsr_io *io,
                         const struct vsr_io_stream_write *write,
                         uint64_t lease);
int vsr_io_streams_close(struct vsr_io *io, uint64_t stream, int32_t status);
/* Completions of SERVE and DATA ops (caller or library): EINVAL for an id
 * that is not outstanding. A SERVE completed with any status but OK
 * refuses the stream (END with RETRY, then the link closes). */
int vsr_io_streams_served(struct vsr_io *io, uint64_t op, int32_t status);
int vsr_io_streams_data_done(struct vsr_io *io, uint64_t op);

/* Inbound frames on a stream link, from the link module: request at the
 * source; chunk and end at the requester. The body's bytes live in pool
 * slab `slab`, which the module retains (vsr_io_pool_retain) if it keeps
 * them past the call. Returns false to leave the frame where it is, the
 * link then retries it at every poll (a window with no free unit, no free
 * stream, no room in the forwarded ring) and pauses its receive until
 * every byte it holds is carved (decision 99: the TCP window throttles
 * the source meanwhile); an inbound link is bound to its
 * stream by setting links.links[link].stream at the request frame, which
 * also exempts it from the idle close. A frame the stream's state refuses
 * (a chunk at the source, a second request, a chunk at the wrong offset, a
 * malformed body) counts in frames_rejected and closes the link with
 * -EPROTO; the requester reports FAILED. */
bool vsr_io_streams_frame(struct vsr_io *io, uint32_t link, uint16_t kind,
                          const struct vsr_io_cursor *body, uint32_t slab);
/* Link lifecycle from the link module. A late event for a stream that no
 * longer expects it is ignored. */
void vsr_io_streams_link_up(struct vsr_io *io, uint32_t stream);
void vsr_io_streams_link_lost(struct vsr_io *io, uint32_t stream,
                              int32_t error);
/* Send progress: called at every send completion and NOTIF of the
 * stream's link with its notified offset (unchanged at a zero-copy
 * result); releases the units up to it, retries a frame that got EBUSY
 * and emits the WRITTEN ops that became due. Never closes the link. */
void vsr_io_streams_sent(struct vsr_io *io, uint32_t stream,
                         uint64_t notified_offset);

/* A popped STREAM deadline (inactivity of stream `index`), dispatched by
 * the engine's poll before vsr_io_streams_poll: the stream ends with RETRY
 * and its link closes with -ETIMEDOUT. */
void vsr_io_streams_deadline(struct vsr_io *io, uint32_t index, uint64_t now);
/* Poll: chunking and sends, WRITTEN and END ops into the forwarded queue,
 * link closes and stream release. Prepare: file chunk reads. Complete:
 * read completions (slot kind STREAM). Shutdown: every stream ends with
 * CANCELLED; the engine is closed once `active` is zero. */
void vsr_io_streams_poll(struct vsr_io *io, uint64_t now);
void vsr_io_streams_prepare(struct vsr_io *io, struct vsr_io_sqe *sqes,
                            uint32_t capacity, uint32_t *count);
void vsr_io_streams_complete(struct vsr_io *io, uint32_t slot,
                             const struct vsr_io_cqe *cqe);
void vsr_io_streams_shutdown(struct vsr_io *io);

#endif /* VSR_IO_STREAM_H */
