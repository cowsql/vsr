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
 * (docs/io-design.md decision 38): a FILE write is served by reading the
 * range into pool slabs, stream_chunk_bytes at a time, with fixed-buffer
 * reads, and sending each slab as a chunk frame exactly like a BUFFERS
 * write. Flow control is credit: at most stream_window DATA ops outstanding
 * at the requester, and at most stream_window chunk reads or buffer sends
 * in flight at the source.
 *
 * Library streams: a request beginning with VSR_IO_LIBRARY_MAGIC is served
 * by the engine itself (snapshot.h opens the file and feeds FILE writes),
 * and a stream the engine requests (the clients half of SNAPSHOT_FETCH) is
 * owned by snapshot.h, which receives its chunks instead of the caller.
 */

enum vsr_io_stream_state {
    VSR_IO_STREAM_FREE,
    VSR_IO_STREAM_DIALING,   /* Requester: link pending. */
    VSR_IO_STREAM_REQUESTED, /* Requester: request sent, chunks expected. */
    VSR_IO_STREAM_SERVING,   /* Source: STREAM_SERVE at the caller, or the
                                library request being resolved. */
    VSR_IO_STREAM_OPEN,      /* Source: accepted; writes flow. */
    VSR_IO_STREAM_ENDING,    /* END sent or received; draining. */
    VSR_IO_STREAM_CLOSED
};

enum vsr_io_stream_owner {
    VSR_IO_STREAM_CALLER, /* Cookie and DATA/SERVE ops are the caller's. */
    VSR_IO_STREAM_LIBRARY /* snapshot.h owns it. */
};

/* One outstanding unit of the window: a DATA op at the requester, a chunk
 * read or a buffer write at the source. */
struct vsr_io_stream_unit {
    uint64_t offset;
    uint32_t slab; /* Pool slab, or INDEX_NONE for caller buffers. */
    uint32_t slot; /* Read slot at the source, or NONE. */
    uint32_t length;
    uint32_t state;    /* 0 free, 1 reading, 2 sending, 3 at the caller. */
    uint64_t write;    /* Source: caller's write id; requester: op id. */
    uint64_t send_end; /* Link stream offset after this chunk's frame. */
};

struct vsr_io_stream {
    uint32_t state;     /* enum vsr_io_stream_state */
    uint32_t owner;     /* enum vsr_io_stream_owner */
    uint32_t direction; /* INBOUND: source; OUTBOUND: requester. */
    uint32_t link;      /* Link index or INDEX_NONE. */
    uint64_t cookie;    /* Requester: caller's cookie; source: handle. */
    uint64_t node;
    uint64_t offset;         /* Bytes delivered or sent so far. */
    uint64_t replica;        /* Library streams: replica index. */
    uint64_t request_lease;  /* Requester: caller's lease on the request. */
    struct vsr_span request; /* Source: request bytes (slab), pinned by the
                                SERVE op until completed. */
    uint32_t request_slab;
    uint32_t serve_op; /* Op id of the outstanding SERVE, or 0. */
    uint32_t units_used;
    uint32_t units_head; /* Ring of units, in stream order. */
    uint32_t deadline;   /* Deadline handle (inactivity). */
    int32_t status;      /* Status to report at END. */
    /* Source FILE write in progress: chunks are read in order. */
    uint32_t file_slot;
    uint32_t file_pending; /* 1 while a FILE write is being chunked. */
    uint64_t file_offset;  /* Next byte to read. */
    uint64_t file_end;
    uint64_t file_write;              /* Its caller write id. */
    struct vsr_io_stream_unit *units; /* [stream_window] */
};

struct vsr_io_streams {
    struct vsr_io_stream *streams;    /* [limits.streams] */
    struct vsr_io_stream_unit *units; /* [streams * stream_window] */
    uint32_t count;
    uint32_t window;
    uint32_t chunk_bytes; /* options.stream_chunk_bytes */
    uint32_t active;
};

int vsr_io_streams_size(const struct vsr_io_limits *limits, size_t *bytes,
                        size_t *alignment);
void vsr_io_streams_init(struct vsr_io_streams *streams, void *memory,
                         size_t size, const struct vsr_io_limits *limits,
                         uint32_t chunk_bytes);

/* Caller events. open returns ELIMIT when no stream or link is free. */
int vsr_io_streams_open(struct vsr_io *io, uint64_t cookie,
                        const struct vsr_io_stream_open *open, uint64_t lease,
                        uint32_t owner, uint32_t *index);
int vsr_io_streams_write(struct vsr_io *io,
                         const struct vsr_io_stream_write *write,
                         uint64_t lease);
int vsr_io_streams_close(struct vsr_io *io, uint64_t stream, int32_t status);
/* Completions of SERVE and DATA ops (caller or library). */
int vsr_io_streams_served(struct vsr_io *io, uint64_t op, int32_t status);
int vsr_io_streams_data_done(struct vsr_io *io, uint64_t op);

/* Inbound frames on a stream link, from the link module: request at the
 * source; chunk and end at the requester. The body's bytes live in pool
 * slab `slab`, which the module retains (vsr_io_pool_retain) if it keeps
 * them past the call. Returns false to leave the frame where it is, the
 * link then retries it at every poll (a window with no free unit); an
 * inbound link is bound to its stream by setting links.links[link].stream
 * at the request frame, which also exempts it from the idle close. */
bool vsr_io_streams_frame(struct vsr_io *io, uint32_t link, uint16_t kind,
                          const struct vsr_io_cursor *body, uint32_t slab);
/* Link lifecycle from the link module. */
void vsr_io_streams_link_up(struct vsr_io *io, uint32_t stream);
void vsr_io_streams_link_lost(struct vsr_io *io, uint32_t stream,
                              int32_t error);
/* Send progress: called at every send completion and NOTIF of the
 * stream's link with its notified offset (unchanged at a zero-copy
 * result); releases the units up to it and retries a frame that got
 * EBUSY. */
void vsr_io_streams_sent(struct vsr_io *io, uint32_t stream,
                         uint64_t notified_offset);

/* Poll: expired streams, END emission, DATA/SERVE/WRITTEN ops into the
 * forwarded queue. Prepare: file chunk reads. Complete: read completions. */
void vsr_io_streams_poll(struct vsr_io *io, uint64_t now);
void vsr_io_streams_prepare(struct vsr_io *io, struct vsr_io_sqe *sqes,
                            uint32_t capacity, uint32_t *count);
void vsr_io_streams_complete(struct vsr_io *io, uint32_t slot,
                             const struct vsr_io_cqe *cqe);
void vsr_io_streams_shutdown(struct vsr_io *io);

#endif /* VSR_IO_STREAM_H */
