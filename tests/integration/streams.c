#include "config.h"

#include "lib/check.h"
#include "lib/io_world.h"
#include "vsr-io.h"
#include "vsr-sim.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * Bulk streams between engines over the simulation (docs/io-implementation
 * section 9, row `streams`): caller streams through vsr-io.h's rails, the
 * source writing a seeded pattern as BUFFERS or FILE writes and the
 * requester checking every byte against it (tests/lib/io_world), with the
 * purity guard armed around every primitive. Library streams (the snapshot
 * module's clients files) run beside caller streams in
 * tests/integration/snapshots.
 *
 * IOW_TEST=name runs one test; IOW_SEED=n shifts every test's seed;
 * IOW_TRACE=1 prints the simulation's trace.
 */

static uint64_t seed_base;
static uint64_t cookie_next = 1;

static uint64_t seed(uint64_t own)
{
    return seed_base != 0 ? seed_base * 1000u + own : own;
}

static uint64_t cookie(void)
{
    return (UINT64_C(1) << 62) | cookie_next++;
}

static void pair(uint32_t nodes, uint64_t own,
                 const struct vsr_sim_faults *faults)
{
    iow_open_sim(nodes, seed(own), faults);
    for (uint32_t i = 0; i < nodes; ++i) {
        iow_node_open(i);
    }
}

struct ended {
    struct iow_rstream *r[IOW_STREAMS];
    uint32_t count;
};

static bool all_ended(void *ctx)
{
    const struct ended *e = ctx;

    for (uint32_t i = 0; i < e->count; ++i) {
        if (!e->r[i]->ended) {
            return false;
        }
    }
    return true;
}

static bool one_ended(void *ctx)
{
    const struct iow_rstream *r = ctx;

    return r->ended;
}

/* The source's stream record of the (only) stream the node serves with
 * `kind`, or NULL. */
static struct iow_sstream *source_of(struct iow_node *n, uint64_t seed_value)
{
    for (uint32_t i = 0; i < IOW_STREAMS; ++i) {
        if (n->sstreams[i].used && n->sstreams[i].seed == seed_value) {
            return &n->sstreams[i];
        }
    }
    return NULL;
}

static bool sources_ended(void *ctx)
{
    struct iow_node *n = ctx;

    for (uint32_t i = 0; i < IOW_STREAMS; ++i) {
        if (n->sstreams[i].used && n->sstreams[i].accepted &&
            !n->sstreams[i].ended) {
            return false;
        }
    }
    return true;
}

static void expect_ok(const struct iow_rstream *r, uint64_t length)
{
    if (r->status != VSR_IO_OK || r->end_bytes != length ||
        r->received != length || r->mismatch) {
        fprintf(stderr,
                "stream %" PRIx64 ": status %d bytes %" PRIu64
                " received %" PRIu64 " (want %" PRIu64 ") mismatch %d\n",
                r->cookie, r->status, r->end_bytes, r->received, length,
                r->mismatch);
        CHECK(false);
    }
}

/* -------------------------------------------------------------------------
 * Transfers
 * ---------------------------------------------------------------------- */

/* Streams in both directions at once, each engine requesting two and
 * serving two (its whole stream table): BUFFERS writes of pieces smaller
 * and larger than a chunk, FILE writes from the caller's registered file,
 * and the edge lengths (empty, one byte, exactly one chunk). Every byte
 * arrives once and in order, every write gets its WRITTEN, both ends see
 * OK, and the same cookies serve a second round. */
static void test_transfers(void)
{
    static const struct {
        uint64_t length;
        uint32_t piece;
        uint32_t kind;
    } cases[] = {{0, 100, VSR_IO_WRITE_BUFFERS},
                 {1, 100, VSR_IO_WRITE_BUFFERS},
                 {1024, 1024, VSR_IO_WRITE_BUFFERS},
                 {100000, 3000, VSR_IO_WRITE_BUFFERS},
                 {70000, 700, VSR_IO_WRITE_BUFFERS},
                 {IOW_FILE_BYTES, 5000, VSR_IO_WRITE_FILE},
                 {4096, 4096, VSR_IO_WRITE_FILE},
                 {33000, 1024, VSR_IO_WRITE_FILE}};
    struct iow_node *a;
    struct iow_node *b;
    struct ended e;
    uint32_t count = (uint32_t)(sizeof(cases) / sizeof(cases[0]));

    pair(2, 1, NULL);
    a = &iow.node[0];
    b = &iow.node[1];
    iow_caller_file(a);
    iow_caller_file(b);
    for (uint32_t base = 0; base < count; base += 4) {
        memset(&e, 0, sizeof(e));
        for (uint32_t k = 0; k < 4 && base + k < count; ++k) {
            struct iow_stream_request r = iow_pattern(
                cases[base + k].kind == VSR_IO_WRITE_FILE ? IOW_FILE_SEED
                                                          : 1000u + base + k,
                cases[base + k].length, cases[base + k].piece);
            struct iow_node *from = k % 2 == 0 ? a : b;
            struct iow_node *to = k % 2 == 0 ? b : a;

            r.write_kind = cases[base + k].kind;
            e.r[e.count++] = iow_stream_open(from, to->index + 1, cookie(), &r);
        }
        CHECK(iow_run_until(all_ended, &e, 10000 * IOW_MS));
        for (uint32_t k = 0; k < e.count; ++k) {
            expect_ok(e.r[k], cases[base + k].length);
        }
        CHECK(iow_run_until(sources_ended, a, 1000 * IOW_MS));
        CHECK(iow_run_until(sources_ended, b, 1000 * IOW_MS));
        for (uint32_t i = 0; i < IOW_STREAMS; ++i) {
            struct iow_sstream *s = &a->sstreams[i];

            for (uint32_t side = 0; side < 2; ++side) {
                s = side == 0 ? &a->sstreams[i] : &b->sstreams[i];
                if (s->used) {
                    CHECK(s->status == VSR_IO_OK);
                    CHECK(s->written == s->writes);
                    CHECK(s->end_bytes == s->length);
                    s->used = false;
                }
            }
        }
    }
    iow_run_for(50 * IOW_MS);
    {
        struct vsr_io_stats stats;

        vsr_io_get_stats(a->io, &stats);
        CHECK(stats.streams == 0 && stats.frames_rejected == 0);
    }
    iow_close_node(a);
    iow_close_node(b);
    iow_close();
}

/* -------------------------------------------------------------------------
 * Backpressure
 * ---------------------------------------------------------------------- */

/* A requester whose caller holds its DATA ops: at most stream_window are
 * outstanding, its link pauses its receive (decision 99) and the source,
 * whose writes are refused AGAIN while stream_window are queued, waits.
 * Released one at a time with pauses longer than a heartbeat, the
 * transfer completes byte for byte, OK on both sides. */
static void test_backpressure(void)
{
    struct iow_node *a;
    struct iow_node *b;
    struct iow_rstream *r;
    struct iow_sstream *s;
    struct iow_stream_request request = iow_pattern(55, 60000, 4096);
    uint32_t released = 0;

    pair(2, 2, NULL);
    a = &iow.node[0];
    b = &iow.node[1];
    a->hold_data = true;
    r = iow_stream_open(a, 2, cookie(), &request);
    while (!r->ended) {
        CHECK(released < 10000);
        iow_run_for(20 * IOW_MS);
        CHECK(a->held_data_count <= iow.io_limits.stream_window);
        s = source_of(b, 55);
        if (s != NULL && !r->ended) {
            /* The source never has more than a window queued ahead. */
            CHECK(s->writes - s->written <= iow.io_limits.stream_window);
        }
        if (a->held_data_count > 0) {
            /* One at a time, oldest first. */
            uint64_t id = a->held_data[0];

            memmove(a->held_data, a->held_data + 1,
                    (size_t)(a->held_data_count - 1) * sizeof(a->held_data[0]));
            a->held_data_count--;
            a->hold_data = false;
            {
                struct vsr_io_event event;
                uint32_t consumed = 0;

                memset(&event, 0, sizeof(event));
                event.kind = VSR_IO_EVENT_COMPLETE;
                event.event.type = VSR_EVENT_COMPLETE;
                event.event.id = id;
                CHECK(iow_submit(a, &event, 1, &consumed) == VSR_OK);
                vsr_io_wake(a->io);
            }
            a->hold_data = true;
            released++;
        }
    }
    if (r->status != VSR_IO_OK) {
        struct vsr_io_node_status st;
        struct vsr_io_stats stats;

        CHECK(vsr_io_node_status(a->io, 2, &st) == VSR_OK);
        vsr_io_get_stats(a->io, &stats);
        fprintf(stderr,
                "requester: node 2 last error %d, rejected %" PRIu64
                ", released %u, t=%" PRIu64 "\n",
                st.last_error, stats.frames_rejected, released, iow_now());
        s = source_of(b, 55);
        if (s != NULL) {
            fprintf(stderr, "source: ended %d status %d writes %u written %u\n",
                    s->ended, s->status, s->writes, s->written);
        }
    }
    expect_ok(r, request.length);
    CHECK(r->data_ops == released);
    CHECK(iow_run_until(sources_ended, b, 1000 * IOW_MS));
    s = source_of(b, 55);
    CHECK(s != NULL && s->status == VSR_IO_OK && s->written == s->writes);
    iow_close_node(a);
    iow_close_node(b);
    iow_close();
}

/* -------------------------------------------------------------------------
 * Early ends
 * ---------------------------------------------------------------------- */

/* The ways a stream ends before its bytes: a refused SERVE (RETRY at the
 * requester, no END op at the source), a source CLOSE with FAILED after
 * half the bytes (FAILED with that half at the requester), a FILE range
 * running past the file's end (FAILED on both sides after the bytes the
 * file had), a SERVE nobody answers (RETRY at the requester once the
 * inactivity timer fires; the source's END follows its late refusal only
 * when accepted), and a source CLOSE with NOT_FOUND after nothing. */
static void test_early_ends(void)
{
    struct iow_node *a;
    struct iow_node *b;
    struct iow_rstream *r;
    struct iow_sstream *s;
    struct iow_stream_request request;
    uint64_t ends;
    uint64_t start;

    pair(2, 3, NULL);
    a = &iow.node[0];
    b = &iow.node[1];
    iow_caller_file(b);

    /* Refused. */
    request = iow_pattern(1, 10000, 1000);
    request.refuse = VSR_IO_NOT_FOUND;
    ends = b->stream_ends;
    r = iow_stream_open(a, 2, cookie(), &request);
    CHECK(iow_run_until(one_ended, r, 2000 * IOW_MS));
    CHECK(r->status == VSR_IO_RETRY && r->end_bytes == 0 && r->received == 0);
    iow_run_for(50 * IOW_MS);
    CHECK(b->stream_ends == ends);

    /* Closed FAILED halfway. */
    request = iow_pattern(2, 20000, 1000);
    request.stop = 10000;
    request.close_status = VSR_IO_FAILED;
    r = iow_stream_open(a, 2, cookie(), &request);
    CHECK(iow_run_until(one_ended, r, 2000 * IOW_MS));
    CHECK(r->status == VSR_IO_FAILED && r->end_bytes == 10000);
    CHECK(r->received == 10000 && !r->mismatch);
    CHECK(iow_run_until(sources_ended, b, 1000 * IOW_MS));
    s = source_of(b, 2);
    CHECK(s != NULL && s->status == VSR_IO_FAILED && s->written == s->writes);
    s->used = false;

    /* A FILE range past the file's end. */
    request = iow_pattern(IOW_FILE_SEED, IOW_FILE_BYTES + 5000, 7000);
    request.write_kind = VSR_IO_WRITE_FILE;
    r = iow_stream_open(a, 2, cookie(), &request);
    CHECK(iow_run_until(one_ended, r, 2000 * IOW_MS));
    CHECK(r->status == VSR_IO_FAILED && !r->mismatch);
    CHECK(r->received <= IOW_FILE_BYTES && r->end_bytes == r->received);
    CHECK(iow_run_until(sources_ended, b, 1000 * IOW_MS));
    s = source_of(b, IOW_FILE_SEED);
    CHECK(s != NULL && s->status == VSR_IO_FAILED && s->written == s->writes);
    s->used = false;

    /* Nobody answers the SERVE: the requester's inactivity timer. */
    b->hold_serve = true;
    request = iow_pattern(3, 5000, 1000);
    start = iow_now();
    r = iow_stream_open(a, 2, cookie(), &request);
    CHECK(iow_run_until(one_ended, r, 2000 * IOW_MS));
    CHECK(r->status == VSR_IO_RETRY && r->received == 0);
    CHECK(iow_now() - start >= iow.handshake_timeout_ns);
    b->hold_serve = false;
    s = source_of(b, 3);
    CHECK(s != NULL && s->serve_op != 0);
    /* Its late refusal frees the source's stream without an END op. */
    {
        struct vsr_io_event event;
        uint32_t consumed = 0;

        memset(&event, 0, sizeof(event));
        event.kind = VSR_IO_EVENT_COMPLETE;
        event.event.type = VSR_EVENT_COMPLETE;
        event.event.id = s->serve_op;
        event.event.status = VSR_IO_RETRY;
        CHECK(iow_submit(b, &event, 1, &consumed) == VSR_OK);
        s->used = false;
    }
    iow_run_for(300 * IOW_MS);
    {
        struct vsr_io_stats stats;

        vsr_io_get_stats(b->io, &stats);
        CHECK(stats.streams == 0);
        vsr_io_get_stats(a->io, &stats);
        CHECK(stats.streams == 0);
    }

    /* Accepted, then closed NOT_FOUND before any byte. */
    request = iow_pattern(4, 5000, 1000);
    request.stop = 0;
    request.close_status = VSR_IO_NOT_FOUND;
    r = iow_stream_open(a, 2, cookie(), &request);
    CHECK(iow_run_until(one_ended, r, 2000 * IOW_MS));
    CHECK(r->status == VSR_IO_NOT_FOUND && r->end_bytes == 0);
    iow_close_node(a);
    iow_close_node(b);
    iow_close();
}

/* -------------------------------------------------------------------------
 * An idle source
 * ---------------------------------------------------------------------- */

/* An engine with nothing to time sleeps without a deadline; the stream
 * that wakes it arrives as an accept completion, processed before the
 * poll that brings the engine's clock forward. The accepted link's
 * handshake timer was armed from the previous poll's time, already past,
 * and the next poll closed the new link ETIMEDOUT before its request was
 * read: every stream to an engine idle longer than handshake_timeout_ns
 * failed (RETRY at the requester, no SERVE at the source). Timers armed
 * while completions are processed now count from the poll that follows
 * (decision G2). */
static void test_idle_source(void)
{
    struct iow_node *a;
    struct iow_node *b;
    struct iow_rstream *r;
    struct iow_stream_request request = iow_pattern(41, 10000, 1000);

    pair(2, 9, NULL);
    a = &iow.node[0];
    b = &iow.node[1];
    r = iow_stream_open(a, 2, cookie(), &request);
    CHECK(iow_run_until(one_ended, r, 2000 * IOW_MS));
    expect_ok(r, request.length);
    for (uint32_t round = 0; round < 3; ++round) {
        /* Both engines idle for twice the handshake timeout. */
        iow_run_for(2 * iow.handshake_timeout_ns);
        for (uint32_t i = 0; i < IOW_STREAMS; ++i) {
            if (b->sstreams[i].ended) {
                b->sstreams[i].used = false;
            }
        }
        request = iow_pattern(42 + round, 10000, 1000);
        r = iow_stream_open(a, 2, cookie(), &request);
        CHECK(iow_run_until(one_ended, r, 2000 * IOW_MS));
        expect_ok(r, request.length);
    }
    iow_close_node(a);
    iow_close_node(b);
    iow_close();
}

/* -------------------------------------------------------------------------
 * Engines closing and connections lost
 * ---------------------------------------------------------------------- */

/* A requester engine closes mid-transfer: its stream ends CANCELLED
 * (decision 101), the source's RETRY (OK if its END frame was on the wire
 * already, decision 97); a source engine closes: CANCELLED
 * there, RETRY at the requester; the connection is reset: RETRY on both
 * sides; the source's node crashes: RETRY at the requester. Every engine
 * still closes. */
static void test_loss(void)
{
    struct iow_node *a;
    struct iow_node *b;
    struct iow_rstream *r;
    struct iow_sstream *s;
    struct iow_stream_request request = iow_pattern(9, 200000, 2048);

    /* The requester's engine closes. */
    pair(2, 4, NULL);
    a = &iow.node[0];
    b = &iow.node[1];
    a->hold_data = true;
    r = iow_stream_open(a, 2, cookie(), &request);
    for (uint32_t i = 0; i < 100000 && a->held_data_count == 0; ++i) {
        CHECK(iow_round());
    }
    CHECK(a->held_data_count > 0);
    iow_close_io(a);
    iow_release_data(a);
    CHECK(iow_run_until(one_ended, r, 2000 * IOW_MS));
    CHECK(r->status == VSR_IO_CANCELLED && !r->mismatch);
    CHECK(iow_run_until(iow_node_closed, a, 2000 * IOW_MS));
    CHECK(iow_run_until(sources_ended, b, 2000 * IOW_MS));
    /* RETRY, or OK when its END frame had reached the kernel before the
     * loss (decision 97): the simulation's socket buffer takes the whole
     * transfer at once. */
    s = source_of(b, 9);
    CHECK(s != NULL &&
          (s->status == VSR_IO_RETRY ||
           (s->status == VSR_IO_OK && s->end_bytes == request.length)));
    CHECK(vsr_io_deinit(a->io) == VSR_OK);
    a->open = false;
    iow_close_node(b);
    iow_close();

    /* The source's engine closes while its stream is open. */
    pair(2, 5, NULL);
    a = &iow.node[0];
    b = &iow.node[1];
    a->hold_data = true;
    request.hold = 1;
    r = iow_stream_open(a, 2, cookie(), &request);
    request.hold = 0;
    for (uint32_t i = 0; i < 100000 && a->held_data_count == 0; ++i) {
        CHECK(iow_round());
    }
    iow_close_io(b);
    CHECK(iow_run_until(sources_ended, b, 2000 * IOW_MS));
    s = source_of(b, 9);
    CHECK(s != NULL && s->status == VSR_IO_CANCELLED);
    iow_release_data(a);
    CHECK(iow_run_until(one_ended, r, 2000 * IOW_MS));
    CHECK(r->status == VSR_IO_RETRY && !r->mismatch);
    CHECK(iow_run_until(iow_node_closed, b, 2000 * IOW_MS));
    CHECK(vsr_io_deinit(b->io) == VSR_OK);
    b->open = false;
    iow_close_node(a);
    iow_close();

    /* A reset, then a crash. */
    pair(2, 6, NULL);
    a = &iow.node[0];
    b = &iow.node[1];
    request.hold = 1; /* The source is mid-stream at the reset. */
    r = iow_stream_open(a, 2, cookie(), &request);
    request.hold = 0;
    for (uint32_t i = 0; i < 100000 && r->received < 20000; ++i) {
        CHECK(iow_round());
    }
    vsr_sim_reset(iow.sim, 0, 1);
    CHECK(iow_run_until(one_ended, r, 2000 * IOW_MS));
    CHECK(r->status == VSR_IO_RETRY && !r->mismatch);
    CHECK(r->received < request.length);
    CHECK(iow_run_until(sources_ended, b, 2000 * IOW_MS));
    s = source_of(b, 9);
    CHECK(s != NULL && s->status == VSR_IO_RETRY);
    s->used = false;
    r = iow_stream_open(a, 2, cookie(), &request);
    for (uint32_t i = 0; i < 100000 && r->received < 20000; ++i) {
        CHECK(iow_round());
    }
    iow_crash(b);
    CHECK(iow_run_until(one_ended, r, 5000 * IOW_MS));
    CHECK(r->status == VSR_IO_RETRY && !r->mismatch);
    iow_close_node(a);
    iow_close();
}

/* -------------------------------------------------------------------------
 * Network faults
 * ---------------------------------------------------------------------- */

/* Segments split at random boundaries, delayed, some corrupted: a
 * corrupted frame fails its CRC and closes its link (frames_rejected),
 * which ends the transfer RETRY; the requester never sees a wrong byte,
 * and transfers that end OK are complete. Split alone never fails one. */
static void test_faults(void)
{
    struct vsr_sim_faults faults;
    struct iow_node *a;
    struct iow_node *b;
    uint32_t ok = 0;
    uint32_t failed = 0;
    struct vsr_io_stats stats;

    memset(&faults, 0, sizeof(faults));
    faults.disk.latency_min_ns = 1000;
    faults.disk.latency_max_ns = 20000;
    faults.disk.fsync_min_ns = 1000;
    faults.disk.fsync_max_ns = 50000;
    faults.disk.unsynced_keep_ppm = 1000000;
    faults.network.delay_min_ns = 1000;
    faults.network.delay_max_ns = 200000;
    faults.network.stall_reset_ns = 100 * IOW_MS;
    faults.network.connect_timeout_ns = 100 * IOW_MS;
    faults.network.split_ppm = 300000;
    pair(2, 7, &faults);
    a = &iow.node[0];
    b = &iow.node[1];
    /* Split only: every transfer completes. */
    for (uint32_t round = 0; round < 4; ++round) {
        struct iow_stream_request request =
            iow_pattern(100 + round, 30000 + 7777 * round, 1500);
        struct iow_rstream *r = iow_stream_open(a, 2, cookie(), &request);

        CHECK(iow_run_until(one_ended, r, 5000 * IOW_MS));
        expect_ok(r, request.length);
        CHECK(iow_run_until(sources_ended, b, 1000 * IOW_MS));
        for (uint32_t i = 0; i < IOW_STREAMS; ++i) {
            if (b->sstreams[i].ended) {
                b->sstreams[i].used = false;
            }
        }
    }
    /* Corruption as well. */
    faults.network.corrupt_ppm = 20000;
    vsr_sim_set_faults(iow.sim, &faults);
    for (uint32_t round = 0; round < 12; ++round) {
        struct iow_stream_request request =
            iow_pattern(200 + round, 40000, 2000);
        struct iow_rstream *r = iow_stream_open(a, 2, cookie(), &request);

        CHECK(iow_run_until(one_ended, r, 20000 * IOW_MS));
        CHECK(!r->mismatch);
        if (r->status == VSR_IO_OK) {
            expect_ok(r, request.length);
            ok++;
        } else {
            CHECK(r->status == VSR_IO_RETRY || r->status == VSR_IO_FAILED);
            CHECK(r->received < request.length || r->status != VSR_IO_OK);
            failed++;
        }
        CHECK(iow_run_until(sources_ended, b, 2000 * IOW_MS));
        for (uint32_t i = 0; i < IOW_STREAMS; ++i) {
            if (b->sstreams[i].ended) {
                b->sstreams[i].used = false;
            }
        }
    }
    vsr_io_get_stats(a->io, &stats);
    fprintf(stderr, "faults: %u ok, %u failed, %" PRIu64 " frames rejected\n",
            ok, failed, stats.frames_rejected);
    CHECK(ok + failed == 12);
    faults.network.corrupt_ppm = 0;
    vsr_sim_set_faults(iow.sim, &faults);
    iow_close_node(a);
    iow_close_node(b);
    iow_close();
}

/* -------------------------------------------------------------------------
 * Streams beside a replicated group
 * ---------------------------------------------------------------------- */

struct view_watch {
    struct iow_group *g;
    uint64_t view;
    bool changed;
};

/* A stream never delays the protocol: while the primary's node sends a
 * backup a large stream whose requester drips its DATA completions (the
 * stream link paused most of the time), and another streams the other
 * way, the group commits every request in its first view, with no view
 * change. Streams ride their own links; the peer links carry the
 * heartbeats. */
static void test_beside_group(void)
{
    struct iow_group g;
    struct iow_app *primary;
    struct iow_node *p;
    struct iow_node *q;
    struct iow_rstream *slow;
    struct iow_rstream *fast;
    /* The slow stream stays within what a paused link can hold of a burst
     * (VSR_IO_LINK_HELD runs, decision 99): the simulation's socket takes
     * a whole transfer at once and delivers it before the pause's CANCEL
     * lands, and a larger one closes the link -ENOBUFS (section 11). */
    struct iow_stream_request big = iow_pattern(31, 60000, 4096);
    struct iow_stream_request other = iow_pattern(32, 150000, 3000);
    uint64_t view;

    iow_open_sim(3, seed(8), NULL);
    /* The source ends once the socket buffer (256 KiB in the simulation)
     * took its bytes, then lingers handshake_timeout_ns for the
     * requester's close (decision 96): the drip below must fit in it
     * (docs/io-implementation.md section 11). */
    iow.handshake_timeout_ns = 2000 * IOW_MS;
    g = iow_group_open(3, iow_cluster(20));
    primary = iow_group_primary(&g);
    CHECK(primary != NULL);
    view = primary->status.view;
    p = primary->node;
    q = &iow.node[(p->index + 1) % 3];
    q->hold_data = true;
    slow = iow_stream_open(q, p->index + 1, cookie(), &big);
    fast = iow_stream_open(p, q->index + 1, cookie(), &other);
    for (uint32_t round = 0; round < 12; ++round) {
        iow_group_commit(&g, 1);
        iow_run_for(15 * IOW_MS);
        /* A trickle of the slow stream's DATA completions. */
        if (q->held_data_count > 0) {
            struct vsr_io_event event;
            uint32_t consumed = 0;
            uint64_t id = q->held_data[0];

            memmove(q->held_data, q->held_data + 1,
                    (size_t)(q->held_data_count - 1) * sizeof(q->held_data[0]));
            q->held_data_count--;
            memset(&event, 0, sizeof(event));
            event.kind = VSR_IO_EVENT_COMPLETE;
            event.event.type = VSR_EVENT_COMPLETE;
            event.event.id = id;
            CHECK(iow_submit(q, &event, 1, &consumed) == VSR_OK);
            vsr_io_wake(q->io);
        }
        for (uint32_t i = 0; i < 3; ++i) {
            CHECK(g.apps[i]->status.view == view);
            CHECK(g.apps[i]->status.state == VSR_STATE_NORMAL);
        }
    }
    CHECK(iow_run_until(one_ended, fast, 5000 * IOW_MS));
    expect_ok(fast, other.length);
    CHECK(!slow->ended && !slow->mismatch);
    iow_release_data(q);
    CHECK(iow_run_until(one_ended, slow, 20000 * IOW_MS));
    expect_ok(slow, big.length);
    for (uint32_t i = 0; i < 3; ++i) {
        CHECK(g.apps[i]->status.view == view);
    }
    iow_group_close(&g);
    iow_close();
}

int main(int argc, char **argv)
{
    const char *only = getenv("IOW_TEST");
    const char *seed_env = getenv("IOW_SEED");

    (void)argc;
    (void)argv;
    seed_base = seed_env != NULL ? strtoull(seed_env, NULL, 10) : 0;
#define RUN(test)                                                              \
    do {                                                                       \
        if (only == NULL || strcmp(only, #test) == 0) {                        \
            test();                                                            \
            printf("%s: ok\n", #test);                                         \
        }                                                                      \
    } while (0)
    setvbuf(stdout, NULL, _IONBF, 0);
    RUN(test_transfers);
    RUN(test_backpressure);
    RUN(test_early_ends);
    RUN(test_idle_source);
    RUN(test_loss);
    RUN(test_faults);
    RUN(test_beside_group);
#undef RUN
    return 0;
}
