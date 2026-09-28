#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

/* Contract tests for "Log and checkpoint transfer" in docs/vsr-api.md: the
 * lifecycle of immutable log offers served to fetch requests. A single NORMAL
 * member serves discovery and range requests injected from an authenticated
 * learner (replica PEER, not a member); its responses are inspected in the
 * cluster queue before being dropped. */

enum { PEER = 2 };
#define NONE UINT32_MAX

struct fixture {
    struct mem_cluster *cluster;
    struct mem_node *node;
    uint64_t nonce;
};

struct response {
    uint32_t type;
    uint64_t nonce;
    uint64_t first;
    uint64_t next;
    uint64_t sequence;
    uint64_t log_begin;
    uint64_t log_end;
    uint64_t committed;
    uint32_t entries;
    uint64_t first_op;
};

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    return result;
}

static size_t find(struct mem_node *node, uint32_t type)
{
    for (size_t i = 0; i < mem_node_effects(node); i++) {
        if (mem_node_effect(node, i)->type == type)
            return i;
    }
    return SIZE_MAX;
}

/* Complete every effect except the held type until the node idles. */
static void drive(struct fixture *f, uint32_t hold)
{
    for (size_t turn = 0; turn < 20000; turn++) {
        bool progress = false;
        const struct mem_step step = mem_node_event(f->node, NULL);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        progress = step.emitted != 0 || (step.flags & VSR_UPDATE_MORE) != 0;
        for (size_t j = 0; j < mem_node_effects(f->node); j++) {
            if (mem_node_effect(f->node, j)->type == hold)
                continue;
            if (mem_node_complete(f->node, j, VSR_IO_OK)) {
                progress = true;
                break;
            }
        }
        mem_cluster_check(f->cluster);
        if (!progress)
            return;
    }
    CHECK(false);
}

static void tick(struct fixture *f, uint64_t now)
{
    CHECK(mem_node_time(f->node, now).consumed == 1);
    drive(f, NONE);
}

static struct fixture create(uint32_t transfers, bool timed)
{
    static const struct vsr_member member = {1, VSR_MEMBER_FULL, 0};
    const struct vsr_membership membership = {0, &member, 1, 0};
    struct fixture f = {.cluster = mem_cluster_create(), .nonce = 0};
    struct vsr_options options = mem_options(1, &membership);
    options.limits.transfers = transfers;
    options.limits.batch_entries = 1;
    options.limits.log_cache_entries = 2;
    f.node = mem_cluster_add(f.cluster, &options);
    if (timed)
        CHECK(mem_node_time(f.node, 0).consumed == 1);
    drive(&f, NONE);
    CHECK(status(f.node).state == VSR_STATE_NORMAL);
    return f;
}

static void command(struct fixture *f, uint64_t number)
{
    const struct vsr_span span = {"offer-command", 13};
    const struct vsr_blob body = {&span, 13, 1, 0};
    const struct vsr_request request = {
        {{700, 1}, number}, 0, VSR_REQUEST_COMMAND, 0, &body};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, number, &request, 1};
    CHECK(mem_node_event(f->node, &event).consumed == 1);
    drive(f, NONE);
    CHECK(status(f->node).committed == number);
}

/* Inject a fetch from PEER; returns the step so callers can observe
 * admission. Discovery uses revision zero and first = end = 0. */
static struct mem_step fetch(struct fixture *f, uint32_t type,
                             struct vsr_revision revision, uint64_t first,
                             uint64_t end)
{
    const struct vsr_fetch request = {
        {{PEER, 1}, ++f->nonce}, revision, first, end, 1024 + 64, 1, 0};
    const struct vsr_message message = {
        {1, 1},  status(f->node).epoch, status(f->node).view, PEER, type, 0, 0,
        &request};
    const struct vsr_event event = {VSR_EVENT_MESSAGE, 0, 0, &message, 1};
    return mem_node_event(f->node, &event);
}

static void discover(struct fixture *f)
{
    const struct vsr_revision zero = {{0, 0}, 0};
    CHECK(fetch(f, VSR_MSG_GET_STATE, zero, 0, 0).consumed == 1);
}

static void range(struct fixture *f, uint32_t type, uint64_t sequence,
                  uint64_t first, uint64_t end)
{
    const struct vsr_revision revision = {{1, 1}, sequence};
    CHECK(fetch(f, type, revision, first, end).consumed == 1);
}

/* Drain the node and take the single response queued for PEER. */
static struct response take(struct fixture *f)
{
    struct response out;
    memset(&out, 0, sizeof(out));
    drive(f, NONE);
    CHECK(mem_cluster_messages(f->cluster) == 1);
    uint64_t to;
    const struct vsr_message *message = mem_cluster_message(f->cluster, 0, &to);
    CHECK(to == PEER);
    CHECK(message->type == VSR_MSG_NEW_STATE || message->type == VSR_MSG_LOG ||
          message->type == VSR_MSG_STATE_UNAVAILABLE);
    const struct vsr_state_chunk *chunk = message->body;
    out.type = message->type;
    out.nonce = chunk->nonce.counter;
    out.first = chunk->first;
    out.next = chunk->next;
    out.sequence = chunk->state.revision.sequence;
    out.log_begin = chunk->state.log_begin;
    out.log_end = chunk->state.log_end;
    out.committed = chunk->state.committed;
    out.entries = chunk->state.entries.count;
    out.first_op = out.entries == 0 ? 0 : chunk->state.entries.entries[0].op;
    CHECK(message->number == out.log_end - 1);
    CHECK(chunk->nonce.incarnation.hi == PEER);
    mem_cluster_drop(f->cluster, 0);
    return out;
}

static void expect_unavailable(struct fixture *f, uint64_t first)
{
    const struct response r = take(f);
    CHECK(r.type == VSR_MSG_STATE_UNAVAILABLE);
    CHECK(r.nonce == f->nonce && r.first == first && r.next == first);
    CHECK(r.entries == 0);
    CHECK(r.log_end >= 1 && r.committed < r.log_end);
}

static void finish(struct fixture *f)
{
    mem_cluster_destroy(f->cluster);
}

/* "GET_STATE with zero revision and first=end=0 discovers a current offer.
 * Its response has first=next=0." "A successful chunk echoes the nonce and
 * first, contains exactly [first,next), and advances toward the requested
 * end." "an empty range returns next=first=end." GET_LOG answers with LOG. */
static void discovery_and_ranges(void)
{
    struct fixture f = create(8, true);
    command(&f, 1);
    command(&f, 2);
    discover(&f);
    const struct response offer = take(&f);
    CHECK(offer.type == VSR_MSG_NEW_STATE && offer.nonce == f.nonce);
    CHECK(offer.first == 0 && offer.next == 0 && offer.entries == 0);
    CHECK(offer.log_begin == 1 && offer.log_end == 3 && offer.committed == 2);
    CHECK(offer.sequence == status(f.node).stored_sequence);
    for (uint64_t first = 1; first < 3; first++) {
        range(&f, VSR_MSG_GET_STATE, offer.sequence, first, 3);
        const struct response chunk = take(&f);
        CHECK(chunk.type == VSR_MSG_NEW_STATE && chunk.nonce == f.nonce);
        CHECK(chunk.sequence == offer.sequence);
        CHECK(chunk.first == first && chunk.next == first + 1);
        CHECK(chunk.entries == 1 && chunk.first_op == first);
        CHECK(chunk.log_begin == 1 && chunk.log_end == 3);
    }
    range(&f, VSR_MSG_GET_STATE, offer.sequence, 3, 3);
    const struct response empty = take(&f);
    CHECK(empty.type == VSR_MSG_NEW_STATE && empty.entries == 0);
    CHECK(empty.first == 3 && empty.next == 3);
    range(&f, VSR_MSG_GET_LOG, offer.sequence, 2, 3);
    const struct response log = take(&f);
    CHECK(log.type == VSR_MSG_LOG && log.first == 2 && log.next == 3);
    CHECK(log.entries == 1 && log.first_op == 2);
    /* "Unknown/expired revisions or unavailable ranges produce
     * STATE_UNAVAILABLE with the echoed nonce and first, next=first". */
    range(&f, VSR_MSG_GET_STATE, offer.sequence + 1000, 1, 3);
    expect_unavailable(&f, 1);
    range(&f, VSR_MSG_GET_STATE, offer.sequence, 1, 4);
    expect_unavailable(&f, 1);
    {
        const struct vsr_revision foreign = {{7, 7}, offer.sequence};
        CHECK(fetch(&f, VSR_MSG_GET_STATE, foreign, 1, 3).consumed == 1);
        expect_unavailable(&f, 1);
    }
    CHECK(status(f.node).failure.code == VSR_FAILURE_NONE);
    finish(&f);
}

/* "transfer_timeout_ns expires idle offers" and "A valid range request
 * renews its retention": the offer discovered at time 0 would expire at 100;
 * a range request at 60 keeps it alive past 100, and an idle offer is gone
 * by 170. Responding never changes the served revision's metadata. */
static void renewal_and_expiry(void)
{
    struct fixture f = create(8, true);
    command(&f, 1);
    discover(&f);
    const struct response offer = take(&f);
    tick(&f, 60);
    range(&f, VSR_MSG_GET_STATE, offer.sequence, 1, 2);
    const struct response renewed = take(&f);
    CHECK(renewed.type == VSR_MSG_NEW_STATE && renewed.entries == 1);
    tick(&f, 110);
    range(&f, VSR_MSG_GET_STATE, offer.sequence, 1, 2);
    const struct response alive = take(&f);
    CHECK(alive.type == VSR_MSG_NEW_STATE && alive.entries == 1);
    CHECK(alive.sequence == offer.sequence && alive.committed == 1);
    tick(&f, 170);
    /* Past 110 + 100 the renewed offer has been released. */
    tick(&f, 220);
    range(&f, VSR_MSG_GET_STATE, offer.sequence, 1, 2);
    expect_unavailable(&f, 1);
    /* The receiver revalidates: a fresh discovery serves the same history. */
    discover(&f);
    const struct response again = take(&f);
    CHECK(again.type == VSR_MSG_NEW_STATE && again.log_end == 2);
    range(&f, VSR_MSG_GET_STATE, again.sequence, 1, 2);
    CHECK(take(&f).entries == 1);
    CHECK(status(f.node).failure.code == VSR_FAILURE_NONE);
    finish(&f);
}

/* "When capacity is needed for a newer fenced revision, the oldest
 * unreferenced offer may be evicted earlier; requests for it receive
 * STATE_UNAVAILABLE and must revalidate." With spare capacity the older
 * revision stays served until it expires. */
static void eviction(uint32_t transfers)
{
    struct fixture f = create(transfers, true);
    command(&f, 1);
    discover(&f);
    const struct response old = take(&f);
    CHECK(old.log_end == 2);
    command(&f, 2);
    tick(&f, 10);
    discover(&f);
    const struct response current = take(&f);
    CHECK(current.log_end == 3 && current.sequence > old.sequence);
    range(&f, VSR_MSG_GET_STATE, old.sequence, 1, 2);
    if (transfers == 1) {
        expect_unavailable(&f, 1);
    } else {
        const struct response chunk = take(&f);
        CHECK(chunk.type == VSR_MSG_NEW_STATE && chunk.entries == 1);
        CHECK(chunk.sequence == old.sequence && chunk.log_end == 2);
    }
    range(&f, VSR_MSG_GET_STATE, current.sequence, 1, 3);
    const struct response chunk = take(&f);
    CHECK(chunk.type == VSR_MSG_NEW_STATE &&
          chunk.sequence == current.sequence);
    CHECK(chunk.first == 1 && chunk.next == 2 && chunk.log_end == 3);
    CHECK(status(f.node).failure.code == VSR_FAILURE_NONE);
    finish(&f);
}

/* "temporary resource pressure returns AGAIN without consuming it" and
 * "Unconsumed events transfer nothing": with one transfer slot busy serving
 * an indexed read, a second range request is not consumed and keeps its
 * lease; it is admitted once the first response is out. Likewise a discovery
 * cannot displace the only offer while a send still references it. */
static void saturation(void)
{
    struct fixture f = create(1, true);
    command(&f, 1);
    discover(&f);
    const struct response offer = take(&f);
    range(&f, VSR_MSG_GET_STATE, offer.sequence, 1, 2);
    drive(&f, VSR_OP_LOAD);
    CHECK(find(f.node, VSR_OP_LOAD) != SIZE_MAX);
    const size_t leases = mem_node_leases(f.node);
    const struct vsr_revision revision = {{1, 1}, offer.sequence};
    struct mem_step step = fetch(&f, VSR_MSG_GET_STATE, revision, 1, 2);
    CHECK(step.result == VSR_AGAIN && step.consumed == 0);
    CHECK((step.flags & VSR_UPDATE_INPUT_BLOCKED) != 0);
    CHECK(mem_node_leases(f.node) == leases);
    CHECK(mem_node_complete(f.node, find(f.node, VSR_OP_LOAD), VSR_IO_OK));
    const struct response first = take(&f);
    CHECK(first.type == VSR_MSG_NEW_STATE && first.nonce == f.nonce - 1);
    range(&f, VSR_MSG_GET_STATE, offer.sequence, 1, 2);
    const struct response second = take(&f);
    CHECK(second.type == VSR_MSG_NEW_STATE && second.nonce == f.nonce);
    /* Hold the discovery response's send: its pin references the offer. */
    discover(&f);
    drive(&f, VSR_OP_SEND);
    CHECK(find(f.node, VSR_OP_SEND) != SIZE_MAX);
    {
        const struct vsr_span span = {"offer-command", 13};
        const struct vsr_blob body = {&span, 13, 1, 0};
        const struct vsr_request request = {
            {{700, 1}, 2}, 0, VSR_REQUEST_COMMAND, 0, &body};
        const struct vsr_event event = {VSR_EVENT_REQUEST, 0, 2, &request, 1};
        CHECK(mem_node_event(f.node, &event).consumed == 1);
    }
    drive(&f, VSR_OP_SEND);
    CHECK(status(f.node).committed == 2);
    const struct vsr_revision zero = {{0, 0}, 0};
    step = fetch(&f, VSR_MSG_GET_STATE, zero, 0, 0);
    CHECK(step.result == VSR_AGAIN && step.consumed == 0);
    CHECK((step.flags & VSR_UPDATE_INPUT_BLOCKED) != 0);
    CHECK(mem_node_complete(f.node, find(f.node, VSR_OP_SEND), VSR_IO_OK));
    const struct response held = take(&f);
    CHECK(held.log_end == 2);
    discover(&f);
    const struct response fresh = take(&f);
    CHECK(fresh.log_end == 3 && fresh.sequence > held.sequence);
    CHECK(status(f.node).failure.code == VSR_FAILURE_NONE);
    finish(&f);
}

/* "LOAD | Retry transient unavailability; ... unexpected absence ... fences
 * storage": the indexed read behind a range response is retried after
 * retry_ns on RETRY, and an offered range cannot be absent. "Active
 * loads/sends retain their pins until completion even if the offer expires." */
static void serve_load(int code)
{
    struct fixture f = create(8, true);
    command(&f, 1);
    discover(&f);
    const struct response offer = take(&f);
    range(&f, VSR_MSG_GET_STATE, offer.sequence, 1, 2);
    drive(&f, VSR_OP_LOAD);
    size_t index = find(f.node, VSR_OP_LOAD);
    CHECK(index != SIZE_MAX);
    const uint64_t first = mem_node_effect(f.node, index)->id;
    const struct vsr_store_read read =
        *(const struct vsr_store_read *)mem_node_effect(f.node, index)->data;
    CHECK(read.type == VSR_LOAD_LOG && read.sequence == offer.sequence);
    CHECK(read.first == 1 && read.end == 2);
    if (code != VSR_IO_RETRY) {
        /* Expire the offer first: the outstanding read keeps its pin. */
        CHECK(mem_node_time(f.node, 150).consumed == 1);
        drive(&f, VSR_OP_LOAD);
        index = find(f.node, VSR_OP_LOAD);
        CHECK(index != SIZE_MAX && mem_node_effect(f.node, index)->id == first);
    }
    CHECK(mem_node_complete(f.node, index, code));
    if (code == VSR_IO_RETRY) {
        drive(&f, VSR_OP_LOAD);
        CHECK(find(f.node, VSR_OP_LOAD) == SIZE_MAX);
        CHECK(mem_cluster_messages(f.cluster) == 0);
        tick(&f, 4);
        CHECK(find(f.node, VSR_OP_LOAD) == SIZE_MAX);
        CHECK(mem_node_time(f.node, 5).consumed == 1);
        drive(&f, VSR_OP_LOAD);
        index = find(f.node, VSR_OP_LOAD);
        CHECK(index != SIZE_MAX && mem_node_effect(f.node, index)->id != first);
        const struct vsr_store_read *again =
            mem_node_effect(f.node, index)->data;
        CHECK(again->sequence == read.sequence && again->first == 1);
        CHECK(mem_node_complete(f.node, index, VSR_IO_OK));
        const struct response chunk = take(&f);
        CHECK(chunk.type == VSR_MSG_NEW_STATE && chunk.entries == 1);
        CHECK(chunk.nonce == f.nonce);
        CHECK(status(f.node).failure.code == VSR_FAILURE_NONE);
    } else {
        const struct vsr_status current = status(f.node);
        CHECK(current.state == VSR_STATE_FAILED);
        CHECK(current.failure.code == VSR_FAILURE_STORAGE);
        CHECK(current.failure.operation == first);
        CHECK(current.failure.operation_type == VSR_OP_LOAD);
        CHECK(current.failure.status == code);
    }
    finish(&f);
}

/* "The first TIME establishes the origin; protocol timers ... do not fire
 * beforehand": an offer served before any TIME never expires until the clock
 * starts, then lives one transfer timeout from the origin. */
static void offer_before_time(void)
{
    struct fixture f = create(8, false);
    command(&f, 1);
    discover(&f);
    const struct response offer = take(&f);
    CHECK(offer.log_end == 2);
    drive(&f, NONE);
    range(&f, VSR_MSG_GET_STATE, offer.sequence, 1, 2);
    CHECK(take(&f).entries == 1);
    tick(&f, 1000);
    range(&f, VSR_MSG_GET_STATE, offer.sequence, 1, 2);
    CHECK(take(&f).entries == 1);
    tick(&f, 1099);
    range(&f, VSR_MSG_GET_STATE, offer.sequence, 1, 2);
    CHECK(take(&f).entries == 1);
    tick(&f, 1300);
    range(&f, VSR_MSG_GET_STATE, offer.sequence, 1, 2);
    expect_unavailable(&f, 1);
    CHECK(status(f.node).failure.code == VSR_FAILURE_NONE);
    finish(&f);
}

/* "STOPPED means no outstanding operations, pins, or queued releases
 * remain": stopping while a range request's indexed read is outstanding
 * cancels it and releases the offer's and the request's pins. */
static void stop_while_serving(void)
{
    struct fixture f = create(1, true);
    command(&f, 1);
    discover(&f);
    const struct response offer = take(&f);
    range(&f, VSR_MSG_GET_STATE, offer.sequence, 1, 2);
    drive(&f, VSR_OP_LOAD);
    CHECK(find(f.node, VSR_OP_LOAD) != SIZE_MAX);
    const struct vsr_event stop = {VSR_EVENT_STOP, 0, 0, NULL, 0};
    struct mem_step step = {0};
    for (size_t attempt = 0; attempt < 1000 && step.consumed == 0; attempt++)
        step = mem_node_event(f.node, &stop);
    CHECK(step.consumed == 1);
    for (size_t turn = 0; turn < 1000; turn++) {
        step = mem_node_event(f.node, NULL);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        for (size_t i = 0; i < mem_node_effects(f.node); i++) {
            if (mem_node_complete(f.node, i, VSR_IO_CANCELLED))
                break;
        }
        if (status(f.node).state == VSR_STATE_STOPPED)
            break;
    }
    CHECK(status(f.node).state == VSR_STATE_STOPPED);
    CHECK(mem_node_effects(f.node) == 0 && mem_node_leases(f.node) == 0);
    CHECK(mem_cluster_messages(f.cluster) == 0);
    CHECK(vsr_deinit(mem_node_core(f.node)) == VSR_OK);
    finish(&f);
}

static bool selected(int argc, char **argv, const char *name)
{
    if (argc == 1 || strcmp(argv[1], name) == 0) {
        fprintf(stderr, "offers: %s\n", name);
        return true;
    }
    return false;
}

int main(int argc, char **argv)
{
    CHECK(argc <= 2);
    if (selected(argc, argv, "ranges"))
        discovery_and_ranges();
    if (selected(argc, argv, "renewal"))
        renewal_and_expiry();
    if (selected(argc, argv, "eviction")) {
        eviction(1);
        eviction(8);
    }
    if (selected(argc, argv, "saturation"))
        saturation();
    if (selected(argc, argv, "serve-load")) {
        serve_load(VSR_IO_RETRY);
        serve_load(VSR_IO_NOT_FOUND);
        serve_load(VSR_IO_CORRUPT);
    }
    if (selected(argc, argv, "before-time"))
        offer_before_time();
    if (selected(argc, argv, "stop"))
        stop_while_serving();
    return 0;
}
