#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>
#include <string.h>

/* Contract tests for the "Requests, replies, and reads" table of
 * docs/vsr-api.md, for admission backpressure, and for batch_delay_ns. Every
 * scenario states the sentence of the contract it derives from. */

struct fixture {
    struct mem_cluster *cluster;
    struct mem_node *nodes[3];
    uint32_t count;
    uint64_t hold;     /* Bit per vsr_op_type withheld on every node. */
    uint64_t isolated; /* Bit per replica whose incoming messages drop. */
    uint32_t
        withheld_message; /* Message type never delivered, or UINT32_MAX. */
};

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    CHECK(result.failure.code == VSR_FAILURE_NONE);
    return result;
}

static bool held(const struct fixture *f, const struct vsr_op *op)
{
    return (f->hold & (UINT64_C(1) << op->type)) != 0;
}

/* Runs until idle, completing every effect not withheld and delivering every
 * message not withheld. Nothing advances logical time here. */
static void drive(struct fixture *f)
{
    for (unsigned turn = 0; turn < 100000; turn++) {
        bool progress = false;
        for (uint32_t i = 0; i < f->count; i++) {
            struct mem_node *node = f->nodes[i];
            if (!mem_node_alive(node))
                continue;
            const struct mem_step step = mem_node_event(node, NULL);
            CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            progress |=
                step.emitted != 0 || (step.flags & VSR_UPDATE_MORE) != 0;
            for (size_t j = 0; j < mem_node_effects(node); j++) {
                if (held(f, mem_node_effect(node, j)))
                    continue;
                if (mem_node_complete(node, j, VSR_IO_OK)) {
                    progress = true;
                    break;
                }
            }
        }
        for (size_t i = 0; i < mem_cluster_messages(f->cluster); i++) {
            uint64_t to;
            const struct vsr_message *message =
                mem_cluster_message(f->cluster, i, &to);
            if (message->type == f->withheld_message)
                continue;
            if ((f->isolated & (UINT64_C(1) << to)) != 0 ||
                !mem_node_alive(mem_cluster_find(f->cluster, to))) {
                mem_cluster_drop(f->cluster, i);
                progress = true;
                break;
            }
            if (mem_cluster_deliver(f->cluster, i)) {
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

static void time_all(struct fixture *f, uint64_t now)
{
    for (uint32_t i = 0; i < f->count; i++) {
        if (!mem_node_alive(f->nodes[i]))
            continue;
        for (unsigned attempt = 0;; attempt++) {
            CHECK(attempt < 1000);
            if (mem_node_time(f->nodes[i], now).consumed == 1)
                break;
            drive(f);
        }
    }
}

static struct fixture create(uint32_t count, bool witness,
                             const struct vsr_options *template)
{
    struct fixture f = {mem_cluster_create(), {0}, count, 0, 0, UINT32_MAX};
    struct vsr_member members[3];
    const struct vsr_membership group = {0, members, count,
                                         count == 1 ? 0u : 1u};
    for (uint32_t i = 0; i < count; i++)
        members[i] = (struct vsr_member){
            i + 1,
            witness && i + 1 == count ? VSR_MEMBER_WITNESS : VSR_MEMBER_FULL,
            0};
    for (uint32_t i = 0; i < count; i++) {
        struct vsr_options options = mem_options(i + 1, &group);
        if (template != NULL) {
            options.limits = template->limits;
            options.heartbeat_ns = template->heartbeat_ns;
            options.view_timeout_ns = template->view_timeout_ns;
            options.retry_ns = template->retry_ns;
            options.batch_delay_ns = template->batch_delay_ns;
        }
        f.nodes[i] = mem_cluster_add(f.cluster, &options);
        CHECK(mem_node_time(f.nodes[i], 0).consumed == 1);
    }
    drive(&f);
    for (uint32_t i = 0; i < count; i++)
        CHECK(status(f.nodes[i]).state == VSR_STATE_NORMAL);
    return f;
}

static struct mem_step request(struct mem_node *node, uint64_t client,
                               uint64_t number, uint64_t epoch,
                               unsigned char tag, size_t size, uint64_t route)
{
    unsigned char bytes[64];
    CHECK(size <= sizeof(bytes));
    memset(bytes, tag, sizeof(bytes));
    const struct vsr_span span = {bytes, size};
    const struct vsr_blob body = {size == 0 ? NULL : &span, size,
                                  size == 0 ? 0u : 1u, 0};
    const struct vsr_request request = {
        {{7, client}, number}, epoch, VSR_REQUEST_COMMAND, 0, &body};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, route, &request, 1};
    return mem_node_event(node, &event);
}

static void command(struct mem_node *node, uint64_t client, uint64_t number,
                    unsigned char tag, uint64_t route)
{
    const struct mem_step step =
        request(node, client, number, 0, tag, 32, route);
    CHECK(step.consumed == 1);
}

static void query(struct mem_node *node, uint64_t client, uint64_t route)
{
    const struct vsr_id id = {7, client};
    const struct vsr_event event = {VSR_EVENT_CLIENT_QUERY, 0, route, &id, 1};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

static const struct vsr_reply *reply(struct mem_node *node, uint64_t route)
{
    for (size_t i = mem_node_replies(node); i > 0; i--) {
        uint64_t found;
        const struct vsr_reply *answer = mem_node_reply(node, i - 1, &found);
        if (found == route)
            return answer;
    }
    CHECK(false);
    return NULL;
}

static size_t replies_for(struct mem_node *node, uint64_t route)
{
    size_t count = 0;
    for (size_t i = 0; i < mem_node_replies(node); i++) {
        uint64_t found;
        (void)mem_node_reply(node, i, &found);
        count += found == route;
    }
    return count;
}

static void restart(struct fixture *f, struct mem_node *node,
                    struct vsr_id incarnation)
{
    mem_node_crash(node);
    CHECK(mem_node_restart(node, incarnation) == VSR_OK);
    CHECK(mem_node_time(node, 0).consumed == 1);
    drive(f);
    CHECK(status(node).state == VSR_STATE_NORMAL);
}

/* "Identity reuse with different contents violates the client contract. The
 * core can reject conflicts while the original entry is available; completed
 * records do not retain command bodies for indefinite comparison." */
static void conflicting_retry_invalid(void)
{
    struct fixture f = create(1, false, NULL);
    struct mem_node *node = f.nodes[0];
    /* (a) The original is still pending: its route holds the request. */
    f.hold = UINT64_C(1) << VSR_OP_APPLY;
    command(node, 1, 1, 'A', 1);
    drive(&f);
    CHECK(status(node).applied == 0 && mem_node_replies(node) == 0);
    command(node, 1, 1, 'B', 2);
    drive(&f);
    CHECK(reply(node, 2)->status == VSR_REPLY_INVALID);
    CHECK(reply(node, 2)->op == 0 && reply(node, 2)->flags == 0);
    f.hold = 0;
    drive(&f);
    CHECK(reply(node, 1)->status == VSR_REPLY_OK);
    CHECK(reply(node, 1)->op == 1 && status(node).applied == 1);
    CHECK(mem_node_history(node, 1)->request.number == 1);
    CHECK(((const unsigned char
                *)((const struct vsr_blob *)mem_node_history(node, 1)->body)
               ->spans[0]
               .data)[0] == 'A');
    /* (b) Executed and cached: the contract permits either rejecting the
     * conflict or answering from the completed record; it never executes
     * the different body. */
    command(node, 1, 1, 'C', 3);
    drive(&f);
    CHECK(reply(node, 3)->status == VSR_REPLY_INVALID ||
          (reply(node, 3)->status == VSR_REPLY_OK && reply(node, 3)->op == 1 &&
           reply(node, 3)->result.code == reply(node, 1)->result.code));
    CHECK(status(node).applied == 1 && mem_node_history(node, 2) == NULL);
    mem_cluster_destroy(f.cluster);
}

/* (c) After a durable restart only the retained-request index knows a stored
 * but uncommitted entry: the core loads it (LOAD_REQUEST) and compares the
 * retried body against it before deciding. The primary's acknowledgments are
 * withheld so the entry stays uncommitted across the restart. */
static void conflicting_retry_after_restart(void)
{
    struct fixture f = create(3, false, NULL);
    struct mem_node *node = f.nodes[0];
    f.withheld_message = VSR_MSG_PREPARE_OK;
    command(node, 2, 1, 'A', 1);
    drive(&f);
    CHECK(status(node).committed == 0 && mem_node_replies(node) == 0);
    restart(&f, node, (struct vsr_id){1, 2});
    CHECK(status(node).primary == 1 && status(node).committed == 0);
    bool loaded_request = false;
    command(node, 2, 1, 'B', 2);
    for (unsigned turn = 0; turn < 1000 && !loaded_request; turn++) {
        const struct mem_step step = mem_node_event(node, NULL);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        for (size_t i = 0; i < mem_node_effects(node); i++) {
            const struct vsr_op *op = mem_node_effect(node, i);
            if (op->type == VSR_OP_LOAD &&
                ((const struct vsr_store_read *)op->data)->type ==
                    VSR_LOAD_REQUEST)
                loaded_request = true;
            if (mem_node_complete(node, i, VSR_IO_OK))
                break;
        }
    }
    CHECK(loaded_request);
    drive(&f);
    CHECK(reply(node, 2)->status == VSR_REPLY_INVALID);
    CHECK(status(node).committed == 0);
    /* The faithful retry joins the retained proposal and resolves with it. */
    command(node, 2, 1, 'A', 3);
    drive(&f);
    CHECK(replies_for(node, 3) == 0);
    f.withheld_message = UINT32_MAX;
    drive(&f);
    CHECK(reply(node, 3)->status == VSR_REPLY_OK && reply(node, 3)->op == 1);
    CHECK((reply(node, 3)->flags & VSR_REPLY_EXECUTED) != 0);
    CHECK(status(node).applied == 1 && mem_node_history(node, 2) == NULL);
    CHECK(((const unsigned char
                *)((const struct vsr_blob *)mem_node_history(node, 1)->body)
               ->spans[0]
               .data)[0] == 'A');
    mem_cluster_destroy(f.cluster);
}

/* "A newer request cannot overtake a known pending request from the same
 * client: it receives BUSY until that request resolves." "STALE_REQUEST: A
 * newer request is known; this request will not execute again." Duplicates
 * of the latest completed request are answered from the cached result, and
 * after a restart from the indexed client record. */
static void ordering(void)
{
    struct fixture f = create(1, false, NULL);
    struct mem_node *node = f.nodes[0];
    f.hold = UINT64_C(1) << VSR_OP_APPLY;
    command(node, 1, 1, 'A', 1);
    drive(&f);
    command(node, 1, 2, 'B', 2);
    drive(&f);
    CHECK(reply(node, 2)->status == VSR_REPLY_BUSY);
    CHECK(status(node).applied == 0);
    f.hold = 0;
    drive(&f);
    CHECK(reply(node, 1)->status == VSR_REPLY_OK && reply(node, 1)->op == 1);
    command(node, 1, 2, 'B', 3);
    drive(&f);
    CHECK(reply(node, 3)->status == VSR_REPLY_OK && reply(node, 3)->op == 2);
    CHECK((reply(node, 3)->flags & VSR_REPLY_EXECUTED) != 0);
    command(node, 1, 1, 'A', 4);
    drive(&f);
    CHECK(reply(node, 4)->status == VSR_REPLY_STALE_REQUEST);
    CHECK(reply(node, 4)->op == 0 && reply(node, 4)->flags == 0);
    command(node, 1, 2, 'B', 5);
    drive(&f);
    CHECK(reply(node, 5)->status == VSR_REPLY_OK && reply(node, 5)->op == 2);
    CHECK((reply(node, 5)->flags & VSR_REPLY_EXECUTED) != 0);
    CHECK(reply(node, 5)->result.code == reply(node, 3)->result.code);
    CHECK(status(node).applied == 2 && mem_node_history(node, 3) == NULL);
    restart(&f, node, (struct vsr_id){1, 2});
    command(node, 1, 2, 'B', 6);
    drive(&f);
    CHECK(reply(node, 6)->status == VSR_REPLY_OK && reply(node, 6)->op == 2);
    CHECK((reply(node, 6)->flags & VSR_REPLY_EXECUTED) != 0);
    CHECK(reply(node, 6)->result.code == reply(node, 3)->result.code);
    command(node, 1, 1, 'A', 7);
    drive(&f);
    CHECK(reply(node, 7)->status == VSR_REPLY_STALE_REQUEST);
    CHECK(status(node).applied == 2 && mem_node_history(node, 3) == NULL);
    mem_cluster_destroy(f.cluster);
}

static void check_epoch(struct mem_node *node, uint64_t client, uint64_t epoch,
                        uint64_t routing, uint64_t route)
{
    const struct vsr_check_epoch body = {epoch};
    const struct vsr_request input = {
        {{7, client}, 1}, routing, VSR_REQUEST_CHECK_EPOCH, 0, &body};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, route, &input, 1};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

static void reconfigure(struct mem_node *node, uint64_t client, uint64_t epoch,
                        uint64_t routing, uint64_t route)
{
    const struct vsr_member members[] = {{1, VSR_MEMBER_FULL, 0},
                                         {2, VSR_MEMBER_FULL, 0},
                                         {3, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership next = {epoch, members, 3, 1};
    const struct vsr_request input = {
        {{7, client}, 1}, routing, VSR_REQUEST_RECONFIGURE, 0, &next};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, route, &input, 1};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

/* "NOT_PRIMARY: Retry using the advertised primary, if known"; "NEW_EPOCH:
 * Routing epoch differs; use returned membership and retry"; "INVALID /
 * LIMIT: Well-formed request violates a protocol rule"; a CHECK_EPOCH target
 * "epoch 0 trivially ready"; "a fresh proposal must target routing epoch + 1";
 * "BUSY: Admission or transition temporarily prevents handling". */
static void routing(void)
{
    for (unsigned witness = 0; witness < 2; witness++) {
        struct fixture f = create(3, witness != 0, NULL);
        command(f.nodes[1], 1, 1, 'A', 1);
        drive(&f);
        CHECK(reply(f.nodes[1], 1)->status == VSR_REPLY_NOT_PRIMARY);
        CHECK(reply(f.nodes[1], 1)->primary == 1);
        CHECK(reply(f.nodes[1], 1)->membership != NULL);
        CHECK(reply(f.nodes[1], 1)->membership->count == 3);
        command(f.nodes[2], 1, 1, 'A', 2);
        query(f.nodes[2], 1, 3);
        drive(&f);
        CHECK(reply(f.nodes[2], 2)->status == VSR_REPLY_NOT_PRIMARY);
        /* "Witnesses do not generate application-result records; client
         * queries receive NOT_PRIMARY." A full backup answers locally. */
        CHECK(
            reply(f.nodes[2], 3)->status ==
            (witness != 0 ? VSR_REPLY_NOT_PRIMARY : VSR_REPLY_CLIENT_UNKNOWN));
        CHECK(status(f.nodes[0]).applied == 0);
        /* Nothing above was logged. */
        CHECK(request(f.nodes[0], 1, 1, 1, 'A', 32, 4).consumed == 1);
        drive(&f);
        CHECK(reply(f.nodes[0], 4)->status == VSR_REPLY_NEW_EPOCH);
        CHECK(reply(f.nodes[0], 4)->membership != NULL);
        CHECK(reply(f.nodes[0], 4)->membership->epoch == 0);
        check_epoch(f.nodes[0], 2, 1, 0, 5);
        drive(&f);
        CHECK(reply(f.nodes[0], 5)->status == VSR_REPLY_INVALID);
        CHECK(status(f.nodes[0]).applied == 0);
        check_epoch(f.nodes[0], 3, 0, 0, 6);
        drive(&f);
        CHECK(reply(f.nodes[0], 6)->status == VSR_REPLY_OK);
        CHECK((reply(f.nodes[0], 6)->flags & VSR_REPLY_EXECUTED) != 0);
        CHECK(reply(f.nodes[0], 6)->result.data.size == 0);
        CHECK(status(f.nodes[0]).applied == 1);
        reconfigure(f.nodes[0], 4, 2, 0, 7);
        drive(&f);
        CHECK(reply(f.nodes[0], 7)->status == VSR_REPLY_INVALID);
        CHECK(status(f.nodes[0]).epoch == 0 && status(f.nodes[0]).applied == 1);
        mem_cluster_destroy(f.cluster);
    }
    /* A candidate in VIEW_CHANGE is the would-be primary of its view but
     * cannot admit work: BUSY. Its peer redirects to that candidate. */
    struct fixture f = create(3, false, NULL);
    mem_node_crash(f.nodes[0]);
    f.isolated = (UINT64_C(1) << 2) | (UINT64_C(1) << 3);
    time_all(&f, 60);
    drive(&f);
    CHECK(status(f.nodes[1]).state == VSR_STATE_VIEW_CHANGE);
    CHECK(status(f.nodes[1]).view == 1 && status(f.nodes[1]).primary == 2);
    CHECK(status(f.nodes[2]).state == VSR_STATE_VIEW_CHANGE);
    command(f.nodes[1], 1, 1, 'A', 1);
    command(f.nodes[2], 1, 1, 'A', 2);
    drive(&f);
    CHECK(reply(f.nodes[1], 1)->status == VSR_REPLY_BUSY);
    CHECK(reply(f.nodes[2], 2)->status == VSR_REPLY_NOT_PRIMARY);
    CHECK(reply(f.nodes[2], 2)->primary == 2);
    mem_cluster_destroy(f.cluster);
}

/* "An inherited RECONFIGURE at the log end prevents further old-epoch
 * proposals" and "Until STEADY, the next RECONFIGURE is rejected with BUSY."
 * Both rejections are transient: the same proposal succeeds once the handoff
 * completes. */
static void reconfiguration_busy(void)
{
    struct fixture f = create(3, false, NULL);
    struct mem_node *node = f.nodes[0];
    f.withheld_message = VSR_MSG_EPOCH_STARTED;
    reconfigure(node, 1, 1, 0, 1);
    reconfigure(node, 2, 1, 0, 2);
    drive(&f);
    CHECK(reply(node, 2)->status == VSR_REPLY_BUSY);
    CHECK(reply(node, 1)->status == VSR_REPLY_OK);
    CHECK(status(node).epoch == 1);
    CHECK(status(node).configuration->phase == VSR_EPOCH_INSTALLED);
    reconfigure(node, 3, 2, 1, 3);
    drive(&f);
    CHECK(reply(node, 3)->status == VSR_REPLY_BUSY);
    CHECK(status(node).epoch == 1);
    f.withheld_message = UINT32_MAX;
    drive(&f);
    CHECK(status(node).configuration->phase == VSR_EPOCH_STEADY);
    reconfigure(node, 3, 2, 1, 4);
    drive(&f);
    CHECK(reply(node, 4)->status == VSR_REPLY_OK);
    CHECK(status(node).epoch == 2);
    for (uint32_t i = 0; i < 3; i++)
        CHECK(status(f.nodes[i]).epoch == 2 &&
              status(f.nodes[i]).state == VSR_STATE_NORMAL);
    mem_cluster_destroy(f.cluster);
}

/* "CLIENT_STATE: Latest locally known request, with result only if
 * EXECUTED"; "CLIENT_UNKNOWN: No local completed or retained request;
 * queried client, number/op zero"; "Client queries instead report the latest
 * locally known ID for the queried client." */
static void client_query(void)
{
    struct fixture f = create(1, false, NULL);
    struct mem_node *node = f.nodes[0];
    query(node, 9, 1);
    drive(&f);
    CHECK(reply(node, 1)->status == VSR_REPLY_CLIENT_UNKNOWN);
    CHECK(reply(node, 1)->request.client.lo == 9);
    CHECK(reply(node, 1)->request.number == 0 && reply(node, 1)->op == 0);
    CHECK(reply(node, 1)->flags == 0);
    f.hold = UINT64_C(1) << VSR_OP_APPLY;
    command(node, 9, 1, 'A', 2);
    drive(&f);
    query(node, 9, 3);
    drive(&f);
    CHECK(reply(node, 3)->status == VSR_REPLY_CLIENT_STATE);
    CHECK(reply(node, 3)->request.number == 1 && reply(node, 3)->op == 1);
    CHECK(reply(node, 3)->flags == 0);
    CHECK(reply(node, 3)->result.data.size == 0);
    f.hold = 0;
    drive(&f);
    CHECK(reply(node, 2)->status == VSR_REPLY_OK);
    query(node, 9, 4);
    drive(&f);
    CHECK(reply(node, 4)->status == VSR_REPLY_CLIENT_STATE);
    CHECK(reply(node, 4)->request.number == 1 && reply(node, 4)->op == 1);
    CHECK((reply(node, 4)->flags & VSR_REPLY_EXECUTED) != 0);
    CHECK(reply(node, 4)->result.code == reply(node, 2)->result.code);
    restart(&f, node, (struct vsr_id){1, 2});
    query(node, 9, 5);
    drive(&f);
    CHECK(reply(node, 5)->status == VSR_REPLY_CLIENT_STATE);
    CHECK(reply(node, 5)->request.number == 1 && reply(node, 5)->op == 1);
    CHECK((reply(node, 5)->flags & VSR_REPLY_EXECUTED) != 0);
    CHECK(reply(node, 5)->result.code == reply(node, 2)->result.code);
    query(node, 10, 6);
    drive(&f);
    CHECK(reply(node, 6)->status == VSR_REPLY_CLIENT_UNKNOWN);
    mem_cluster_destroy(f.cluster);
}

/* "Duplicate attempts may share one pending route; admission is not a
 * promise that every retransmission will receive a separate reply." */
static void duplicates_share_route(void)
{
    struct fixture f = create(1, false, NULL);
    struct mem_node *node = f.nodes[0];
    f.hold = UINT64_C(1) << VSR_OP_APPLY;
    command(node, 1, 1, 'A', 1);
    drive(&f);
    command(node, 1, 1, 'A', 2);
    command(node, 1, 1, 'A', 3);
    drive(&f);
    CHECK(mem_node_replies(node) == 0);
    f.hold = 0;
    drive(&f);
    CHECK(status(node).applied == 1 && mem_node_history(node, 2) == NULL);
    CHECK(mem_node_replies(node) >= 1 && mem_node_replies(node) <= 3);
    for (size_t i = 0; i < mem_node_replies(node); i++) {
        uint64_t route;
        const struct vsr_reply *answer = mem_node_reply(node, i, &route);
        CHECK(route >= 1 && route <= 3);
        CHECK(answer->status == VSR_REPLY_OK && answer->op == 1);
        CHECK((answer->flags & VSR_REPLY_EXECUTED) != 0);
    }
    CHECK(replies_for(node, 3) == 1);
    mem_cluster_destroy(f.cluster);
}

static struct vsr_options minimum_template(void)
{
    const struct vsr_member member = {1, VSR_MEMBER_FULL, 0};
    const struct vsr_membership group = {0, &member, 1, 0};
    struct vsr_options options = mem_options(1, &group);
    options.limits.log_cache_entries = 4;
    options.limits.batch_entries = 2;
    options.limits.client_cache_entries = 1;
    options.limits.transfers = 1;
    options.limits.input_leases = 9;
    options.limits.command_bytes = 18;
    options.limits.result_bytes = 18;
    options.limits.manifest_bytes = 8;
    options.limits.message_bytes = 26;
    options.limits.pinned_payload_bytes = 112;
    return options;
}

/* "A permanently inadmissible graph returns ELIMIT; temporary resource
 * pressure returns AGAIN without consuming it." "Unconsumed events transfer
 * nothing." A blocked proposal is accepted once completions free capacity,
 * and every accepted request still completes. */
static void payload_backpressure(void)
{
    const struct vsr_options template = minimum_template();
    struct fixture f = create(1, false, &template);
    struct mem_node *node = f.nodes[0];
    const size_t bytes = (size_t) template.limits.command_bytes;
    struct mem_step step = request(node, 1, 1, 0, 'X', bytes + 1, 1);
    CHECK(step.result == VSR_ELIMIT && step.consumed == 0);
    CHECK(mem_node_leases(node) == 0);
    CHECK(status(node).failure.code == VSR_FAILURE_NONE);
    f.hold = UINT64_C(1) << VSR_OP_APPLY;
    uint64_t accepted = 0;
    uint64_t blocked = 0;
    for (uint64_t client = 1; client <= 64; client++) {
        drive(&f);
        const size_t leases = mem_node_leases(node);
        step = request(node, client, 1, 0, 'X', bytes, client);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        if (step.consumed == 0) {
            CHECK(step.result == VSR_AGAIN);
            CHECK((step.flags & VSR_UPDATE_INPUT_BLOCKED) != 0);
            CHECK(mem_node_leases(node) == leases);
            blocked = client;
            break;
        }
        accepted = client;
    }
    CHECK(blocked != 0 && accepted != 0);
    CHECK(status(node).applied == 0);
    f.hold = 0;
    drive(&f);
    for (unsigned attempt = 0;; attempt++) {
        CHECK(attempt < 100);
        step = request(node, blocked, 1, 0, 'X', bytes, blocked);
        if (step.consumed == 1)
            break;
        drive(&f);
    }
    drive(&f);
    for (uint64_t client = 1; client <= blocked; client++) {
        CHECK(reply(node, client)->status == VSR_REPLY_OK);
        CHECK((reply(node, client)->flags & VSR_REPLY_EXECUTED) != 0);
    }
    CHECK(status(node).applied == blocked);
    mem_cluster_destroy(f.cluster);
}

static bool appending(struct mem_node *node)
{
    for (size_t i = 0; i < mem_node_effects(node); i++) {
        const struct vsr_op *op = mem_node_effect(node, i);
        if (op->type != VSR_OP_STORE)
            continue;
        const struct vsr_store *store = op->data;
        for (uint32_t j = 0; j < store->count; j++)
            if (store->changes[j].type == VSR_STORE_APPEND)
                return true;
    }
    return false;
}

/* Drains without completing STOREs, reporting whether an APPEND was issued
 * and the last replacement deadline the core returned. */
static bool drain_for_append(struct fixture *f, uint64_t *deadline)
{
    struct mem_node *node = f->nodes[0];
    f->hold = UINT64_C(1) << VSR_OP_STORE;
    for (unsigned turn = 0; turn < 1000; turn++) {
        const struct mem_step step = mem_node_event(node, NULL);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        *deadline = step.deadline;
        bool progress =
            step.emitted != 0 || (step.flags & VSR_UPDATE_MORE) != 0;
        for (size_t j = 0; j < mem_node_effects(node); j++) {
            if (held(f, mem_node_effect(node, j)))
                continue;
            if (mem_node_complete(node, j, VSR_IO_OK)) {
                progress = true;
                break;
            }
        }
        if (appending(node))
            return true;
        if (!progress)
            return false;
    }
    CHECK(false);
    return false;
}

/* "batch_delay_ns = 0 disables intentional delay; positive values bound it
 * from the first queued entry. Heartbeats and retries cannot wait for
 * application batching." A full batch never waits. */
static void batch_delay(void)
{
    for (unsigned variant = 0; variant < 3; variant++) {
        const struct vsr_member member = {1, VSR_MEMBER_FULL, 0};
        const struct vsr_membership group = {0, &member, 1, 0};
        struct vsr_options template = mem_options(1, &group);
        template.batch_delay_ns = 20;
        template.limits.batch_entries = 2;
        if (variant != 2) {
            /* Every other timer is later than the batching deadline. */
            template.heartbeat_ns = 100;
            template.view_timeout_ns = 200;
            template.retry_ns = 50;
        }
        struct fixture f = create(1, false, &template);
        struct mem_node *node = f.nodes[0];
        uint64_t deadline = VSR_NO_DEADLINE;
        command(node, 1, 1, 'A', 1);
        if (variant == 1) {
            /* A second entry fills the batch before the delay elapses. */
            command(node, 2, 1, 'B', 2);
            CHECK(drain_for_append(&f, &deadline));
            f.hold = 0;
            drive(&f);
            CHECK(reply(node, 1)->status == VSR_REPLY_OK);
            CHECK(reply(node, 2)->status == VSR_REPLY_OK);
            CHECK(status(node).applied == 2);
            mem_cluster_destroy(f.cluster);
            continue;
        }
        CHECK(!drain_for_append(&f, &deadline));
        /* The batching deadline is reported, unless an earlier protocol
         * timer (heartbeat or retry) is due first. */
        CHECK(deadline == (variant == 0 ? 20u : 5u));
        CHECK(mem_node_replies(node) == 0);
        CHECK(mem_node_time(node, 10).consumed == 1);
        CHECK(!drain_for_append(&f, &deadline));
        CHECK(deadline == (variant == 0 ? 20u : 15u));
        CHECK(mem_node_time(node, 20).consumed == 1);
        CHECK(drain_for_append(&f, &deadline));
        f.hold = 0;
        drive(&f);
        CHECK(reply(node, 1)->status == VSR_REPLY_OK);
        CHECK(status(node).applied == 1);
        mem_cluster_destroy(f.cluster);
    }
}

int main(int argc, char **argv)
{
    CHECK(argc <= 2);
    if (argc == 1 || strcmp(argv[1], "conflict") == 0)
        conflicting_retry_invalid();
    if (argc == 1 || strcmp(argv[1], "conflict-restart") == 0)
        conflicting_retry_after_restart();
    if (argc == 1 || strcmp(argv[1], "ordering") == 0)
        ordering();
    if (argc == 1 || strcmp(argv[1], "routing") == 0)
        routing();
    if (argc == 1 || strcmp(argv[1], "reconfigure") == 0)
        reconfiguration_busy();
    if (argc == 1 || strcmp(argv[1], "query") == 0)
        client_query();
    if (argc == 1 || strcmp(argv[1], "duplicates") == 0)
        duplicates_share_route();
    if (argc == 1 || strcmp(argv[1], "backpressure") == 0)
        payload_backpressure();
    if (argc == 1 || strcmp(argv[1], "batch-delay") == 0)
        batch_delay();
    return 0;
}
