#include "config.h"

#include "lib/check.h"
#include "lib/random.h"
#include "vsr-client.h"

#include <inttypes.h>
#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* vsr-client.h: sans-IO bookkeeping for a client of a VSR group. These tests
 * pin its contract: arena planning, lanes, attempts, reply interpretation,
 * timeouts and backoff, topology, causal-read positions, and persistence. */

#define TIMEOUT UINT64_C(100)
#define BACKOFF UINT64_C(10)
#define BACKOFF_MAX UINT64_C(50)

struct fixture {
    struct vsr_member members[3];
    struct vsr_membership seed;
    struct vsr_client_options options;
    void *memory;
    size_t size;
    struct vsr_client *client;
};

static const struct vsr_blob empty_command = {NULL, 0, 0, 0};

static void setup_options(struct fixture *f, uint32_t lanes, uint32_t members)
{
    memset(f, 0, sizeof(*f));
    f->members[0] = (struct vsr_member){1, VSR_MEMBER_FULL, 0};
    f->members[1] = (struct vsr_member){2, VSR_MEMBER_FULL, 0};
    f->members[2] = (struct vsr_member){3, VSR_MEMBER_FULL, 0};
    f->seed = (struct vsr_membership){0, f->members, 3, 1};
    f->options = (struct vsr_client_options){
        .seed = &f->seed,
        .lanes = lanes,
        .members = members,
        .request_timeout_ns = TIMEOUT,
        .backoff_ns = BACKOFF,
        .backoff_max_ns = BACKOFF_MAX,
    };
}

static struct vsr_client *create(struct fixture *f)
{
    struct vsr_layout layout;
    CHECK(vsr_client_layout(&f->options, &layout) == VSR_OK);
    CHECK(layout.size != 0 && layout.size % layout.alignment == 0);
    f->memory = aligned_alloc(layout.alignment, layout.size);
    CHECK(f->memory != NULL);
    f->size = layout.size;
    CHECK(vsr_client_init(f->memory, f->size, &f->options, &f->client) ==
          VSR_OK);
    CHECK(f->client != NULL);
    return f->client;
}

static void setup(struct fixture *f, uint32_t lanes, uint32_t members)
{
    setup_options(f, lanes, members);
    (void)create(f);
}

static void teardown(struct fixture *f)
{
    free(f->memory);
}

static struct vsr_id incarnation(uint64_t n)
{
    return (struct vsr_id){0x1234, n};
}

static uint32_t open_lane(struct vsr_client *client, uint64_t n,
                          uint64_t next_number)
{
    uint32_t lane = VSR_CLIENT_NO_LANE;
    CHECK(vsr_client_open(client, incarnation(n), next_number, &lane) ==
          VSR_OK);
    return lane;
}

static struct vsr_client_lane_status lane_status(struct vsr_client *client,
                                                 uint32_t lane)
{
    struct vsr_client_lane_status status;
    vsr_client_lane_status(client, lane, &status);
    return status;
}

static struct vsr_client_status client_status(struct vsr_client *client)
{
    struct vsr_client_status status;
    vsr_client_get_status(client, &status);
    return status;
}

static struct vsr_reply answer(const struct vsr_client_attempt *attempt,
                               uint32_t status)
{
    struct vsr_reply reply;
    memset(&reply, 0, sizeof(reply));
    reply.request = attempt->request.id;
    reply.status = status;
    if (status == VSR_REPLY_OK) {
        reply.flags = VSR_REPLY_EXECUTED;
    }
    return reply;
}

static bool same_request(const struct vsr_request *a,
                         const struct vsr_request *b)
{
    return a->id.client.hi == b->id.client.hi &&
           a->id.client.lo == b->id.client.lo && a->id.number == b->id.number &&
           a->type == b->type && a->body == b->body && a->reserved == 0 &&
           b->reserved == 0;
}

static struct vsr_client_attempt begin(struct vsr_client *client, uint32_t lane,
                                       uint64_t now)
{
    struct vsr_client_attempt attempt;
    CHECK(vsr_client_begin(client, lane, VSR_REQUEST_COMMAND, &empty_command,
                           now, &attempt) == VSR_OK);
    return attempt;
}

static uint32_t reply(struct vsr_client *client, uint32_t lane,
                      const struct vsr_reply *r, uint64_t now,
                      struct vsr_client_outcome *outcome)
{
    CHECK(vsr_client_reply(client, lane, r, now, outcome) == VSR_OK);
    CHECK(outcome->lane == lane);
    return outcome->action;
}

/* The CRC-32C the image trailer uses, to forge well-checksummed images. */
static uint32_t crc32c(const unsigned char *bytes, size_t size)
{
    uint32_t crc = UINT32_MAX;
    for (size_t i = 0; i < size; i++) {
        crc ^= (uint32_t)bytes[i];
        for (unsigned bit = 0; bit < 8; bit++) {
            crc = (crc >> 1) ^ ((crc & 1u) != 0 ? UINT32_C(0x82F63B78) : 0u);
        }
    }
    return crc ^ UINT32_MAX;
}

static void reseal(unsigned char *bytes, size_t size)
{
    uint32_t crc = crc32c(bytes, size - 4);
    for (unsigned i = 0; i < 4; i++) {
        bytes[size - 4 + i] = (unsigned char)((crc >> (8 * i)) & 0xffu);
    }
}

/* "Positive fixed capacities"; request_timeout_ns and backoff_ns nonzero,
 * backoff_max_ns >= backoff_ns, cache_line_bytes a power of two, reserved
 * zero. On error the layout is {0, 0}. */
static void layout_errors(void)
{
    struct fixture f;
    struct vsr_layout layout = {1, 1};
    setup_options(&f, 2, 3);
    CHECK(vsr_client_layout(&f.options, NULL) == VSR_EINVAL);
    CHECK(vsr_client_layout(NULL, &layout) == VSR_EINVAL);
    CHECK(layout.size == 0 && layout.alignment == 0);

#define INVALID(field, value, expected)                                        \
    do {                                                                       \
        struct vsr_client_options o = f.options;                               \
        o.field = value;                                                       \
        layout = (struct vsr_layout){1, 1};                                    \
        CHECK(vsr_client_layout(&o, &layout) == (expected));                   \
        CHECK(layout.size == 0 && layout.alignment == 0);                      \
    } while (0)
    INVALID(lanes, 0, VSR_EINVAL);
    INVALID(members, 0, VSR_EINVAL);
    INVALID(request_timeout_ns, 0, VSR_EINVAL);
    INVALID(request_timeout_ns, VSR_NO_DEADLINE, VSR_EINVAL);
    INVALID(backoff_ns, 0, VSR_EINVAL);
    INVALID(backoff_max_ns, BACKOFF - 1, VSR_EINVAL);
    INVALID(backoff_max_ns, VSR_NO_DEADLINE, VSR_EINVAL);
    INVALID(cache_line_bytes, 24, VSR_EINVAL);
    INVALID(reserved, 1, VSR_EINVAL);
    /* The seed must be a valid membership that fits. */
    INVALID(members, 2, VSR_ELIMIT);
#undef INVALID
    {
        struct vsr_client_options o = f.options;
        struct vsr_member unsorted[3] = {f.members[1], f.members[0],
                                         f.members[2]};
        struct vsr_membership bad = {0, unsorted, 3, 1};
        o.seed = &bad;
        CHECK(vsr_client_layout(&o, &layout) == VSR_EINVAL);
        bad = (struct vsr_membership){0, f.members, 3, 2};
        CHECK(vsr_client_layout(&o, &layout) == VSR_EINVAL);
        o.seed = NULL;
        CHECK(vsr_client_layout(&o, &layout) == VSR_OK);
    }

    const uint32_t lines[] = {0, 8, 64, 128, 4096};
    for (size_t i = 0; i < sizeof(lines) / sizeof(lines[0]); i++) {
        f.options.cache_line_bytes = lines[i];
        CHECK(vsr_client_layout(&f.options, &layout) == VSR_OK);
        CHECK((layout.alignment & (layout.alignment - 1)) == 0);
        CHECK(layout.alignment >= alignof(max_align_t));
        CHECK(layout.alignment >= (lines[i] == 0 ? 64u : lines[i]));
        CHECK(layout.size % layout.alignment == 0);
    }
    /* More lanes or members never shrink the arena. */
    f.options.cache_line_bytes = 0;
    CHECK(vsr_client_layout(&f.options, &layout) == VSR_OK);
    size_t base = layout.size;
    f.options.lanes = 64;
    CHECK(vsr_client_layout(&f.options, &layout) == VSR_OK);
    CHECK(layout.size > base);
    f.options.members = 64;
    size_t lanes = layout.size;
    CHECK(vsr_client_layout(&f.options, &layout) == VSR_OK);
    CHECK(layout.size > lanes);
}

/* Init: NULL arguments and misalignment are EINVAL, a short arena ELIMIT; on
 * error *out is NULL, except that an out inside the arena is not written.
 * Options and seed are copied. */
static void init_errors(void)
{
    struct fixture f;
    struct vsr_layout layout;
    struct vsr_client *client = (void *)&f;
    setup_options(&f, 2, 4);
    CHECK(vsr_client_layout(&f.options, &layout) == VSR_OK);
    unsigned char *memory = aligned_alloc(layout.alignment, 2 * layout.size);
    CHECK(memory != NULL);

    CHECK(vsr_client_init(memory, layout.size, &f.options, NULL) == VSR_EINVAL);
    CHECK(vsr_client_init(NULL, layout.size, &f.options, &client) ==
          VSR_EINVAL);
    CHECK(client == NULL);
    client = (void *)&f;
    CHECK(vsr_client_init(memory, layout.size, NULL, &client) == VSR_EINVAL);
    CHECK(client == NULL);
    client = (void *)&f;
    CHECK(vsr_client_init(memory + 8, layout.size, &f.options, &client) ==
          VSR_EINVAL);
    CHECK(client == NULL);
    client = (void *)&f;
    CHECK(vsr_client_init(memory, layout.size - 1, &f.options, &client) ==
          VSR_ELIMIT);
    CHECK(client == NULL);
    client = (void *)&f;
    f.options.backoff_ns = 0;
    CHECK(vsr_client_init(memory, layout.size, &f.options, &client) ==
          VSR_EINVAL);
    CHECK(client == NULL);
    f.options.backoff_ns = BACKOFF;
    {
        /* An out pointer inside the arena is never written. */
        struct vsr_client **inside = (void *)(memory + 16);
        *inside = (void *)&f;
        CHECK(vsr_client_init(memory, layout.size, &f.options, inside) ==
              VSR_EINVAL);
        CHECK(*inside == (void *)&f);
    }
    {
        /* Options or a seed inside the arena are rejected. */
        struct vsr_client_options *inside = (void *)(memory + 64);
        *inside = f.options;
        CHECK(vsr_client_init(memory, layout.size, inside, &client) ==
              VSR_EINVAL);
        struct vsr_member *members = (void *)(memory + 64);
        memcpy(members, f.members, sizeof(f.members));
        f.seed.members = members;
        CHECK(vsr_client_init(memory, layout.size, &f.options, &client) ==
              VSR_EINVAL);
        CHECK(client == NULL);
        f.seed.members = f.members;
    }
    CHECK(vsr_client_init(memory, 2 * layout.size, &f.options, &client) ==
          VSR_OK);
    CHECK(client == (void *)memory);
    /* The seed is a copy: later changes to the caller's do not leak in. */
    f.members[0].id = 99;
    f.seed.epoch = 7;
    struct vsr_client_status status = client_status(client);
    CHECK(status.epoch == 0 && status.primary == VSR_NO_REPLICA);
    CHECK(status.min_op == 0 && status.lanes == 0 && status.busy == 0);
    CHECK(status.membership != NULL && status.membership != &f.seed);
    CHECK(status.membership->count == 3 && status.membership->faults == 1);
    CHECK(status.membership->members[0].id == 1);
    CHECK(status.membership->members[2].id == 3);
    CHECK(vsr_client_deadline(client) == VSR_NO_DEADLINE);
    free(memory);

    /* Without a seed nothing is known. */
    setup_options(&f, 1, 1);
    f.options.seed = NULL;
    create(&f);
    status = client_status(f.client);
    CHECK(status.membership == NULL && status.epoch == 0);
    teardown(&f);
}

/* Open lanes until ELIMIT; close only IDLE lanes; closed slots are reused. */
static void lane_lifecycle(void)
{
    struct fixture f;
    uint32_t lane = 7;
    setup(&f, 3, 3);
    struct vsr_client *c = f.client;
    CHECK(vsr_client_open(c, (struct vsr_id){0, 0}, 1, &lane) == VSR_EINVAL);
    CHECK(vsr_client_open(c, incarnation(1), 0, &lane) == VSR_EINVAL);
    CHECK(vsr_client_open(c, incarnation(1), UINT64_MAX, &lane) == VSR_EINVAL);
    CHECK(vsr_client_open(c, incarnation(1), 1, NULL) == VSR_EINVAL);
    CHECK(vsr_client_open(NULL, incarnation(1), 1, &lane) == VSR_EINVAL);
    CHECK(lane == 7);
    CHECK(open_lane(c, 1, 1) == 0);
    /* The same incarnation cannot be open twice. */
    CHECK(vsr_client_open(c, incarnation(1), 5, &lane) == VSR_EINVAL);
    CHECK(open_lane(c, 2, 10) == 1);
    CHECK(open_lane(c, 3, 20) == 2);
    CHECK(vsr_client_open(c, incarnation(4), 1, &lane) == VSR_ELIMIT);
    CHECK(client_status(c).lanes == 3);

    struct vsr_client_lane_status s = lane_status(c, 1);
    CHECK(s.state == VSR_CLIENT_LANE_IDLE && s.next_number == 10);
    CHECK(s.incarnation.hi == 0x1234 && s.incarnation.lo == 2);
    CHECK(s.pending_number == 0 && s.deadline_ns == VSR_NO_DEADLINE);
    CHECK(s.replica == VSR_NO_REPLICA && s.attempts == 0);

    CHECK(vsr_client_close(c, 3) == VSR_EINVAL);
    CHECK(vsr_client_close(NULL, 0) == VSR_EINVAL);
    (void)begin(c, 1, 0);
    CHECK(vsr_client_close(c, 1) == VSR_EBUSY);
    CHECK(vsr_client_close(c, 0) == VSR_OK);
    CHECK(vsr_client_close(c, 0) == VSR_EINVAL);
    CHECK(lane_status(c, 0).state == VSR_CLIENT_LANE_CLOSED);
    CHECK(lane_status(c, 99).state == VSR_CLIENT_LANE_CLOSED);
    CHECK(client_status(c).lanes == 2 && client_status(c).busy == 1);
    CHECK(open_lane(c, 5, 1) == 0);
    teardown(&f);
}

/* begin fills the attempt: identity, routing epoch, type, body, target,
 * deadline, lane and attempt 1; EBUSY while not IDLE; EINVAL otherwise. */
static void begin_attempt(void)
{
    struct fixture f;
    struct vsr_client_attempt a;
    setup(&f, 2, 4);
    struct vsr_client *c = f.client;
    uint32_t lane = open_lane(c, 1, 41);

    CHECK(vsr_client_begin(c, 2, VSR_REQUEST_COMMAND, &empty_command, 0, &a) ==
          VSR_EINVAL);
    CHECK(vsr_client_begin(c, lane, VSR_REQUEST_NOOP, NULL, 0, &a) ==
          VSR_EINVAL);
    CHECK(vsr_client_begin(c, lane, 99, &empty_command, 0, &a) == VSR_EINVAL);
    CHECK(vsr_client_begin(c, lane, VSR_REQUEST_COMMAND, NULL, 0, &a) ==
          VSR_EINVAL);
    CHECK(vsr_client_begin(c, lane, VSR_REQUEST_COMMAND, &empty_command,
                           VSR_NO_DEADLINE, &a) == VSR_EINVAL);
    CHECK(vsr_client_begin(c, lane, VSR_REQUEST_COMMAND, &empty_command, 0,
                           NULL) == VSR_EINVAL);
    {
        struct vsr_blob broken = {NULL, 4, 1, 0};
        CHECK(vsr_client_begin(c, lane, VSR_REQUEST_COMMAND, &broken, 0, &a) ==
              VSR_EINVAL);
        struct vsr_membership bad = {0, f.members, 3, 2};
        CHECK(vsr_client_begin(c, lane, VSR_REQUEST_RECONFIGURE, &bad, 0, &a) ==
              VSR_EINVAL);
    }
    CHECK(lane_status(c, lane).state == VSR_CLIENT_LANE_IDLE);
    CHECK(lane_status(c, lane).next_number == 41);

    static const char payload[] = "set x 1";
    struct vsr_span span = {payload, sizeof(payload)};
    struct vsr_blob command = {&span, sizeof(payload), 1, 0};
    CHECK(vsr_client_begin(c, lane, VSR_REQUEST_COMMAND, &command, 1000, &a) ==
          VSR_OK);
    CHECK(a.request.id.client.hi == 0x1234 && a.request.id.client.lo == 1);
    CHECK(a.request.id.number == 41 && a.request.epoch == 0);
    CHECK(a.request.type == VSR_REQUEST_COMMAND && a.request.body == &command);
    CHECK(a.request.reserved == 0);
    /* The primary is unknown: the first member in ID order. */
    CHECK(a.replica == 1);
    CHECK(a.deadline_ns == 1000 + TIMEOUT && a.lane == lane && a.attempt == 1);
    struct vsr_client_lane_status s = lane_status(c, lane);
    CHECK(s.state == VSR_CLIENT_LANE_PENDING && s.pending_number == 41);
    CHECK(s.next_number == 42 && s.deadline_ns == 1000 + TIMEOUT);
    CHECK(s.replica == 1 && s.attempts == 1 && s.busy_streak == 0);
    CHECK(vsr_client_begin(c, lane, VSR_REQUEST_COMMAND, &command, 1000, &a) ==
          VSR_EBUSY);
    CHECK(vsr_client_deadline(c) == 1000 + TIMEOUT);

    /* Control requests carry their own bodies. */
    uint32_t other = open_lane(c, 2, 1);
    struct vsr_member next[2] = {{1, VSR_MEMBER_FULL, 0},
                                 {2, VSR_MEMBER_FULL, 0}};
    struct vsr_membership proposal = {1, next, 2, 0};
    CHECK(vsr_client_begin(c, other, VSR_REQUEST_RECONFIGURE, &proposal, 5,
                           &a) == VSR_OK);
    CHECK(a.request.type == VSR_REQUEST_RECONFIGURE &&
          a.request.body == &proposal);
    /* The next target after the other lane's previous one, from member 1. */
    CHECK(a.replica == 1);
    teardown(&f);

    setup(&f, 1, 3);
    lane = open_lane(f.client, 1, 1);
    struct vsr_check_epoch check = {0};
    CHECK(vsr_client_begin(f.client, lane, VSR_REQUEST_CHECK_EPOCH, &check, 0,
                           &a) == VSR_OK);
    teardown(&f);
}

/* OK with EXECUTED is DONE and raises min_op; stale and mismatched replies
 * are IGNORE; OK without EXECUTED is malformed. */
static void reply_done(void)
{
    struct fixture f;
    struct vsr_client_outcome o;
    setup(&f, 2, 3);
    struct vsr_client *c = f.client;
    uint32_t lane = open_lane(c, 1, 1);
    struct vsr_client_attempt a = begin(c, lane, 0);
    struct vsr_reply r = answer(&a, VSR_REPLY_OK);

    /* Malformed replies. */
    CHECK(vsr_client_reply(c, lane, NULL, 0, &o) == VSR_EINVAL);
    CHECK(vsr_client_reply(c, lane, &r, 0, NULL) == VSR_EINVAL);
    CHECK(vsr_client_reply(c, 1, &r, 0, &o) == VSR_EINVAL);
    CHECK(o.action == VSR_CLIENT_IGNORE);
    r.flags = 0;
    CHECK(vsr_client_reply(c, lane, &r, 0, &o) == VSR_EINVAL);
    r.flags = 2 | VSR_REPLY_EXECUTED;
    CHECK(vsr_client_reply(c, lane, &r, 0, &o) == VSR_EINVAL);
    r.flags = VSR_REPLY_EXECUTED;
    r.status = 99;
    CHECK(vsr_client_reply(c, lane, &r, 0, &o) == VSR_EINVAL);
    r.status = VSR_REPLY_OK;
    CHECK(lane_status(c, lane).state == VSR_CLIENT_LANE_PENDING);

    /* Mismatched identities. */
    struct vsr_reply wrong = r;
    wrong.request.number = 2;
    CHECK(reply(c, lane, &wrong, 1, &o) == VSR_CLIENT_IGNORE);
    wrong = r;
    wrong.request.client.lo = 9;
    CHECK(reply(c, lane, &wrong, 1, &o) == VSR_CLIENT_IGNORE);
    /* Belongs elsewhere. */
    wrong = r;
    wrong.flags = 0;
    wrong.status = VSR_REPLY_TIMEOUT;
    CHECK(reply(c, lane, &wrong, 1, &o) == VSR_CLIENT_IGNORE);
    wrong.status = VSR_REPLY_CLIENT_STATE;
    CHECK(reply(c, lane, &wrong, 1, &o) == VSR_CLIENT_IGNORE);
    wrong.status = VSR_REPLY_CLIENT_UNKNOWN;
    CHECK(reply(c, lane, &wrong, 1, &o) == VSR_CLIENT_IGNORE);
    CHECK(lane_status(c, lane).state == VSR_CLIENT_LANE_PENDING);

    r.op = 17;
    r.view = 4;
    r.primary = 2;
    CHECK(reply(c, lane, &r, 2, &o) == VSR_CLIENT_DONE);
    CHECK(o.status == VSR_REPLY_OK && o.deadline_ns == VSR_NO_DEADLINE);
    struct vsr_client_lane_status s = lane_status(c, lane);
    CHECK(s.state == VSR_CLIENT_LANE_IDLE && s.pending_number == 0);
    CHECK(s.next_number == 2 && s.attempts == 0);
    CHECK(vsr_client_min_op(c) == 17 && client_status(c).min_op == 17);
    /* The reply's topology is adopted. */
    CHECK(client_status(c).primary == 2);
    /* A duplicate after DONE, or for an IDLE lane, is IGNORE. */
    CHECK(reply(c, lane, &r, 3, &o) == VSR_CLIENT_IGNORE);
    CHECK(vsr_client_deadline(c) == VSR_NO_DEADLINE);

    /* The next request goes to the primary; a smaller op never lowers
     * min_op. */
    a = begin(c, lane, 10);
    CHECK(a.request.id.number == 2 && a.replica == 2);
    r = answer(&a, VSR_REPLY_OK);
    r.op = 5;
    r.view = 4;
    r.primary = 2;
    CHECK(reply(c, lane, &r, 11, &o) == VSR_CLIENT_DONE);
    CHECK(vsr_client_min_op(c) == 17);
    teardown(&f);
}

/* NOT_PRIMARY records the primary and retries there with the same identity;
 * a redirect to where the attempt already went is IGNORE; one naming no
 * primary waits and then tries the next member; an older view never
 * overrides a newer one. */
static void not_primary(void)
{
    struct fixture f;
    struct vsr_client_outcome o;
    setup(&f, 1, 3);
    struct vsr_client *c = f.client;
    uint32_t lane = open_lane(c, 1, 1);
    struct vsr_client_attempt first = begin(c, lane, 0);
    CHECK(first.replica == 1);

    struct vsr_reply r = answer(&first, VSR_REPLY_NOT_PRIMARY);
    r.view = 2;
    r.primary = 3;
    r.membership = &f.seed;
    CHECK(reply(c, lane, &r, 5, &o) == VSR_CLIENT_RETRY);
    CHECK(o.attempt.replica == 3 && o.attempt.attempt == 2);
    CHECK(same_request(&o.attempt.request, &first.request));
    CHECK(o.attempt.request.epoch == first.request.epoch);
    CHECK(o.attempt.deadline_ns == 5 + TIMEOUT && o.attempt.lane == lane);
    CHECK(client_status(c).primary == 3);
    CHECK(lane_status(c, lane).replica == 3);
    CHECK(lane_status(c, lane).attempts == 2);

    /* A duplicate redirect to the current target: IGNORE, nothing changes. */
    CHECK(reply(c, lane, &r, 6, &o) == VSR_CLIENT_IGNORE);
    CHECK(lane_status(c, lane).attempts == 2);
    CHECK(lane_status(c, lane).deadline_ns == 5 + TIMEOUT);

    /* An older view naming another primary is not adopted. */
    struct vsr_reply old = r;
    old.view = 1;
    old.primary = 2;
    CHECK(reply(c, lane, &old, 7, &o) == VSR_CLIENT_IGNORE);
    CHECK(client_status(c).primary == 3);

    /* A newer view without a primary: WAIT, then the next member. */
    r.view = 3;
    r.primary = VSR_NO_REPLICA;
    CHECK(reply(c, lane, &r, 8, &o) == VSR_CLIENT_WAIT);
    CHECK(o.deadline_ns == 8 + BACKOFF);
    CHECK(client_status(c).primary == VSR_NO_REPLICA);
    CHECK(lane_status(c, lane).state == VSR_CLIENT_LANE_WAITING);
    CHECK(vsr_client_deadline(c) == 8 + BACKOFF);
    /* A WAITING lane does not follow redirects. */
    r.primary = 2;
    CHECK(reply(c, lane, &r, 9, &o) == VSR_CLIENT_IGNORE);
    CHECK(vsr_client_time(c, 8 + BACKOFF - 1, &o) == 0);
    CHECK(vsr_client_time(c, 8 + BACKOFF, &o) == 1);
    CHECK(o.action == VSR_CLIENT_RETRY && o.lane == lane);
    CHECK(o.attempt.replica == 1); /* After 3, wrapping in ID order. */
    CHECK(same_request(&o.attempt.request, &first.request));
    CHECK(o.attempt.attempt == 3);

    /* Member 1 names 2 in view 3: go there. */
    CHECK(reply(c, lane, &r, 20, &o) == VSR_CLIENT_RETRY);
    CHECK(o.attempt.replica == 2 && client_status(c).primary == 2);
    teardown(&f);
}

/* NEW_EPOCH adopts the returned membership and epoch and retries with the
 * new routing epoch; a lagging replica's older epoch is never adopted; a
 * membership over capacity is not adopted and reports ELIMIT. */
static void new_epoch(void)
{
    struct fixture f;
    struct vsr_client_outcome o;
    setup(&f, 2, 3);
    struct vsr_client *c = f.client;
    uint32_t lane = open_lane(c, 1, 1);
    struct vsr_client_attempt first = begin(c, lane, 0);

    struct vsr_member next[3] = {{2, VSR_MEMBER_FULL, 0},
                                 {3, VSR_MEMBER_FULL, 0},
                                 {4, VSR_MEMBER_FULL, 0}};
    struct vsr_membership epoch1 = {1, next, 3, 1};
    struct vsr_reply r = answer(&first, VSR_REPLY_NEW_EPOCH);
    r.membership = &epoch1;
    r.view = 0;
    r.primary = 4;
    CHECK(reply(c, lane, &r, 1, &o) == VSR_CLIENT_RETRY);
    CHECK(o.attempt.request.epoch == 1 && o.attempt.replica == 4);
    CHECK(same_request(&o.attempt.request, &first.request));
    struct vsr_client_status s = client_status(c);
    CHECK(s.epoch == 1 && s.primary == 4 && s.membership != NULL);
    CHECK(s.membership != &epoch1 && s.membership->epoch == 1);
    CHECK(s.membership->count == 3 && s.membership->members[0].id == 2);
    CHECK(s.membership->members[2].id == 4);

    /* The retried attempt meets a replica still in epoch 0: it cannot be the
     * primary of epoch 1, so the client backs off and forgets it. */
    r.membership = &f.seed;
    r.primary = 1;
    CHECK(reply(c, lane, &r, 2, &o) == VSR_CLIENT_WAIT);
    CHECK(o.deadline_ns == 2 + BACKOFF);
    s = client_status(c);
    CHECK(s.epoch == 1 && s.membership->epoch == 1);
    CHECK(s.primary == VSR_NO_REPLICA);
    CHECK(vsr_client_time(c, 2 + BACKOFF, &o) == 1);
    CHECK(o.attempt.replica == 2); /* After 4, wrapping to 2. */
    CHECK(o.attempt.request.epoch == 1);

    /* A NEW_EPOCH carrying a membership over capacity: the epoch and primary
     * are adopted, the membership is not, and the call reports ELIMIT. */
    struct vsr_member big[4] = {{2, VSR_MEMBER_FULL, 0},
                                {3, VSR_MEMBER_FULL, 0},
                                {4, VSR_MEMBER_FULL, 0},
                                {5, VSR_MEMBER_FULL, 0}};
    struct vsr_membership epoch2 = {2, big, 4, 1};
    r.membership = &epoch2;
    r.primary = 5;
    CHECK(vsr_client_reply(c, lane, &r, 3, &o) == VSR_ELIMIT);
    CHECK(o.action == VSR_CLIENT_RETRY && o.attempt.request.epoch == 2);
    CHECK(o.attempt.replica == 5);
    s = client_status(c);
    CHECK(s.epoch == 2 && s.primary == 5 && s.membership->epoch == 1);

    /* A malformed membership is EINVAL and changes nothing. */
    struct vsr_membership broken = {3, big, 4, 2};
    r.membership = &broken;
    CHECK(vsr_client_reply(c, lane, &r, 4, &o) == VSR_EINVAL);
    CHECK(client_status(c).epoch == 2);
    teardown(&f);
}

/* BUSY waits backoff_ns doubled per consecutive BUSY, capped at
 * backoff_max_ns; other replies reset the streak. */
static void busy_backoff(void)
{
    struct fixture f;
    struct vsr_client_outcome o;
    setup(&f, 1, 3);
    struct vsr_client *c = f.client;
    uint32_t lane = open_lane(c, 1, 1);
    struct vsr_client_attempt a = begin(c, lane, 0);
    const uint64_t expected[] = {10, 20, 40, 50, 50, 50};
    uint64_t now = 0;
    for (uint32_t i = 0; i < sizeof(expected) / sizeof(expected[0]); i++) {
        struct vsr_reply r = answer(&a, VSR_REPLY_BUSY);
        CHECK(reply(c, lane, &r, now, &o) == VSR_CLIENT_WAIT);
        CHECK(o.deadline_ns == now + expected[i]);
        struct vsr_client_lane_status s = lane_status(c, lane);
        CHECK(s.busy_streak == i + 1 && s.state == VSR_CLIENT_LANE_WAITING);
        /* A second BUSY while waiting is a duplicate. */
        CHECK(reply(c, lane, &r, now, &o) == VSR_CLIENT_IGNORE);
        CHECK(vsr_client_deadline(c) == now + expected[i]);
        CHECK(vsr_client_time(c, now + expected[i] - 1, &o) == 0);
        now += expected[i];
        CHECK(vsr_client_time(c, now, &o) == 1);
        CHECK(o.action == VSR_CLIENT_RETRY && o.attempt.attempt == i + 2);
        CHECK(same_request(&o.attempt.request, &a.request));
        CHECK(vsr_client_time(c, now, &o) == 0);
    }
    struct vsr_reply r = answer(&a, VSR_REPLY_NOT_PRIMARY);
    r.primary = 2;
    r.view = 1;
    CHECK(reply(c, lane, &r, now, &o) == VSR_CLIENT_RETRY);
    CHECK(lane_status(c, lane).busy_streak == 0);
    r = answer(&a, VSR_REPLY_BUSY);
    CHECK(reply(c, lane, &r, now, &o) == VSR_CLIENT_WAIT);
    CHECK(o.deadline_ns == now + BACKOFF);

    /* A WAITING lane still accepts the final answer. */
    r = answer(&a, VSR_REPLY_OK);
    CHECK(reply(c, lane, &r, now, &o) == VSR_CLIENT_DONE);
    CHECK(lane_status(c, lane).busy_streak == 0);
    CHECK(vsr_client_deadline(c) == VSR_NO_DEADLINE);
    teardown(&f);
}

/* INVALID, LIMIT and STALE_REQUEST are FAILED with that status; the lane is
 * IDLE again and its counter continues. */
static void failed(void)
{
    struct fixture f;
    struct vsr_client_outcome o;
    setup(&f, 1, 3);
    struct vsr_client *c = f.client;
    uint32_t lane = open_lane(c, 1, 7);
    const uint32_t statuses[] = {VSR_REPLY_INVALID, VSR_REPLY_LIMIT,
                                 VSR_REPLY_STALE_REQUEST};
    for (uint32_t i = 0; i < 3; i++) {
        struct vsr_client_attempt a = begin(c, lane, i);
        CHECK(a.request.id.number == 7 + i);
        struct vsr_reply r = answer(&a, statuses[i]);
        CHECK(reply(c, lane, &r, i, &o) == VSR_CLIENT_FAILED);
        CHECK(o.status == statuses[i]);
        struct vsr_client_lane_status s = lane_status(c, lane);
        CHECK(s.state == VSR_CLIENT_LANE_IDLE && s.pending_number == 0);
        CHECK(s.next_number == 8 + i);
        CHECK(vsr_client_min_op(c) == 0);
    }
    /* After STALE_REQUEST the caller retires the incarnation. */
    CHECK(vsr_client_close(c, lane) == VSR_OK);
    teardown(&f);
}

/* An expired attempt retries at the primary if known; one that expires at
 * the primary forgets it, and retries walk the members in ID order until a
 * reply names the primary again. */
static void time_expiry(void)
{
    struct fixture f;
    struct vsr_client_outcome o;
    setup(&f, 2, 3);
    struct vsr_client *c = f.client;
    CHECK(vsr_client_time(NULL, 0, &o) == VSR_EINVAL);
    CHECK(vsr_client_time(c, 0, NULL) == VSR_EINVAL);
    CHECK(vsr_client_time(c, VSR_NO_DEADLINE, &o) == VSR_EINVAL);
    CHECK(vsr_client_time(c, 0, &o) == 0);
    CHECK(vsr_client_learn(c, NULL, 2) == VSR_OK);
    CHECK(client_status(c).primary == 2);

    uint32_t lane = open_lane(c, 1, 1);
    struct vsr_client_attempt first = begin(c, lane, 0);
    CHECK(first.replica == 2 && first.deadline_ns == TIMEOUT);
    CHECK(vsr_client_time(c, TIMEOUT - 1, &o) == 0);
    CHECK(vsr_client_time(c, TIMEOUT, &o) == 1);
    CHECK(o.action == VSR_CLIENT_RETRY && o.attempt.replica == 3);
    CHECK(o.attempt.deadline_ns == 2 * TIMEOUT && o.attempt.attempt == 2);
    CHECK(same_request(&o.attempt.request, &first.request));
    CHECK(client_status(c).primary == VSR_NO_REPLICA);
    CHECK(vsr_client_time(c, TIMEOUT, &o) == 0);
    CHECK(vsr_client_time(c, 2 * TIMEOUT, &o) == 1);
    CHECK(o.attempt.replica == 1); /* Wraps around. */
    CHECK(vsr_client_time(c, 3 * TIMEOUT, &o) == 1);
    CHECK(o.attempt.replica == 2);
    CHECK(vsr_client_time(c, 4 * TIMEOUT, &o) == 1);
    CHECK(o.attempt.replica == 3);
    /* A live backup names the primary of a newer view. */
    struct vsr_reply r = answer(&first, VSR_REPLY_NOT_PRIMARY);
    r.view = 1;
    r.primary = 1;
    CHECK(reply(c, lane, &r, 4 * TIMEOUT + 1, &o) == VSR_CLIENT_RETRY);
    CHECK(o.attempt.replica == 1 && client_status(c).primary == 1);

    /* Two lanes: the earliest deadline expires first; a backoff ending
     * retries at the primary without forgetting it. */
    uint32_t other = open_lane(c, 2, 1);
    struct vsr_client_attempt second = begin(c, other, 4 * TIMEOUT + 2);
    CHECK(second.replica == 1);
    r = answer(&second, VSR_REPLY_BUSY);
    CHECK(reply(c, other, &r, 4 * TIMEOUT + 3, &o) == VSR_CLIENT_WAIT);
    CHECK(vsr_client_deadline(c) == 4 * TIMEOUT + 3 + BACKOFF);
    uint64_t now = 5 * TIMEOUT + 1;
    CHECK(vsr_client_time(c, now, &o) == 1);
    CHECK(o.lane == other && o.attempt.replica == 1);
    CHECK(client_status(c).primary == 1);
    CHECK(vsr_client_time(c, now, &o) == 1);
    CHECK(o.lane == lane && o.attempt.replica == 2);
    CHECK(client_status(c).primary == VSR_NO_REPLICA);
    CHECK(vsr_client_time(c, now, &o) == 0);
    CHECK(vsr_client_deadline(c) == now + TIMEOUT);
    teardown(&f);
}

/* Imported pending lanes are DETACHED: resume reattaches the body; query and
 * queried resolve the outcome from a node's local knowledge. */
static void detached(void)
{
    struct fixture f;
    struct fixture g;
    struct vsr_client_outcome o;
    struct vsr_client_attempt a;
    unsigned char image[1024];
    size_t written;
    setup(&f, 4, 3);
    uint32_t lanes[4];
    struct vsr_client_attempt firsts[4];
    for (uint32_t i = 0; i < 4; i++) {
        lanes[i] = open_lane(f.client, 10 + i, 100);
        firsts[i] = begin(f.client, lanes[i], 0);
    }
    CHECK(vsr_client_export(f.client, image, sizeof(image), &written) ==
          VSR_OK);
    setup(&g, 4, 3);
    struct vsr_client *c = g.client;
    CHECK(vsr_client_import(c, image, written) == VSR_OK);
    for (uint32_t i = 0; i < 4; i++) {
        struct vsr_client_lane_status s = lane_status(c, lanes[i]);
        CHECK(s.state == VSR_CLIENT_LANE_DETACHED);
        CHECK(s.pending_number == 100 && s.next_number == 101);
        CHECK(s.deadline_ns == VSR_NO_DEADLINE);
    }
    CHECK(vsr_client_deadline(c) == VSR_NO_DEADLINE);
    CHECK(client_status(c).busy == 4);
    CHECK(vsr_client_begin(c, lanes[0], VSR_REQUEST_COMMAND, &empty_command, 0,
                           &a) == VSR_EBUSY);
    CHECK(vsr_client_close(c, lanes[0]) == VSR_EBUSY);
    /* Replies are for attached requests only. */
    struct vsr_reply r = answer(&firsts[0], VSR_REPLY_OK);
    CHECK(reply(c, lanes[0], &r, 0, &o) == VSR_CLIENT_IGNORE);

    /* Lane 0: resume with the identical body. */
    CHECK(vsr_client_resume(c, lanes[0], NULL, 0, &a) == VSR_EINVAL);
    CHECK(vsr_client_resume(c, lanes[0], &empty_command, 5, &a) == VSR_OK);
    CHECK(same_request(&a.request, &firsts[0].request));
    CHECK(a.attempt == 2 && a.deadline_ns == 5 + TIMEOUT);
    CHECK(lane_status(c, lanes[0]).state == VSR_CLIENT_LANE_PENDING);
    CHECK(vsr_client_resume(c, lanes[0], &empty_command, 5, &a) == VSR_EINVAL);
    struct vsr_id who;
    uint64_t replica;
    CHECK(vsr_client_query(c, lanes[0], &who, &replica) == VSR_EINVAL);
    r = answer(&a, VSR_REPLY_OK);
    r.op = 9;
    CHECK(reply(c, lanes[0], &r, 6, &o) == VSR_CLIENT_DONE);
    CHECK(vsr_client_resume(c, lanes[0], &empty_command, 5, &a) == VSR_EINVAL);

    /* Lane 1: the query reports the pending number as executed. */
    CHECK(vsr_client_query(c, lanes[1], NULL, &replica) == VSR_EINVAL);
    CHECK(vsr_client_query(c, lanes[1], &who, &replica) == VSR_OK);
    CHECK(who.hi == 0x1234 && who.lo == 11 && replica == 1);
    CHECK(lane_status(c, lanes[1]).state == VSR_CLIENT_LANE_QUERYING);
    CHECK(lane_status(c, lanes[1]).replica == 1);
    struct vsr_reply q;
    memset(&q, 0, sizeof(q));
    q.request.client = who;
    q.request.number = 100;
    q.op = 30;
    q.status = VSR_REPLY_CLIENT_STATE;
    q.flags = VSR_REPLY_EXECUTED;
    CHECK(vsr_client_queried(c, lanes[1], &q, 7, NULL) == VSR_EINVAL);
    /* Another incarnation's state answers nothing. */
    struct vsr_reply foreign = q;
    foreign.request.client.lo = 99;
    CHECK(vsr_client_queried(c, lanes[1], &foreign, 7, &o) == VSR_OK);
    CHECK(o.action == VSR_CLIENT_IGNORE);
    CHECK(lane_status(c, lanes[1]).state == VSR_CLIENT_LANE_QUERYING);
    CHECK(vsr_client_queried(c, lanes[1], &q, 7, &o) == VSR_OK);
    CHECK(o.action == VSR_CLIENT_DONE && o.lane == lanes[1]);
    CHECK(o.status == VSR_REPLY_CLIENT_STATE);
    CHECK(vsr_client_min_op(c) == 30);
    CHECK(lane_status(c, lanes[1]).state == VSR_CLIENT_LANE_IDLE);
    CHECK(lane_status(c, lanes[1]).next_number == 101);
    /* Once IDLE, a duplicate answer is IGNORE. */
    CHECK(vsr_client_queried(c, lanes[1], &q, 7, &o) == VSR_OK);
    CHECK(o.action == VSR_CLIENT_IGNORE);

    /* Lane 2: a later number is known, so this one will never execute. */
    CHECK(vsr_client_query(c, lanes[2], &who, &replica) == VSR_OK);
    q.request.client = who;
    q.request.number = 101;
    q.flags = 0;
    CHECK(vsr_client_queried(c, lanes[2], &q, 8, &o) == VSR_OK);
    CHECK(o.action == VSR_CLIENT_FAILED && o.status == VSR_REPLY_STALE_REQUEST);
    CHECK(lane_status(c, lanes[2]).state == VSR_CLIENT_LANE_IDLE);

    /* Lane 3: local knowledge that cannot prove anything. */
    CHECK(vsr_client_query(c, lanes[3], &who, &replica) == VSR_OK);
    q.request.client = who;
    q.request.number = 100;
    q.flags = 0; /* Known but not executed. */
    CHECK(vsr_client_queried(c, lanes[3], &q, 9, &o) == VSR_OK);
    CHECK(o.action == VSR_CLIENT_IGNORE);
    CHECK(lane_status(c, lanes[3]).state == VSR_CLIENT_LANE_DETACHED);
    CHECK(vsr_client_query(c, lanes[3], &who, &replica) == VSR_OK);
    q.request.number = 99;
    CHECK(vsr_client_queried(c, lanes[3], &q, 9, &o) == VSR_OK);
    CHECK(o.action == VSR_CLIENT_IGNORE);
    CHECK(lane_status(c, lanes[3]).state == VSR_CLIENT_LANE_DETACHED);
    CHECK(vsr_client_query(c, lanes[3], &who, &replica) == VSR_OK);
    memset(&q, 0, sizeof(q));
    q.request.client = who;
    q.status = VSR_REPLY_CLIENT_UNKNOWN;
    CHECK(vsr_client_queried(c, lanes[3], &q, 9, &o) == VSR_OK);
    CHECK(o.action == VSR_CLIENT_IGNORE);
    CHECK(lane_status(c, lanes[3]).state == VSR_CLIENT_LANE_DETACHED);
    /* Resume after a query, from QUERYING too. */
    CHECK(vsr_client_query(c, lanes[3], &who, &replica) == VSR_OK);
    CHECK(vsr_client_resume(c, lanes[3], &empty_command, 10, &a) == VSR_OK);
    CHECK(same_request(&a.request, &firsts[3].request));
    CHECK(vsr_client_query(c, 3, &who, &replica) == VSR_EINVAL);
    teardown(&f);
    teardown(&g);
}

/* Hints: a newer membership is adopted with its primary, an older one is
 * ignored, and a primary never overrides one a reply reported. */
static void learn(void)
{
    struct fixture f;
    struct vsr_client_outcome o;
    setup(&f, 1, 3);
    struct vsr_client *c = f.client;
    struct vsr_member next[3] = {{4, VSR_MEMBER_FULL, 0},
                                 {5, VSR_MEMBER_FULL, 0},
                                 {6, VSR_MEMBER_WITNESS, 0}};
    struct vsr_membership epoch2 = {2, next, 3, 1};
    CHECK(vsr_client_learn(NULL, &epoch2, 4) == VSR_EINVAL);
    CHECK(vsr_client_learn(c, &epoch2, 6) == VSR_EINVAL); /* A witness. */
    CHECK(vsr_client_learn(c, &epoch2, 1) == VSR_EINVAL); /* Not a member. */
    struct vsr_membership broken = {2, next, 3, 2};
    CHECK(vsr_client_learn(c, &broken, 4) == VSR_EINVAL);
    CHECK(vsr_client_learn(c, NULL, 9) == VSR_EINVAL);
    CHECK(client_status(c).epoch == 0);

    CHECK(vsr_client_learn(c, &epoch2, 5) == VSR_OK);
    struct vsr_client_status s = client_status(c);
    CHECK(s.epoch == 2 && s.primary == 5 && s.membership->epoch == 2);
    CHECK(s.membership->members[2].id == 6);
    /* Older knowledge is ignored. */
    CHECK(vsr_client_learn(c, &f.seed, 1) == VSR_OK);
    s = client_status(c);
    CHECK(s.epoch == 2 && s.primary == 5 && s.membership->members[0].id == 4);
    /* A same-epoch hint may move a hinted primary. */
    CHECK(vsr_client_learn(c, NULL, 4) == VSR_OK);
    CHECK(client_status(c).primary == 4);
    CHECK(vsr_client_learn(c, NULL, VSR_NO_REPLICA) == VSR_OK);
    CHECK(client_status(c).primary == 4);

    /* A reply reports 5; later hints for this epoch do not override it. */
    uint32_t lane = open_lane(c, 1, 1);
    struct vsr_client_attempt a = begin(c, lane, 0);
    CHECK(a.replica == 4 && a.request.epoch == 2);
    struct vsr_reply r = answer(&a, VSR_REPLY_NOT_PRIMARY);
    r.membership = &epoch2;
    r.primary = 5;
    r.view = 1;
    CHECK(reply(c, lane, &r, 1, &o) == VSR_CLIENT_RETRY);
    CHECK(vsr_client_learn(c, &epoch2, 4) == VSR_OK);
    CHECK(vsr_client_learn(c, NULL, 4) == VSR_OK);
    CHECK(client_status(c).primary == 5);

    /* A newer epoch through a hint replaces it, primary unknown. */
    struct vsr_membership epoch3 = {3, next, 3, 1};
    CHECK(vsr_client_learn(c, &epoch3, VSR_NO_REPLICA) == VSR_OK);
    s = client_status(c);
    CHECK(s.epoch == 3 && s.primary == VSR_NO_REPLICA);
    teardown(&f);

    /* Capacity. */
    struct vsr_member two[2] = {{1, VSR_MEMBER_FULL, 0},
                                {2, VSR_MEMBER_FULL, 0}};
    struct vsr_membership small = {0, two, 2, 0};
    setup_options(&f, 1, 2);
    f.options.seed = &small;
    create(&f);
    CHECK(vsr_client_learn(f.client, &epoch2, 4) == VSR_ELIMIT);
    CHECK(client_status(f.client).epoch == 0);
    teardown(&f);
}

/* LINEARIZABLE targets the primary; CAUSAL targets full members round-robin
 * in ID order and carries min_op; observe raises min_op. */
static void reads(void)
{
    struct fixture f;
    struct vsr_read_barrier barrier;
    uint64_t replica;
    setup_options(&f, 1, 4);
    f.options.seed = NULL;
    create(&f);
    struct vsr_client *c = f.client;
    CHECK(vsr_client_read(c, VSR_READ_LINEARIZABLE, 5, &barrier, &replica) ==
          VSR_OK);
    CHECK(replica == VSR_NO_REPLICA);
    CHECK(vsr_client_read(c, VSR_READ_CAUSAL, 5, &barrier, &replica) == VSR_OK);
    CHECK(replica == VSR_NO_REPLICA);
    CHECK(vsr_client_read(c, 7, 5, &barrier, &replica) == VSR_EINVAL);
    CHECK(vsr_client_read(c, VSR_READ_CAUSAL, 5, NULL, &replica) == VSR_EINVAL);
    CHECK(vsr_client_read(c, VSR_READ_CAUSAL, 5, &barrier, NULL) == VSR_EINVAL);

    struct vsr_member members[4] = {{1, VSR_MEMBER_FULL, 0},
                                    {2, VSR_MEMBER_WITNESS, 0},
                                    {3, VSR_MEMBER_FULL, 0},
                                    {4, VSR_MEMBER_FULL, 0}};
    struct vsr_membership group = {0, members, 4, 1};
    CHECK(vsr_client_learn(c, &group, 3) == VSR_OK);
    vsr_client_observe(c, 12);
    vsr_client_observe(c, 8);
    CHECK(vsr_client_min_op(c) == 12);
    CHECK(vsr_client_read(c, VSR_READ_LINEARIZABLE, VSR_NO_DEADLINE, &barrier,
                          &replica) == VSR_OK);
    CHECK(replica == 3 && barrier.consistency == VSR_READ_LINEARIZABLE);
    CHECK(barrier.deadline_ns == VSR_NO_DEADLINE && barrier.reserved == 0);
    const uint64_t expected[] = {1, 3, 4, 1, 3};
    for (size_t i = 0; i < sizeof(expected) / sizeof(expected[0]); i++) {
        CHECK(vsr_client_read(c, VSR_READ_CAUSAL, 100 + i, &barrier,
                              &replica) == VSR_OK);
        CHECK(replica == expected[i]);
        CHECK(barrier.min_op == 12 && barrier.deadline_ns == 100 + i);
        CHECK(barrier.consistency == VSR_READ_CAUSAL);
    }
    vsr_client_observe(c, 40);
    CHECK(vsr_client_read(c, VSR_READ_CAUSAL, 0, &barrier, &replica) == VSR_OK);
    CHECK(barrier.min_op == 40 && replica == 4);
    CHECK(vsr_client_min_op(NULL) == 0);
    teardown(&f);
}

/* Export and import round-trip lanes, counters, pending identities, the
 * topology and min_op; malformed, truncated, newer and oversized images are
 * rejected and leave the client unchanged. */
static void persistence(void)
{
    struct fixture f;
    struct fixture g;
    struct vsr_client_outcome o;
    unsigned char image[1024];
    unsigned char again[1024];
    size_t written = 0;
    setup(&f, 4, 3);
    struct vsr_client *c = f.client;
    uint32_t l0 = open_lane(c, 1, 5);
    uint32_t l1 = open_lane(c, 2, 50);
    uint32_t l2 = open_lane(c, 3, 500);
    uint32_t l3 = open_lane(c, 4, 9);
    CHECK(vsr_client_close(c, l0) == VSR_OK);
    struct vsr_client_attempt a1 = begin(c, l1, 0);
    struct vsr_client_attempt a2 = begin(c, l2, 0);
    struct vsr_reply r = answer(&a2, VSR_REPLY_BUSY);
    r.view = 3;
    r.primary = 2;
    r.membership = &f.seed;
    CHECK(reply(c, l2, &r, 1, &o) == VSR_CLIENT_WAIT);
    vsr_client_observe(c, 42);
    struct vsr_client_attempt a3 = begin(c, l3, 0);
    r = answer(&a3, VSR_REPLY_OK);
    CHECK(reply(c, l3, &r, 1, &o) == VSR_CLIENT_DONE);

    size_t size = vsr_client_export_size(c);
    CHECK(size == 72 + 3 * 48 + 3 * 16 + 4);
    CHECK(vsr_client_export(c, image, size - 1, &written) == VSR_ELIMIT);
    CHECK(written == size);
    CHECK(vsr_client_export(c, NULL, 0, &written) == VSR_ELIMIT);
    CHECK(vsr_client_export(c, image, sizeof(image), NULL) == VSR_EINVAL);
    CHECK(vsr_client_export(c, image, sizeof(image), &written) == VSR_OK);
    CHECK(written == size);

    setup(&g, 4, 3);
    CHECK(vsr_client_import(g.client, NULL, size) == VSR_EINVAL);
    CHECK(vsr_client_import(NULL, image, size) == VSR_EINVAL);
    CHECK(vsr_client_import(g.client, image, size - 1) == VSR_EINVAL);
    CHECK(vsr_client_import(g.client, image, size + 1) == VSR_EINVAL);
    CHECK(vsr_client_import(g.client, image, 10) == VSR_EINVAL);
    for (size_t i = 0; i < size; i += 7) {
        image[i] ^= 0x10;
        CHECK(vsr_client_import(g.client, image, size) == VSR_EINVAL);
        image[i] ^= 0x10;
    }
    /* A newer version, even well checksummed, is rejected. */
    image[4] = 2;
    reseal(image, size);
    CHECK(vsr_client_import(g.client, image, size) == VSR_EINVAL);
    image[4] = 1;
    /* A structurally wrong lane, well checksummed. */
    unsigned char saved = image[72 + 24];
    CHECK(saved == 51); /* next_number of the first lane record. */
    image[72 + 24] = 0;
    reseal(image, size);
    CHECK(vsr_client_import(g.client, image, size) == VSR_EINVAL);
    image[72 + 24] = saved;
    reseal(image, size);
    CHECK(client_status(g.client).lanes == 0);

    CHECK(vsr_client_import(g.client, image, size) == VSR_OK);
    CHECK(vsr_client_import(g.client, image, size) == VSR_EBUSY);
    struct vsr_client *d = g.client;
    CHECK(lane_status(d, l0).state == VSR_CLIENT_LANE_CLOSED);
    struct vsr_client_lane_status s = lane_status(d, l1);
    CHECK(s.state == VSR_CLIENT_LANE_DETACHED && s.pending_number == 50);
    CHECK(s.next_number == 51 && s.incarnation.lo == 2 && s.attempts == 1);
    s = lane_status(d, l2);
    CHECK(s.state == VSR_CLIENT_LANE_DETACHED && s.pending_number == 500);
    s = lane_status(d, l3);
    CHECK(s.state == VSR_CLIENT_LANE_IDLE && s.next_number == 10);
    struct vsr_client_status cs = client_status(c);
    struct vsr_client_status ds = client_status(d);
    CHECK(ds.epoch == cs.epoch && ds.primary == 2 && ds.min_op == 42);
    CHECK(ds.lanes == 3 && ds.busy == 2);
    CHECK(ds.membership != NULL && ds.membership->count == 3);
    CHECK(ds.membership->faults == 1 && ds.membership->members[1].id == 2);
    /* Re-export is byte-identical: DETACHED exports as pending. */
    CHECK(vsr_client_export(d, again, sizeof(again), &written) == VSR_OK);
    CHECK(written == size && memcmp(image, again, size) == 0);
    /* The restored client resumes where the old one left off. */
    struct vsr_client_attempt resumed;
    CHECK(vsr_client_resume(d, l1, &empty_command, 3, &resumed) == VSR_OK);
    CHECK(same_request(&resumed.request, &a1.request));
    CHECK(resumed.replica == 2 && resumed.attempt == 2);
    struct vsr_client_attempt fresh = begin(d, l3, 3);
    CHECK(fresh.request.id.number == 10);
    teardown(&g);

    /* Capacities that do not fit: ELIMIT. */
    setup(&g, 3, 3); /* Lane index 3 does not exist. */
    CHECK(vsr_client_import(g.client, image, size) == VSR_ELIMIT);
    CHECK(client_status(g.client).lanes == 0);
    teardown(&g);
    setup_options(&g, 4, 2);
    g.options.seed = NULL;
    create(&g);
    CHECK(vsr_client_import(g.client, image, size) == VSR_ELIMIT);
    CHECK(client_status(g.client).lanes == 0);
    CHECK(client_status(g.client).membership == NULL);
    teardown(&g);

    /* A client that already knows a newer epoch keeps it. */
    struct vsr_member next[3] = {{7, VSR_MEMBER_FULL, 0},
                                 {8, VSR_MEMBER_FULL, 0},
                                 {9, VSR_MEMBER_FULL, 0}};
    struct vsr_membership newer = {5, next, 3, 1};
    setup_options(&g, 4, 3);
    g.options.seed = &newer;
    create(&g);
    CHECK(vsr_client_import(g.client, image, size) == VSR_OK);
    ds = client_status(g.client);
    CHECK(ds.epoch == 5 && ds.membership->members[0].id == 7);
    CHECK(ds.primary == VSR_NO_REPLICA && ds.min_op == 42);
    teardown(&g);

    /* An empty client round-trips too. */
    setup_options(&g, 1, 1);
    g.options.seed = NULL;
    create(&g);
    CHECK(vsr_client_export_size(g.client) == 76);
    CHECK(vsr_client_export(g.client, again, sizeof(again), &written) ==
          VSR_OK);
    CHECK(vsr_client_import(g.client, again, written) == VSR_OK);
    teardown(&g);
    teardown(&f);
}

/* Lanes are independent: a redirect on one moves the shared topology but
 * neither the other's attempt nor its numbering. */
static void independent_lanes(void)
{
    struct fixture f;
    struct vsr_client_outcome o;
    setup(&f, 2, 3);
    struct vsr_client *c = f.client;
    uint32_t x = open_lane(c, 1, 1);
    uint32_t y = open_lane(c, 2, 1000);
    struct vsr_client_attempt ax = begin(c, x, 0);
    struct vsr_client_attempt ay = begin(c, y, 1);
    CHECK(ax.request.id.number == 1 && ay.request.id.number == 1000);
    CHECK(ax.replica == 1 && ay.replica == 1);
    CHECK(client_status(c).busy == 2);
    /* y's reply is not x's. */
    struct vsr_reply r = answer(&ay, VSR_REPLY_OK);
    CHECK(reply(c, x, &r, 2, &o) == VSR_CLIENT_IGNORE);
    r = answer(&ax, VSR_REPLY_NOT_PRIMARY);
    r.view = 1;
    r.primary = 2;
    CHECK(reply(c, x, &r, 2, &o) == VSR_CLIENT_RETRY);
    CHECK(o.attempt.replica == 2);
    struct vsr_client_lane_status s = lane_status(c, y);
    CHECK(s.state == VSR_CLIENT_LANE_PENDING && s.replica == 1);
    CHECK(s.attempts == 1 && s.deadline_ns == 1 + TIMEOUT);
    /* y's timeout follows the topology x learned. */
    CHECK(vsr_client_time(c, 1 + TIMEOUT, &o) == 1);
    CHECK(o.lane == y && o.attempt.replica == 2);
    CHECK(same_request(&o.attempt.request, &ay.request));
    r = answer(&ay, VSR_REPLY_OK);
    r.op = 3;
    CHECK(reply(c, y, &r, 3, &o) == VSR_CLIENT_DONE);
    CHECK(lane_status(c, x).state == VSR_CLIENT_LANE_PENDING);
    CHECK(client_status(c).busy == 1);
    ay = begin(c, y, 4);
    CHECK(ay.request.id.number == 1001);
    teardown(&f);
}

/* Randomized: many lanes driven through random replies, timeouts, restarts
 * and resolutions, checking the lane invariants after every step. */
#define RANDOM_LANES 6u
#define RANDOM_BODIES 3u

struct model {
    bool open;
    bool outstanding;
    struct vsr_request request; /* The first attempt of the pending one. */
    uint64_t last_number;
    uint64_t last_epoch;
    uint64_t serial;
};

struct world {
    struct test_random random;
    struct fixture fixture;
    struct model lanes[RANDOM_LANES];
    struct vsr_blob bodies[RANDOM_BODIES];
    struct vsr_member members[6];
    struct vsr_membership membership;
    uint64_t now;
    uint64_t min_op;
    uint64_t epoch;
    uint64_t serial;
    uint64_t view;
    uint64_t op;
};

static void check_attempt(struct world *w, const struct vsr_client_attempt *a)
{
    CHECK(a->lane < RANDOM_LANES);
    struct model *m = &w->lanes[a->lane];
    CHECK(m->open && m->outstanding);
    CHECK(same_request(&a->request, &m->request));
    CHECK(a->request.epoch >= m->last_epoch);
    CHECK(a->request.epoch == client_status(w->fixture.client).epoch);
    CHECK(a->deadline_ns > w->now || a->deadline_ns == VSR_NO_DEADLINE - 1);
    CHECK(a->attempt >= 1);
    m->last_epoch = a->request.epoch;
}

static void check_outcome(struct world *w, uint32_t lane,
                          const struct vsr_client_outcome *o)
{
    struct model *m = &w->lanes[lane];
    CHECK(o->lane == lane);
    switch (o->action) {
    case VSR_CLIENT_RETRY:
        CHECK(o->attempt.lane == lane);
        check_attempt(w, &o->attempt);
        break;
    case VSR_CLIENT_DONE:
    case VSR_CLIENT_FAILED:
        CHECK(m->outstanding);
        m->outstanding = false;
        break;
    case VSR_CLIENT_WAIT:
        CHECK(m->outstanding && o->deadline_ns >= w->now);
        break;
    default:
        CHECK(o->action == VSR_CLIENT_IGNORE);
        break;
    }
}

static void check_world(struct world *w)
{
    struct vsr_client *c = w->fixture.client;
    struct vsr_client_status cs = client_status(c);
    uint64_t deadline = VSR_NO_DEADLINE;
    uint32_t open = 0;
    uint32_t busy = 0;
    CHECK(cs.min_op >= w->min_op);
    CHECK(cs.epoch >= w->epoch);
    w->min_op = cs.min_op;
    w->epoch = cs.epoch;
    CHECK(cs.membership == NULL || cs.membership->epoch <= cs.epoch);
    for (uint32_t i = 0; i < RANDOM_LANES; i++) {
        struct model *m = &w->lanes[i];
        struct vsr_client_lane_status s = lane_status(c, i);
        if (!m->open) {
            CHECK(s.state == VSR_CLIENT_LANE_CLOSED);
            continue;
        }
        open++;
        CHECK(s.incarnation.lo == m->serial);
        if (!m->outstanding) {
            CHECK(s.state == VSR_CLIENT_LANE_IDLE && s.pending_number == 0);
            CHECK(s.next_number == m->last_number + 1);
            continue;
        }
        busy++;
        CHECK(s.state != VSR_CLIENT_LANE_IDLE &&
              s.state != VSR_CLIENT_LANE_CLOSED);
        CHECK(s.pending_number == m->request.id.number);
        CHECK(s.next_number == s.pending_number + 1);
        if (s.state == VSR_CLIENT_LANE_PENDING ||
            s.state == VSR_CLIENT_LANE_WAITING) {
            CHECK(s.deadline_ns != VSR_NO_DEADLINE);
            if (s.deadline_ns < deadline) {
                deadline = s.deadline_ns;
            }
        } else {
            CHECK(s.deadline_ns == VSR_NO_DEADLINE);
        }
    }
    CHECK(cs.lanes == open && cs.busy == busy);
    CHECK(vsr_client_deadline(c) == deadline);
}

static const struct vsr_membership *random_membership(struct world *w)
{
    uint32_t count = 0;
    uint32_t roll = test_random_bounded(&w->random, 4);
    uint64_t epoch = w->epoch;
    if (roll == 0) {
        return NULL;
    }
    if (roll == 1 && epoch > 0) {
        epoch--;
    } else if (roll == 3) {
        epoch++;
    }
    for (uint64_t id = 1; id <= 6; id++) {
        if (test_random_bounded(&w->random, 2) == 0 ||
            (count == 0 && id == 6)) {
            w->members[count++] = (struct vsr_member){id, VSR_MEMBER_FULL, 0};
        }
    }
    uint32_t faults = (count - 1) / 2;
    if (count >= 3 && test_random_bounded(&w->random, 3) == 0) {
        w->members[count - 1].role = VSR_MEMBER_WITNESS;
        faults = faults > (count - 2) / 2 ? (count - 2) / 2 : faults;
    }
    w->membership = (struct vsr_membership){epoch, w->members, count, faults};
    return &w->membership;
}

static void random_reply(struct world *w, uint32_t lane)
{
    struct vsr_client *c = w->fixture.client;
    struct model *m = &w->lanes[lane];
    struct vsr_client_lane_status s = lane_status(c, lane);
    struct vsr_client_outcome o;
    struct vsr_reply r;
    memset(&r, 0, sizeof(r));
    r.request.client = s.incarnation;
    r.request.number = m->outstanding ? m->request.id.number : m->last_number;
    uint32_t skew = test_random_bounded(&w->random, 8);
    if (skew == 0 && r.request.number > 1) {
        r.request.number--;
    } else if (skew == 1) {
        r.request.number++;
    }
    r.status = test_random_bounded(&w->random, VSR_REPLY_TIMEOUT + 1);
    if (r.status == VSR_REPLY_OK || (r.status == VSR_REPLY_CLIENT_STATE &&
                                     test_random_bounded(&w->random, 2) == 0)) {
        r.flags = VSR_REPLY_EXECUTED;
        r.op = ++w->op;
    }
    w->view += test_random_bounded(&w->random, 2);
    r.view = w->view;
    r.membership = random_membership(w);
    r.primary = test_random_bounded(&w->random, 7);
    int result = vsr_client_reply(c, lane, &r, w->now, &o);
    bool oversized = r.membership != NULL && r.membership->count > 5;
    CHECK(result == VSR_OK || (result == VSR_ELIMIT && oversized));
    bool matches = m->outstanding && r.request.number == m->request.id.number &&
                   (s.state == VSR_CLIENT_LANE_PENDING ||
                    s.state == VSR_CLIENT_LANE_WAITING);
    if (!matches) {
        CHECK(o.action == VSR_CLIENT_IGNORE);
    }
    if (o.action == VSR_CLIENT_DONE) {
        CHECK(r.status == VSR_REPLY_OK);
        CHECK(vsr_client_min_op(c) >= r.op);
    }
    if (o.action == VSR_CLIENT_FAILED) {
        CHECK(o.status == r.status);
    }
    check_outcome(w, lane, &o);
}

static void random_resolve(struct world *w, uint32_t lane)
{
    struct vsr_client *c = w->fixture.client;
    struct model *m = &w->lanes[lane];
    struct vsr_client_lane_status s = lane_status(c, lane);
    struct vsr_client_attempt a;
    struct vsr_client_outcome o;
    if (s.state != VSR_CLIENT_LANE_DETACHED &&
        s.state != VSR_CLIENT_LANE_QUERYING) {
        CHECK(vsr_client_resume(c, lane, m->request.body, w->now, &a) ==
              VSR_EINVAL);
        return;
    }
    if (test_random_bounded(&w->random, 2) == 0) {
        CHECK(vsr_client_resume(c, lane, m->request.body, w->now, &a) ==
              VSR_OK);
        check_attempt(w, &a);
        return;
    }
    struct vsr_id who;
    uint64_t replica;
    CHECK(vsr_client_query(c, lane, &who, &replica) == VSR_OK);
    struct vsr_reply q;
    memset(&q, 0, sizeof(q));
    q.request.client = who;
    q.status = test_random_bounded(&w->random, 4) == 0
                   ? VSR_REPLY_CLIENT_UNKNOWN
                   : VSR_REPLY_CLIENT_STATE;
    if (q.status == VSR_REPLY_CLIENT_STATE) {
        q.request.number =
            m->request.id.number - 1 + test_random_bounded(&w->random, 3);
        if (test_random_bounded(&w->random, 2) == 0) {
            q.flags = VSR_REPLY_EXECUTED;
            q.op = ++w->op;
        }
    }
    CHECK(vsr_client_queried(c, lane, &q, w->now, &o) == VSR_OK);
    if (o.action == VSR_CLIENT_DONE) {
        CHECK(q.request.number == m->request.id.number && q.flags != 0);
    } else if (o.action == VSR_CLIENT_FAILED) {
        CHECK(q.request.number > m->request.id.number);
        CHECK(o.status == VSR_REPLY_STALE_REQUEST);
    } else {
        CHECK(o.action == VSR_CLIENT_IGNORE);
        CHECK(lane_status(c, lane).state == VSR_CLIENT_LANE_DETACHED);
    }
    check_outcome(w, lane, &o);
}

static void random_restart(struct world *w)
{
    unsigned char image[1024];
    size_t written;
    struct fixture next;
    struct vsr_client *c = w->fixture.client;
    CHECK(vsr_client_export(c, image, sizeof(image), &written) == VSR_OK);
    CHECK(written == vsr_client_export_size(c));
    setup_options(&next, RANDOM_LANES, 5);
    next.options.seed = NULL;
    create(&next);
    CHECK(vsr_client_import(next.client, image, written) == VSR_OK);
    struct vsr_client_status before = client_status(c);
    struct vsr_client_status after = client_status(next.client);
    CHECK(before.epoch == after.epoch && before.primary == after.primary);
    CHECK(before.min_op == after.min_op && before.lanes == after.lanes);
    CHECK(before.busy == after.busy);
    teardown(&w->fixture);
    w->fixture = next;
    for (uint32_t i = 0; i < RANDOM_LANES; i++) {
        if (w->lanes[i].outstanding) {
            CHECK(lane_status(next.client, i).state ==
                  VSR_CLIENT_LANE_DETACHED);
        }
    }
}

static void random_step(struct world *w)
{
    struct vsr_client *c = w->fixture.client;
    uint32_t lane = test_random_bounded(&w->random, RANDOM_LANES);
    struct model *m = &w->lanes[lane];
    struct vsr_client_attempt a;
    struct vsr_client_outcome o;
    switch (test_random_bounded(&w->random, 16)) {
    case 0:
        if (!m->open) {
            uint32_t got;
            m->serial = ++w->serial;
            m->last_number = test_random_bounded(&w->random, 5);
            CHECK(vsr_client_open(c, incarnation(m->serial), m->last_number + 1,
                                  &got) == VSR_OK);
            /* The lowest closed slot. */
            for (uint32_t i = 0; i < got; i++) {
                CHECK(w->lanes[i].open);
            }
            if (got != lane) {
                w->lanes[got] = *m;
                memset(m, 0, sizeof(*m));
                m = &w->lanes[got];
            }
            m->open = true;
            m->outstanding = false;
            m->last_epoch = 0;
        }
        break;
    case 1:
        if (m->open) {
            int result = vsr_client_close(c, lane);
            CHECK(result == (m->outstanding ? VSR_EBUSY : VSR_OK));
            if (result == VSR_OK) {
                memset(m, 0, sizeof(*m));
            }
        }
        break;
    case 2:
    case 3:
    case 4:
        if (m->open) {
            const void *body =
                &w->bodies[test_random_bounded(&w->random, RANDOM_BODIES)];
            int result = vsr_client_begin(c, lane, VSR_REQUEST_COMMAND, body,
                                          w->now, &a);
            if (m->outstanding) {
                CHECK(result == VSR_EBUSY);
                break;
            }
            CHECK(result == VSR_OK && a.attempt == 1 && a.lane == lane);
            /* Numbers strictly increase per lane. */
            CHECK(a.request.id.number == m->last_number + 1);
            m->last_number = a.request.id.number;
            m->request = a.request;
            m->outstanding = true;
            m->last_epoch = 0;
            check_attempt(w, &a);
        }
        break;
    case 5:
    case 6:
    case 7:
    case 8:
    case 9:
        if (m->open) {
            random_reply(w, lane);
        }
        break;
    case 10:
    case 11:
    case 12: {
        w->now += test_random_bounded(&w->random, (uint32_t)(2 * TIMEOUT));
        uint32_t fired = 0;
        int result;
        while ((result = vsr_client_time(c, w->now, &o)) == 1) {
            CHECK(o.action == VSR_CLIENT_RETRY);
            check_outcome(w, o.lane, &o);
            CHECK(++fired <= RANDOM_LANES);
        }
        CHECK(result == 0);
        CHECK(vsr_client_deadline(c) > w->now);
        break;
    }
    case 13:
        if (m->open && m->outstanding) {
            random_resolve(w, lane);
        }
        break;
    case 14:
        if (test_random_bounded(&w->random, 8) == 0) {
            random_restart(w);
        }
        break;
    default: {
        struct vsr_read_barrier barrier;
        uint64_t replica;
        vsr_client_observe(c, w->op);
        CHECK(vsr_client_read(c, VSR_READ_CAUSAL, w->now, &barrier, &replica) ==
              VSR_OK);
        CHECK(barrier.min_op == vsr_client_min_op(c));
        break;
    }
    }
    check_world(w);
}

static void randomized(uint64_t seed, uint32_t steps)
{
    static struct world w;
    static const char text[RANDOM_BODIES][4] = {"a", "bb", "ccc"};
    static struct vsr_span spans[RANDOM_BODIES];
    printf("client randomized seed=%" PRIu64 " steps=%" PRIu32 "\n", seed,
           steps);
    fflush(stdout);
    memset(&w, 0, sizeof(w));
    test_random_seed(&w.random, seed, 54);
    for (uint32_t i = 0; i < RANDOM_BODIES; i++) {
        spans[i] = (struct vsr_span){text[i], i + 1};
        w.bodies[i] = (struct vsr_blob){&spans[i], i + 1, 1, 0};
    }
    setup_options(&w.fixture, RANDOM_LANES, 5);
    w.fixture.options.seed = NULL;
    create(&w.fixture);
    for (uint32_t i = 0; i < steps; i++) {
        random_step(&w);
    }
    teardown(&w.fixture);
}

int main(int argc, char **argv)
{
    uint64_t seed = 1;
    uint32_t steps = 20000;
    if (argc > 1) {
        seed = strtoull(argv[1], NULL, 0);
    }
    if (argc > 2) {
        steps = (uint32_t)strtoul(argv[2], NULL, 0);
    }
    layout_errors();
    init_errors();
    lane_lifecycle();
    begin_attempt();
    reply_done();
    not_primary();
    new_epoch();
    busy_backoff();
    failed();
    time_expiry();
    detached();
    learn();
    reads();
    persistence();
    independent_lanes();
    for (uint64_t i = 0; i < 8; i++) {
        randomized(seed + i, steps);
    }
    return 0;
}
