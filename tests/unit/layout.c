#include "config.h"

#include "lib/check.h"
#include "vsr.h"

#include <stdalign.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

/* vsr_layout "validates options, checks size arithmetic, and includes
 * progress reserves in its result"; vsr_init "requires at least that size and
 * alignment and copies all option/seed metadata". These tests pin the arena
 * planner's argument contract at every limit boundary. */

struct fixture {
    struct vsr_member members[2];
    struct vsr_membership membership;
    struct vsr_options options;
};

static void setup(struct fixture *f)
{
    memset(f, 0, sizeof(*f));
    f->members[0] = (struct vsr_member){1, VSR_MEMBER_FULL, 0};
    f->members[1] = (struct vsr_member){2, VSR_MEMBER_FULL, 0};
    f->membership = (struct vsr_membership){0, f->members, 2, 0};
    f->options = (struct vsr_options){
        .cluster = {1, 1},
        .incarnation = {2, 1},
        .replica = 1,
        .seed = &f->membership,
        .limits =
            {
                .members = 4,
                .operations = 8,
                .input_leases = 16,
                .pending_requests = 4,
                .pending_reads = 4,
                .transfers = 4,
                .log_cache_entries = 8,
                .client_cache_entries = 8,
                .batch_entries = 4,
                .spans_per_blob = 4,
                .work_per_step = 64,
                .command_bytes = 64,
                .result_bytes = 32,
                .manifest_bytes = 64,
                .message_bytes = 256,
                .pinned_payload_bytes = 2048,
            },
        .heartbeat_ns = 10,
        .view_timeout_ns = 30,
        .retry_ns = 5,
        .transfer_timeout_ns = 100,
        .start_mode = VSR_START_NEW,
        .durability = VSR_DURABLE,
    };
}

static size_t size_of(const struct vsr_options *options)
{
    struct vsr_layout layout;
    CHECK(vsr_layout(options, &layout) == VSR_OK);
    CHECK(layout.size != 0 && layout.alignment != 0);
    CHECK((layout.alignment & (layout.alignment - 1)) == 0);
    CHECK(layout.alignment >= alignof(max_align_t));
    return layout.size;
}

static int layout_result(const struct vsr_options *options)
{
    struct vsr_layout layout = {123, 456};
    int result = vsr_layout(options, &layout);
    if (result != VSR_OK)
        CHECK(layout.size == 0 && layout.alignment == 0);
    return result;
}

/* Keep the documented cross-limit invariants when one limit grows:
 * "input_leases >= transfers + 8" and "batch_entries <= log_cache_entries". */
static void coherent(struct vsr_options *options)
{
    struct vsr_limits *bounds = &options->limits;
    if (bounds->input_leases < bounds->transfers + 8u)
        bounds->input_leases = bounds->transfers + 8u;
    if (bounds->log_cache_entries < bounds->batch_entries)
        bounds->log_cache_entries = bounds->batch_entries;
}

/* Each limit reserves capacity; raising any one of them never shrinks the
 * arena, and work_per_step (a per-call budget) reserves nothing. */
static void monotonic(void)
{
    struct fixture f;
    setup(&f);
    const size_t base = size_of(&f.options);
    const size_t offsets32[] = {
        offsetof(struct vsr_limits, members),
        offsetof(struct vsr_limits, operations),
        offsetof(struct vsr_limits, input_leases),
        offsetof(struct vsr_limits, pending_requests),
        offsetof(struct vsr_limits, pending_reads),
        offsetof(struct vsr_limits, transfers),
        offsetof(struct vsr_limits, log_cache_entries),
        offsetof(struct vsr_limits, client_cache_entries),
        offsetof(struct vsr_limits, batch_entries),
        offsetof(struct vsr_limits, spans_per_blob),
    };
    const size_t offsets64[] = {
        offsetof(struct vsr_limits, command_bytes),
        offsetof(struct vsr_limits, result_bytes),
        offsetof(struct vsr_limits, manifest_bytes),
        offsetof(struct vsr_limits, message_bytes),
        offsetof(struct vsr_limits, pinned_payload_bytes),
    };
    for (size_t i = 0; i < sizeof(offsets32) / sizeof(offsets32[0]); i++) {
        struct vsr_options doubled = f.options;
        unsigned char *field = (unsigned char *)&doubled.limits + offsets32[i];
        uint32_t value;
        memcpy(&value, field, sizeof(value));
        value *= 2;
        memcpy(field, &value, sizeof(value));
        coherent(&doubled);
        CHECK(size_of(&doubled) >= base);
        /* Growth is itself monotonic: doubling again never shrinks. */
        struct vsr_options again = doubled;
        value *= 2;
        memcpy((unsigned char *)&again.limits + offsets32[i], &value,
               sizeof(value));
        coherent(&again);
        CHECK(size_of(&again) >= size_of(&doubled));
    }
    for (size_t i = 0; i < sizeof(offsets64) / sizeof(offsets64[0]); i++) {
        struct vsr_options doubled = f.options;
        unsigned char *field = (unsigned char *)&doubled.limits + offsets64[i];
        uint64_t value;
        memcpy(&value, field, sizeof(value));
        value *= 2;
        memcpy(field, &value, sizeof(value));
        /* Byte limits grow the payload budget that the planner checks. */
        doubled.limits.message_bytes *= 2;
        doubled.limits.pinned_payload_bytes *= 4;
        CHECK(size_of(&doubled) >= base);
    }
    {
        struct vsr_options budget = f.options;
        budget.limits.work_per_step = 1;
        CHECK(size_of(&budget) == base);
        budget.limits.work_per_step = UINT32_MAX;
        CHECK(size_of(&budget) == base);
    }
    {
        /* The seed size does not depend on limits beyond its own count. */
        struct vsr_options one = f.options;
        struct vsr_membership single = {0, f.members, 1, 0};
        one.seed = &single;
        CHECK(size_of(&one) <= base);
    }
}

/* "cache_line_bytes = 0 selects 64-byte alignment. Other values must be
 * powers of two; the returned arena alignment also satisfies the alignment of
 * every internal type." */
static void alignment(void)
{
    struct fixture f;
    setup(&f);
    const uint32_t lines[] = {0, 8, 64, 128, 4096};
    for (size_t i = 0; i < sizeof(lines) / sizeof(lines[0]); i++) {
        struct vsr_layout layout;
        f.options.cache_line_bytes = lines[i];
        CHECK(vsr_layout(&f.options, &layout) == VSR_OK);
        CHECK((layout.alignment & (layout.alignment - 1)) == 0);
        CHECK(layout.alignment >= alignof(max_align_t));
        CHECK(layout.alignment >= (lines[i] == 0 ? 64u : lines[i]));
        CHECK(layout.size % layout.alignment == 0);
        void *memory = aligned_alloc(layout.alignment, layout.size);
        struct vsr *v = NULL;
        CHECK(memory != NULL);
        CHECK(vsr_init(memory, layout.size, &f.options, &v) == VSR_OK);
        CHECK(v != NULL && vsr_deinit(v) == VSR_EBUSY);
        free(memory);
    }
    f.options.cache_line_bytes = 24;
    CHECK(layout_result(&f.options) == VSR_EINVAL);
    f.options.cache_line_bytes = 3;
    CHECK(layout_result(&f.options) == VSR_EINVAL);
}

/* "Sizes are checked for overflow. Configurations exceeding limits are
 * rejected before admission": every arithmetic boundary is ELIMIT, with the
 * layout zeroed, and one below the documented payload minimum fails while the
 * minimum itself is accepted. */
static void limits(void)
{
    struct fixture f;
    setup(&f);
    struct vsr_options o;
#define LIMIT(statements)                                                      \
    do {                                                                       \
        o = f.options;                                                         \
        statements;                                                            \
        CHECK(layout_result(&o) == VSR_ELIMIT);                                \
    } while (0)
    LIMIT(o.limits.transfers = UINT32_MAX - 7u;
          o.limits.input_leases = UINT32_MAX);
    LIMIT(o.limits.input_leases = o.limits.transfers + 7u);
    LIMIT(o.limits.operations = UINT32_MAX);
    LIMIT(o.limits.batch_entries = o.limits.log_cache_entries + 1u);
    LIMIT(o.limits.result_bytes = UINT64_MAX / o.limits.batch_entries + 1u);
    LIMIT(o.limits.message_bytes =
              o.limits.command_bytes + o.limits.manifest_bytes - 1u);
    LIMIT(o.limits.manifest_bytes = UINT64_MAX - 1u;
          o.limits.command_bytes = 2);
    LIMIT(o.limits.manifest_bytes = UINT64_MAX / 4u;
          o.limits.message_bytes = UINT64_MAX / 2u;
          o.limits.pinned_payload_bytes = UINT64_MAX);
    LIMIT(o.limits.command_bytes = UINT64_MAX / 2u;
          o.limits.message_bytes = UINT64_MAX / 2u + 64u;
          o.limits.pinned_payload_bytes = UINT64_MAX);
    /* Metadata arithmetic: per-operation descriptor storage overflows. */
    LIMIT(o.limits.operations = UINT32_MAX - 1u;
          o.limits.spans_per_blob = UINT32_MAX;
          o.limits.batch_entries = UINT32_MAX - 8u;
          o.limits.log_cache_entries = UINT32_MAX - 8u;
          o.limits.result_bytes = 1);
    /* Transition module arithmetic: retained offer members overflow. */
    LIMIT(o.limits.transfers = UINT32_MAX - 8u;
          o.limits.input_leases = UINT32_MAX; o.limits.members = UINT32_MAX;
          o.limits.manifest_bytes = 1;
          o.limits.pinned_payload_bytes = UINT64_MAX / 2u);
    LIMIT(o.limits.members = UINT32_MAX; o.limits.transfers = UINT32_MAX / 2u;
          o.limits.input_leases = UINT32_MAX / 2u + 8u;
          o.limits.manifest_bytes = 1;
          o.limits.pinned_payload_bytes = UINT64_MAX / 2u);
#undef LIMIT
    {
        const uint64_t minimum = f.options.limits.message_bytes +
                                 2u * f.options.limits.command_bytes +
                                 ((uint64_t)f.options.limits.transfers + 3u) *
                                     f.options.limits.manifest_bytes +
                                 f.options.limits.result_bytes;
        o = f.options;
        o.limits.pinned_payload_bytes = minimum;
        CHECK(layout_result(&o) == VSR_OK);
        o.limits.pinned_payload_bytes = minimum - 1u;
        CHECK(layout_result(&o) == VSR_ELIMIT);
        o.limits.pinned_payload_bytes = minimum;
        o.limits.input_leases = o.limits.transfers + 8u;
        CHECK(layout_result(&o) == VSR_OK);
        /* The lease reserve is tied to transfers, not batch size. */
        o.limits.batch_entries = 8;
        CHECK(layout_result(&o) == VSR_OK);
    }
    /* Invalid options are EINVAL, never ELIMIT, and zero the layout. */
    o = f.options;
    o.limits.members = 0;
    CHECK(layout_result(&o) == VSR_EINVAL);
    o = f.options;
    o.replica = 3;
    CHECK(layout_result(&o) == VSR_EINVAL);
    CHECK(layout_result(NULL) == VSR_EINVAL);
    CHECK(vsr_layout(&f.options, NULL) == VSR_EINVAL);
}

/* vsr_init: "NULL arguments/misalignment return EINVAL; insufficient size
 * returns ELIMIT. memory must not overlap options, seed metadata, or out."
 * On success the seed is copied; "No retained pointers into options." */
static void init_arguments(void)
{
    struct fixture f;
    struct vsr_layout layout;
    struct vsr *v = (struct vsr *)&f;
    setup(&f);
    CHECK(vsr_layout(&f.options, &layout) == VSR_OK);
    const size_t padded = layout.size + layout.alignment;
    unsigned char *memory = aligned_alloc(layout.alignment, padded);
    CHECK(memory != NULL);
    CHECK(vsr_init(memory, layout.size, &f.options, NULL) == VSR_EINVAL);
    CHECK(vsr_init(NULL, layout.size, &f.options, &v) == VSR_EINVAL);
    CHECK(v == NULL);
    v = (struct vsr *)&f;
    CHECK(vsr_init(memory, layout.size, NULL, &v) == VSR_EINVAL);
    CHECK(v == NULL);
    v = (struct vsr *)&f;
    CHECK(vsr_init(memory + 1, layout.size, &f.options, &v) == VSR_EINVAL);
    CHECK(v == NULL);
    v = (struct vsr *)&f;
    CHECK(vsr_init(memory, layout.size - 1, &f.options, &v) == VSR_ELIMIT);
    CHECK(v == NULL);
    {
        /* Invalid options fail init exactly as they fail layout. */
        struct vsr_options invalid = f.options;
        invalid.limits.input_leases = invalid.limits.transfers + 7u;
        v = (struct vsr *)&f;
        CHECK(vsr_init(memory, layout.size, &invalid, &v) == VSR_ELIMIT);
        CHECK(v == NULL);
        invalid = f.options;
        invalid.heartbeat_ns = 0;
        v = (struct vsr *)&f;
        CHECK(vsr_init(memory, layout.size, &invalid, &v) == VSR_EINVAL);
        CHECK(v == NULL);
    }
    {
        /* The arena may overlap neither the options nor the out pointer. */
        struct vsr_options *inside = (void *)memory;
        *inside = f.options;
        CHECK(vsr_init(memory, layout.size, inside, &v) == VSR_EINVAL);
        struct vsr **out_inside =
            (void *)(memory + layout.size - sizeof(struct vsr *));
        CHECK(vsr_init(memory, layout.size, &f.options, out_inside) ==
              VSR_EINVAL);
    }
    {
        /* Nor the seed membership or its member array. */
        struct vsr_membership *seed = (void *)memory;
        struct vsr_options options = f.options;
        *seed = f.membership;
        options.seed = seed;
        v = (struct vsr *)&f;
        CHECK(vsr_init(memory, layout.size, &options, &v) == VSR_EINVAL);
        struct vsr_member *members =
            (void *)(memory + layout.size - sizeof(f.members));
        struct vsr_membership borrowed = f.membership;
        memcpy(members, f.members, sizeof(f.members));
        borrowed.members = members;
        options.seed = &borrowed;
        v = (struct vsr *)&f;
        CHECK(vsr_init(memory, layout.size, &options, &v) == VSR_EINVAL);
        /* Half-open ranges that merely touch are legal. */
        options.seed = &f.membership;
        v = NULL;
        CHECK(vsr_init(memory, layout.size, &options, &v) == VSR_OK);
        CHECK(v != NULL);
        struct vsr_status status;
        vsr_get_status(v, &status);
        CHECK(status.state == VSR_STATE_STARTING);
        CHECK(status.failure.code == VSR_FAILURE_NONE);
        CHECK(vsr_deinit(v) == VSR_EBUSY);
    }
    {
        /* The seed is copied: mutating the caller's copy changes nothing. */
        unsigned char *arena = aligned_alloc(layout.alignment, layout.size);
        CHECK(arena != NULL);
        v = NULL;
        CHECK(vsr_init(arena, layout.size, &f.options, &v) == VSR_OK);
        f.members[0].id = 77;
        f.membership.count = 1;
        f.options.replica = 5;
        struct vsr_status status;
        vsr_get_status(v, &status);
        CHECK(status.state == VSR_STATE_STARTING);
        /* Larger and more aligned memory than required is accepted. */
        struct vsr *w = NULL;
        setup(&f);
        CHECK(vsr_init(memory, padded, &f.options, &w) == VSR_OK);
        CHECK(w != NULL && w != v);
        CHECK(vsr_deinit(w) == VSR_EBUSY);
        free(arena);
    }
    free(memory);
}

int main(void)
{
    monotonic();
    alignment();
    limits();
    init_arguments();
    return 0;
}
