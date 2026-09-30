#include "config.h"

#include "io/codec.h"
#include "io/crc32c.h"
#include "io/cursor.h"
#include "io/wire.h"
#include "lib/check.h"
#include "lib/random.h"

#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * Round trips of every message type, store change, superblock, segment
 * header and clients file record at minimal, maximal and random shapes
 * under several limit sets; the vectored encoder against a one-shot
 * encoding under random and extreme resource bounds; decoding through
 * cursors split at random positions; truncated and corrupted inputs; and
 * the sizing functions as bounds on the largest legal graphs.
 */

#define ARENA_BYTES (8u << 20)
#define BUFFER_BYTES (1u << 20)
#define REGION_BYTES (1u << 20)
#define MAX_VECTORS 8192u

/* Host graphs under test are built in this arena; decoders get regions of
 * exactly the size the codec computes. */
static unsigned char arena_memory[ARENA_BYTES];
static struct vsr_io_bump arena;
static unsigned char reference_frame[BUFFER_BYTES];
static unsigned char random_frame[BUFFER_BYTES];
static unsigned char writer_memory[BUFFER_BYTES];
static unsigned char region_memory[REGION_BYTES];
static unsigned char scratch_a[BUFFER_BYTES];
static unsigned char scratch_b[BUFFER_BYTES];
static struct vsr_io_vec vectors[MAX_VECTORS];
static struct test_random rng;
static const char *corpus_dir;
static unsigned corpus_index;
static const char *entry_corpus_dir;
static unsigned entry_corpus_index;

static const struct vsr_limits limit_sets[] = {
    {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 4},
    {3, 4, 4, 4, 4, 4, 8, 8, 4, 2, 16, 64, 48, 32, 256, 1024},
    /* The limit set of tests/fuzzy/frame, so its seeds decode there. */
    {5, 8, 16, 8, 8, 8, 8, 8, 4, 4, 64, 256, 256, 256, 1024, 4096},
    {16, 8, 8, 8, 8, 8, 64, 64, 64, 8, 64, 4096, 512, 2048, 16384, 65536},
};

#define LIMIT_SETS (sizeof(limit_sets) / sizeof(limit_sets[0]))

/* -------------------------------------------------------------------------
 * Helpers
 * ---------------------------------------------------------------------- */

static uint32_t below(uint32_t bound)
{
    return test_random_bounded(&rng, bound);
}

static uint64_t random64(void)
{
    return (uint64_t)test_random_next(&rng) << 32 | test_random_next(&rng);
}

static void arena_reset(void)
{
    vsr_io_bump_init(&arena, arena_memory, sizeof(arena_memory));
}

static void *arena_alloc(size_t size, size_t alignment)
{
    void *out = vsr_io_bump_alloc(&arena, size, alignment);

    CHECK(out != NULL);
    return out;
}

#define NEW(type) ((type *)arena_alloc(sizeof(type), alignof(type)))
#define NEW_ARRAY(type, count)                                                 \
    ((type *)arena_alloc(sizeof(type) * (count), alignof(type)))

static void fill_random(unsigned char *out, size_t size)
{
    for (size_t i = 0; i < size; ++i) {
        out[i] = (unsigned char)below(256);
    }
}

/* Writes a fuzz corpus seed into dir when it is set: the harness's
 * selector byte, three zero cut bytes, then the encoding. */
static void write_seed(const char *dir, unsigned *index, unsigned selector,
                       const unsigned char *bytes, size_t size)
{
    char path[512];
    FILE *file;
    unsigned char header[4] = {(unsigned char)selector, 0, 0, 0};

    if (dir == NULL) {
        return;
    }
    if (snprintf(path, sizeof(path), "%s/seed-%03u", dir, (*index)++) >=
        (int)sizeof(path)) {
        return;
    }
    file = fopen(path, "wb");
    CHECK(file != NULL);
    CHECK(fwrite(header, 1, sizeof(header), file) == sizeof(header));
    CHECK(fwrite(bytes, 1, size, file) == size);
    CHECK(fclose(file) == 0);
}

/* A tests/fuzzy/frame seed when VSR_CODEC_CORPUS names a directory; the
 * selector is the harness's target. */
static void seed_corpus(unsigned target, const unsigned char *bytes,
                        size_t size)
{
    write_seed(corpus_dir, &corpus_index, target, bytes, size);
}

/* A tests/fuzzy/entry seed when VSR_CODEC_ENTRY_CORPUS names a directory:
 * an APPEND payload and the index of the entry to decode. */
static void seed_entry_corpus(uint32_t skip, const unsigned char *bytes,
                              size_t size)
{
    write_seed(entry_corpus_dir, &entry_corpus_index, skip, bytes, size);
}

static size_t flatten_vectors(const struct vsr_io_vec *vecs, uint32_t count,
                              unsigned char *out, size_t offset)
{
    for (uint32_t i = 0; i < count; ++i) {
        CHECK(vecs[i].length > 0);
        CHECK(offset + vecs[i].length <= BUFFER_BYTES);
        memcpy(out + offset, vecs[i].base, vecs[i].length);
        offset += vecs[i].length;
    }
    return offset;
}

/* Concatenates a blob's spans. */
static size_t blob_bytes(const struct vsr_blob *blob, unsigned char *out)
{
    size_t size = 0;

    for (uint32_t i = 0; i < blob->count; ++i) {
        CHECK(size + blob->spans[i].size <= BUFFER_BYTES);
        memcpy(out + size, blob->spans[i].data, blob->spans[i].size);
        size += blob->spans[i].size;
    }
    CHECK(size == blob->size);
    return size;
}

/* -------------------------------------------------------------------------
 * Graph builders: mode 0 minimal, 1 maximal, 2 random
 * ---------------------------------------------------------------------- */

struct builder {
    const struct vsr_limits *limits;
    int mode;
    int entry_type;   /* -1: by mode; else every entry has this type. */
    uint64_t payload; /* Blob bytes still allowed under message_bytes. */
};

static void builder_init(struct builder *b, const struct vsr_limits *limits,
                         int mode, int entry_type)
{
    b->limits = limits;
    b->mode = mode;
    b->entry_type = entry_type;
    b->payload = limits->message_bytes;
}

/* 0, maximum, or uniform in [0, maximum] by mode. */
static uint32_t pick(const struct builder *b, uint32_t maximum)
{
    switch (b->mode) {
    case 0:
        return 0;
    case 1:
        return maximum;
    default:
        return below(maximum + 1);
    }
}

static uint64_t pick64(const struct builder *b)
{
    switch (b->mode) {
    case 0:
        return 1;
    case 1:
        return UINT64_C(0x0123456789abcdef);
    default:
        return random64();
    }
}

static void build_blob(struct builder *b, struct vsr_blob *blob, uint64_t limit)
{
    uint64_t cap = limit < b->payload ? limit : b->payload;
    uint32_t size = pick(b, (uint32_t)cap);
    uint32_t parts;
    uint32_t remaining;
    struct vsr_span *spans;
    unsigned char *bytes;

    b->payload -= size;
    blob->reserved = 0;
    blob->size = size;
    if (size == 0) {
        blob->spans = NULL;
        blob->count = 0;
        return;
    }
    parts = b->limits->spans_per_blob < size ? b->limits->spans_per_blob : size;
    parts = 1 + below(parts);
    spans = NEW_ARRAY(struct vsr_span, parts);
    bytes = arena_alloc(size, 1);
    fill_random(bytes, size);
    remaining = size;
    for (uint32_t i = 0; i < parts; ++i) {
        uint32_t reserve = parts - 1 - i;
        uint32_t n =
            i + 1 == parts ? remaining : 1 + below(remaining - reserve);

        spans[i].data = bytes;
        spans[i].size = n;
        bytes += n;
        remaining -= n;
    }
    blob->spans = spans;
    blob->count = parts;
}

static struct vsr_membership *build_membership(struct builder *b,
                                               uint64_t epoch)
{
    struct vsr_membership *membership = NEW(struct vsr_membership);
    uint32_t count = 1 + pick(b, b->limits->members - 1);
    struct vsr_member *members = NEW_ARRAY(struct vsr_member, count);
    uint64_t id = 0;

    for (uint32_t i = 0; i < count; ++i) {
        id += 1 + below(1000);
        members[i].id = id;
        members[i].role = b->mode == 0 || below(2) == 0 ? VSR_MEMBER_FULL
                                                        : VSR_MEMBER_WITNESS;
        members[i].reserved = 0;
    }
    membership->epoch = epoch;
    membership->members = members;
    membership->count = count;
    membership->faults = pick(b, (count - 1) / 2);
    return membership;
}

static struct vsr_epoch *build_epoch(struct builder *b)
{
    struct vsr_epoch *epoch = NEW(struct vsr_epoch);
    bool previous = b->mode == 1 || (b->mode == 2 && below(2) == 0);
    uint64_t number = previous ? 1 + below(1000) : 0;

    epoch->current = build_membership(b, number);
    epoch->previous = previous ? build_membership(b, number - 1) : NULL;
    epoch->boundary = previous ? pick64(b) : 0;
    epoch->phase = pick(b, VSR_EPOCH_INSTALLED);
    epoch->reserved = 0;
    return epoch;
}

static struct vsr_checkpoint *build_checkpoint(struct builder *b)
{
    struct vsr_checkpoint *checkpoint = NEW(struct vsr_checkpoint);

    checkpoint->id.hi = pick64(b);
    checkpoint->id.lo = pick64(b) | 1;
    checkpoint->op = pick64(b);
    checkpoint->view = pick64(b);
    checkpoint->epoch = build_epoch(b);
    build_blob(b, &checkpoint->manifest, b->limits->manifest_bytes);
    return checkpoint;
}

static void build_entry(struct builder *b, struct vsr_entry *entry)
{
    uint32_t type = b->entry_type >= 0 ? (uint32_t)b->entry_type
                    : b->mode == 2     ? below(4)
                                       : VSR_REQUEST_NOOP;

    entry->op = pick64(b);
    entry->epoch = pick64(b);
    entry->view = pick64(b);
    entry->request.client.hi = type == VSR_REQUEST_NOOP ? 0 : pick64(b);
    entry->request.client.lo = type == VSR_REQUEST_NOOP ? 0 : pick64(b) | 1;
    entry->request.number = type == VSR_REQUEST_NOOP ? 0 : pick64(b);
    entry->type = type;
    entry->reserved = 0;
    switch (type) {
    case VSR_REQUEST_COMMAND: {
        struct vsr_blob *blob = NEW(struct vsr_blob);

        build_blob(b, blob, b->limits->command_bytes);
        entry->body = blob;
        break;
    }
    case VSR_REQUEST_RECONFIGURE:
        entry->body = build_membership(b, entry->epoch + 1);
        break;
    case VSR_REQUEST_CHECK_EPOCH: {
        struct vsr_check_epoch *check = NEW(struct vsr_check_epoch);

        check->epoch = pick64(b);
        entry->body = check;
        break;
    }
    default:
        entry->body = NULL;
        break;
    }
}

static struct vsr_entry *build_entry_array(struct builder *b, uint32_t count)
{
    struct vsr_entry *entries;

    if (count == 0) {
        return NULL;
    }
    entries = NEW_ARRAY(struct vsr_entry, count);
    for (uint32_t i = 0; i < count; ++i) {
        build_entry(b, &entries[i]);
    }
    return entries;
}

static void build_entries(struct builder *b, struct vsr_entries *entries)
{
    entries->count = pick(b, b->limits->batch_entries);
    entries->entries = build_entry_array(b, entries->count);
    entries->reserved = 0;
}

static void build_nonce(struct builder *b, struct vsr_nonce *nonce)
{
    nonce->incarnation.hi = pick64(b);
    nonce->incarnation.lo = pick64(b);
    nonce->counter = pick64(b);
}

static void build_log_state(struct builder *b, struct vsr_log_state *state)
{
    state->revision.incarnation.hi = pick64(b);
    state->revision.incarnation.lo = pick64(b);
    state->revision.sequence = pick64(b);
    state->view = pick64(b);
    state->last_normal_view = pick64(b);
    state->committed = pick64(b);
    state->log_begin = pick64(b);
    state->log_end = pick64(b);
    state->epoch = build_epoch(b);
    build_entries(b, &state->entries);
    state->checkpoint = b->mode == 1 || (b->mode == 2 && below(2) == 0)
                            ? build_checkpoint(b)
                            : NULL;
}

static struct vsr_message *build_message(struct builder *b, uint32_t type)
{
    struct vsr_message *message = NEW(struct vsr_message);

    message->cluster.hi = pick64(b);
    message->cluster.lo = pick64(b);
    message->epoch = pick64(b);
    message->view = pick64(b);
    message->from = pick64(b);
    message->type = type;
    message->flags = 0;
    message->number = pick64(b);
    message->body = NULL;
    switch (type) {
    case VSR_MSG_PREPARE: {
        struct vsr_prepare *prepare = NEW(struct vsr_prepare);

        build_entries(b, &prepare->batch);
        prepare->committed = pick64(b);
        message->body = prepare;
        break;
    }
    case VSR_MSG_DO_VIEW_CHANGE:
    case VSR_MSG_START_VIEW: {
        struct vsr_log_state *state = NEW(struct vsr_log_state);

        build_log_state(b, state);
        message->body = state;
        break;
    }
    case VSR_MSG_RECOVERY:
    case VSR_MSG_RECOVERY_RESPONSE: {
        struct vsr_recovery *recovery = NEW(struct vsr_recovery);

        build_nonce(b, &recovery->nonce);
        recovery->state = NULL;
        if (b->mode == 1 || (b->mode == 2 && below(2) == 0)) {
            struct vsr_log_state *state = NEW(struct vsr_log_state);

            build_log_state(b, state);
            recovery->state = state;
        }
        message->body = recovery;
        break;
    }
    case VSR_MSG_GET_STATE:
    case VSR_MSG_GET_LOG: {
        struct vsr_fetch *fetch = NEW(struct vsr_fetch);

        build_nonce(b, &fetch->nonce);
        fetch->revision.incarnation.hi = pick64(b);
        fetch->revision.incarnation.lo = pick64(b);
        fetch->revision.sequence = pick64(b);
        fetch->first = pick64(b);
        fetch->end = pick64(b);
        fetch->max_bytes = pick64(b);
        fetch->max_entries = pick(b, UINT32_MAX - 1);
        fetch->reserved = 0;
        message->body = fetch;
        break;
    }
    case VSR_MSG_NEW_STATE:
    case VSR_MSG_LOG:
    case VSR_MSG_STATE_UNAVAILABLE: {
        struct vsr_state_chunk *chunk = NEW(struct vsr_state_chunk);

        build_nonce(b, &chunk->nonce);
        build_log_state(b, &chunk->state);
        chunk->first = pick64(b);
        chunk->next = pick64(b);
        message->body = chunk;
        break;
    }
    case VSR_MSG_START_EPOCH:
    case VSR_MSG_NEW_EPOCH:
        message->body = build_epoch(b);
        break;
    case VSR_MSG_CHECKPOINT:
        message->body = build_checkpoint(b);
        break;
    case VSR_MSG_READ_PROBE:
    case VSR_MSG_READ_ACK: {
        struct vsr_nonce *nonce = NEW(struct vsr_nonce);

        build_nonce(b, nonce);
        message->body = nonce;
        break;
    }
    default:
        break;
    }
    return message;
}

static struct vsr_hard_state *build_hard_state(struct builder *b)
{
    struct vsr_hard_state *hard = NEW(struct vsr_hard_state);

    hard->view = pick64(b);
    hard->last_normal_view = pick64(b);
    hard->committed = pick64(b);
    hard->epoch = build_epoch(b);
    hard->state = pick(b, VSR_HARD_RETIRED);
    hard->role = b->mode == 0 ? VSR_MEMBER_FULL : 1 + below(2);
    return hard;
}

static struct vsr_store_identity *build_identity(struct builder *b)
{
    struct vsr_store_identity *identity = NEW(struct vsr_store_identity);

    identity->cluster.hi = pick64(b);
    identity->cluster.lo = pick64(b) | 1;
    identity->replica = pick64(b) | 1;
    identity->durability = pick(b, VSR_REPLICATED);
    identity->reserved = 0;
    return identity;
}

static void build_client_record(struct builder *b,
                                struct vsr_client_record *record)
{
    uint64_t saved = b->payload;

    record->request.client.hi = pick64(b);
    record->request.client.lo = pick64(b) | 1;
    record->request.number = pick64(b) | 1;
    record->op = pick64(b) | 1;
    /* Results are bounded per record, not by message_bytes. */
    b->payload = UINT64_MAX;
    build_blob(b, &record->result.data, b->limits->result_bytes);
    b->payload = saved;
    record->result.code =
        b->mode == 2 ? (int32_t)test_random_next(&rng) : -(int32_t)b->mode;
    record->result.reserved = 0;
}

/* -------------------------------------------------------------------------
 * Logical equality
 * ---------------------------------------------------------------------- */

static bool same_blob(const struct vsr_blob *a, const struct vsr_blob *b)
{
    size_t size_a = blob_bytes(a, scratch_a);
    size_t size_b = blob_bytes(b, scratch_b);

    return a->reserved == 0 && b->reserved == 0 && size_a == size_b &&
           memcmp(scratch_a, scratch_b, size_a) == 0;
}

static bool same_membership(const struct vsr_membership *a,
                            const struct vsr_membership *b)
{
    if (a == NULL || b == NULL) {
        return a == b;
    }
    if (a->epoch != b->epoch || a->count != b->count ||
        a->faults != b->faults) {
        return false;
    }
    for (uint32_t i = 0; i < a->count; ++i) {
        if (a->members[i].id != b->members[i].id ||
            a->members[i].role != b->members[i].role ||
            a->members[i].reserved != 0 || b->members[i].reserved != 0) {
            return false;
        }
    }
    return true;
}

static bool same_epoch(const struct vsr_epoch *a, const struct vsr_epoch *b)
{
    if (a == NULL || b == NULL) {
        return a == b;
    }
    return a->boundary == b->boundary && a->phase == b->phase &&
           a->reserved == 0 && b->reserved == 0 &&
           same_membership(a->current, b->current) &&
           same_membership(a->previous, b->previous);
}

static bool same_checkpoint(const struct vsr_checkpoint *a,
                            const struct vsr_checkpoint *b)
{
    if (a == NULL || b == NULL) {
        return a == b;
    }
    return a->id.hi == b->id.hi && a->id.lo == b->id.lo && a->op == b->op &&
           a->view == b->view && same_epoch(a->epoch, b->epoch) &&
           same_blob(&a->manifest, &b->manifest);
}

static bool same_entry(const struct vsr_entry *a, const struct vsr_entry *b)
{
    if (a->op != b->op || a->epoch != b->epoch || a->view != b->view ||
        a->request.client.hi != b->request.client.hi ||
        a->request.client.lo != b->request.client.lo ||
        a->request.number != b->request.number || a->type != b->type ||
        a->reserved != 0 || b->reserved != 0) {
        return false;
    }
    switch (a->type) {
    case VSR_REQUEST_COMMAND:
        return same_blob(a->body, b->body);
    case VSR_REQUEST_RECONFIGURE:
        return same_membership(a->body, b->body);
    case VSR_REQUEST_CHECK_EPOCH: {
        const struct vsr_check_epoch *x = a->body;
        const struct vsr_check_epoch *y = b->body;

        return x->epoch == y->epoch;
    }
    default:
        return a->body == NULL && b->body == NULL;
    }
}

static bool same_entry_array(const struct vsr_entry *a,
                             const struct vsr_entry *b, uint32_t count)
{
    for (uint32_t i = 0; i < count; ++i) {
        if (!same_entry(&a[i], &b[i])) {
            return false;
        }
    }
    return true;
}

static bool same_entries(const struct vsr_entries *a,
                         const struct vsr_entries *b)
{
    return a->count == b->count && a->reserved == 0 && b->reserved == 0 &&
           (a->count == 0) == (a->entries == NULL) &&
           (b->count == 0) == (b->entries == NULL) &&
           same_entry_array(a->entries, b->entries, a->count);
}

static bool same_nonce(const struct vsr_nonce *a, const struct vsr_nonce *b)
{
    return a->incarnation.hi == b->incarnation.hi &&
           a->incarnation.lo == b->incarnation.lo && a->counter == b->counter;
}

static bool same_log_state(const struct vsr_log_state *a,
                           const struct vsr_log_state *b)
{
    if (a == NULL || b == NULL) {
        return a == b;
    }
    return a->revision.incarnation.hi == b->revision.incarnation.hi &&
           a->revision.incarnation.lo == b->revision.incarnation.lo &&
           a->revision.sequence == b->revision.sequence && a->view == b->view &&
           a->last_normal_view == b->last_normal_view &&
           a->committed == b->committed && a->log_begin == b->log_begin &&
           a->log_end == b->log_end && same_epoch(a->epoch, b->epoch) &&
           same_entries(&a->entries, &b->entries) &&
           same_checkpoint(a->checkpoint, b->checkpoint);
}

static bool same_message(const struct vsr_message *a,
                         const struct vsr_message *b)
{
    if (a->cluster.hi != b->cluster.hi || a->cluster.lo != b->cluster.lo ||
        a->epoch != b->epoch || a->view != b->view || a->from != b->from ||
        a->type != b->type || a->flags != 0 || b->flags != 0 ||
        a->number != b->number) {
        return false;
    }
    switch (a->type) {
    case VSR_MSG_PREPARE: {
        const struct vsr_prepare *x = a->body;
        const struct vsr_prepare *y = b->body;

        return x->committed == y->committed &&
               same_entries(&x->batch, &y->batch);
    }
    case VSR_MSG_DO_VIEW_CHANGE:
    case VSR_MSG_START_VIEW:
        return same_log_state(a->body, b->body);
    case VSR_MSG_RECOVERY:
    case VSR_MSG_RECOVERY_RESPONSE: {
        const struct vsr_recovery *x = a->body;
        const struct vsr_recovery *y = b->body;

        return same_nonce(&x->nonce, &y->nonce) &&
               same_log_state(x->state, y->state);
    }
    case VSR_MSG_GET_STATE:
    case VSR_MSG_GET_LOG: {
        const struct vsr_fetch *x = a->body;
        const struct vsr_fetch *y = b->body;

        return same_nonce(&x->nonce, &y->nonce) &&
               x->revision.incarnation.hi == y->revision.incarnation.hi &&
               x->revision.incarnation.lo == y->revision.incarnation.lo &&
               x->revision.sequence == y->revision.sequence &&
               x->first == y->first && x->end == y->end &&
               x->max_bytes == y->max_bytes &&
               x->max_entries == y->max_entries && x->reserved == 0 &&
               y->reserved == 0;
    }
    case VSR_MSG_NEW_STATE:
    case VSR_MSG_LOG:
    case VSR_MSG_STATE_UNAVAILABLE: {
        const struct vsr_state_chunk *x = a->body;
        const struct vsr_state_chunk *y = b->body;

        return same_nonce(&x->nonce, &y->nonce) &&
               same_log_state(&x->state, &y->state) && x->first == y->first &&
               x->next == y->next;
    }
    case VSR_MSG_START_EPOCH:
    case VSR_MSG_NEW_EPOCH:
        return same_epoch(a->body, b->body);
    case VSR_MSG_CHECKPOINT:
        return same_checkpoint(a->body, b->body);
    case VSR_MSG_READ_PROBE:
    case VSR_MSG_READ_ACK:
        return same_nonce(a->body, b->body);
    default:
        return a->body == NULL && b->body == NULL;
    }
}

static bool same_hard_state(const struct vsr_hard_state *a,
                            const struct vsr_hard_state *b)
{
    return a->view == b->view && a->last_normal_view == b->last_normal_view &&
           a->committed == b->committed && a->state == b->state &&
           a->role == b->role && same_epoch(a->epoch, b->epoch);
}

static bool same_identity(const struct vsr_store_identity *a,
                          const struct vsr_store_identity *b)
{
    return a->cluster.hi == b->cluster.hi && a->cluster.lo == b->cluster.lo &&
           a->replica == b->replica && a->durability == b->durability &&
           a->reserved == 0 && b->reserved == 0;
}

static bool same_client_record(const struct vsr_client_record *a,
                               const struct vsr_client_record *b)
{
    return a->request.client.hi == b->request.client.hi &&
           a->request.client.lo == b->request.client.lo &&
           a->request.number == b->request.number && a->op == b->op &&
           a->result.code == b->result.code && a->result.reserved == 0 &&
           b->result.reserved == 0 &&
           same_blob(&a->result.data, &b->result.data);
}

/* -------------------------------------------------------------------------
 * Encoding and decoding helpers
 * ---------------------------------------------------------------------- */

/* One emit with every resource unbounded: the reference frame. */
static size_t encode_reference(const struct vsr_message *message,
                               uint32_t length, uint32_t crc,
                               unsigned char *out)
{
    struct vsr_io_encoder encoder;
    struct vsr_io_writer writer = {writer_memory, sizeof(writer_memory), 0};
    uint32_t count = 0;
    bool done = false;
    size_t total;

    vsr_io_encoder_begin(&encoder, message, length, crc);
    CHECK(vsr_io_encoder_emit(&encoder, &writer, vectors, MAX_VECTORS, &count,
                              UINT64_MAX, &done) == VSR_OK);
    CHECK(done);
    total = flatten_vectors(vectors, count, out, 0);
    CHECK(total == VSR_IO_FRAME_HEADER_BYTES + length);
    /* Nothing more comes out of a finished encoder. */
    CHECK(vsr_io_encoder_emit(&encoder, &writer, vectors, MAX_VECTORS, &count,
                              UINT64_MAX, &done) == VSR_OK);
    CHECK(done && count == 0);
    return total;
}

/* Emits under the given bounds (0 for random small ones) until done and
 * concatenates the vectors; every call's vectors are checked against the
 * bounds it was given. */
static size_t encode_bounded(const struct vsr_message *message, uint32_t length,
                             uint32_t crc, unsigned char *out,
                             uint32_t vector_capacity, size_t writer_capacity,
                             uint64_t budget)
{
    struct vsr_io_encoder encoder;
    size_t total = 0;
    bool done = false;
    unsigned calls = 0;

    vsr_io_encoder_begin(&encoder, message, length, crc);
    while (!done) {
        uint32_t capacity = vector_capacity != 0 ? vector_capacity : below(6);
        size_t room = writer_capacity != 0 ? writer_capacity : below(65);
        uint64_t allowance = budget != 0 ? budget : below(400);
        struct vsr_io_writer writer = {writer_memory, room, 0};
        uint32_t count = 0;
        uint64_t emitted = 0;

        CHECK(vsr_io_encoder_emit(&encoder, &writer, vectors, capacity, &count,
                                  allowance, &done) == VSR_OK);
        CHECK(count <= capacity);
        CHECK(writer.used <= room);
        for (uint32_t i = 0; i < count; ++i) {
            emitted += vectors[i].length;
        }
        CHECK(emitted <= allowance);
        CHECK(encoder.offset == total + emitted);
        total = flatten_vectors(vectors, count, out, total);
        CHECK(++calls < 4000000u);
    }
    CHECK(total == VSR_IO_FRAME_HEADER_BYTES + length);
    return total;
}

/* Decodes a frame body through a cursor over the given pieces. */
static int decode_pieces(const struct vsr_io_piece *pieces, uint32_t count,
                         const struct vsr_limits *limits,
                         struct vsr_io_bump *region, struct vsr_message **out)
{
    struct vsr_io_cursor cursor;

    vsr_io_cursor_init(&cursor, pieces, count);
    return vsr_io_codec_decode_message(&cursor, limits, region, out);
}

static struct vsr_io_bump make_region(const struct vsr_limits *limits)
{
    struct vsr_io_bump region;
    size_t bytes;

    CHECK(vsr_io_codec_message_region(limits, &bytes) == VSR_OK);
    CHECK(bytes <= REGION_BYTES);
    vsr_io_bump_init(&region, region_memory, bytes);
    return region;
}

/* Byte ranges of the body that decoded blobs point into: a cursor piece
 * boundary inside one is rejected until multi-piece bodies exist. */
struct ranges {
    size_t begin[4096];
    size_t end[4096];
    uint32_t count;
};

static void collect_blob(struct ranges *ranges, const struct vsr_blob *blob,
                         const unsigned char *body)
{
    for (uint32_t i = 0; i < blob->count; ++i) {
        const unsigned char *data = blob->spans[i].data;

        CHECK(ranges->count < 4096);
        ranges->begin[ranges->count] = (size_t)(data - body);
        ranges->end[ranges->count] =
            (size_t)(data - body) + blob->spans[i].size;
        ranges->count++;
    }
}

static void collect_entries(struct ranges *ranges,
                            const struct vsr_entries *entries,
                            const unsigned char *body)
{
    for (uint32_t i = 0; i < entries->count; ++i) {
        if (entries->entries[i].type == VSR_REQUEST_COMMAND) {
            collect_blob(ranges, entries->entries[i].body, body);
        }
    }
}

static void collect_log_state(struct ranges *ranges,
                              const struct vsr_log_state *state,
                              const unsigned char *body)
{
    collect_entries(ranges, &state->entries, body);
    if (state->checkpoint != NULL) {
        collect_blob(ranges, &state->checkpoint->manifest, body);
    }
}

static void collect_ranges(struct ranges *ranges,
                           const struct vsr_message *message,
                           const unsigned char *body)
{
    ranges->count = 0;
    switch (message->type) {
    case VSR_MSG_PREPARE: {
        const struct vsr_prepare *prepare = message->body;

        collect_entries(ranges, &prepare->batch, body);
        break;
    }
    case VSR_MSG_DO_VIEW_CHANGE:
    case VSR_MSG_START_VIEW:
        collect_log_state(ranges, message->body, body);
        break;
    case VSR_MSG_RECOVERY:
    case VSR_MSG_RECOVERY_RESPONSE: {
        const struct vsr_recovery *recovery = message->body;

        if (recovery->state != NULL) {
            collect_log_state(ranges, recovery->state, body);
        }
        break;
    }
    case VSR_MSG_NEW_STATE:
    case VSR_MSG_LOG:
    case VSR_MSG_STATE_UNAVAILABLE: {
        const struct vsr_state_chunk *chunk = message->body;

        collect_log_state(ranges, &chunk->state, body);
        break;
    }
    case VSR_MSG_CHECKPOINT: {
        const struct vsr_checkpoint *checkpoint = message->body;

        collect_blob(ranges, &checkpoint->manifest, body);
        break;
    }
    default:
        break;
    }
}

static bool cut_inside_range(const struct ranges *ranges, size_t cut)
{
    for (uint32_t i = 0; i < ranges->count; ++i) {
        if (cut > ranges->begin[i] && cut < ranges->end[i]) {
            return true;
        }
    }
    return false;
}

/* -------------------------------------------------------------------------
 * Messages
 * ---------------------------------------------------------------------- */

/* One message through every path: reference and bounded encodings agree
 * with each other and the digest, the decoded graph equals the original
 * at every legal split, truncations and corruptions are rejected. */
static void check_message(const struct vsr_message *message,
                          const struct vsr_limits *limits, bool extreme)
{
    struct vsr_io_bump region = make_region(limits);
    struct vsr_io_cursor cursor;
    struct vsr_io_wire_frame header;
    struct vsr_message *decoded = NULL;
    struct ranges ranges;
    uint32_t length;
    uint32_t crc;
    uint32_t body_crc = 0;
    size_t total;
    const unsigned char *body = reference_frame + VSR_IO_FRAME_HEADER_BYTES;

    CHECK(vsr_io_codec_message_digest(message, limits, &length, &crc) ==
          VSR_OK);
    total = encode_reference(message, length, crc, reference_frame);
    CHECK(vsr_io_crc32c(0, body, length) == crc);
    seed_corpus(0, reference_frame, total);

    /* Bounded encodings reproduce the reference byte for byte. */
    for (unsigned round = 0; round < 3; ++round) {
        CHECK(encode_bounded(message, length, crc, random_frame, 0, 0, 0) ==
              total);
        CHECK(memcmp(random_frame, reference_frame, total) == 0);
    }
    if (extreme) {
        CHECK(encode_bounded(message, length, crc, random_frame, 1,
                             VSR_IO_FRAME_HEADER_BYTES, 1) == total);
        CHECK(memcmp(random_frame, reference_frame, total) == 0);
    }

    /* The frame header through a cursor, then the body CRC. */
    vsr_io_cursor_init_one(&cursor, reference_frame, total);
    CHECK(vsr_io_codec_get_frame(&cursor, (uint32_t)total, &header) == VSR_OK);
    CHECK(header.kind == VSR_IO_FRAME_MESSAGE && header.length == length &&
          header.body_crc == crc && cursor.position == 24);
    CHECK(vsr_io_cursor_crc(&cursor, length, &body_crc) && body_crc == crc);

    /* One piece, into a region of exactly the computed size. */
    vsr_io_cursor_init_one(&cursor, body, length);
    CHECK(vsr_io_codec_decode_message(&cursor, limits, &region, &decoded) ==
          VSR_OK);
    CHECK(decoded != NULL && vsr_io_cursor_remaining(&cursor) == 0);
    CHECK(same_message(message, decoded));
    CHECK(region.used <= region.size);
    collect_ranges(&ranges, decoded, body);

    /* Random splits into 1..4 pieces. */
    for (unsigned round = 0; round < 8; ++round) {
        struct vsr_io_piece pieces[VSR_IO_CURSOR_PIECES];
        uint32_t count = 1 + below(VSR_IO_CURSOR_PIECES);
        size_t cuts[VSR_IO_CURSOR_PIECES];
        size_t from = 0;
        bool legal = true;
        int rc;

        for (uint32_t i = 0; i + 1 < count; ++i) {
            cuts[i] = below((uint32_t)length + 1);
        }
        for (uint32_t i = 0; i + 1 < count; ++i) {
            for (uint32_t j = i + 1; j + 1 < count; ++j) {
                if (cuts[j] < cuts[i]) {
                    size_t swap = cuts[i];

                    cuts[i] = cuts[j];
                    cuts[j] = swap;
                }
            }
        }
        for (uint32_t i = 0; i < count; ++i) {
            size_t to = i + 1 < count ? cuts[i] : length;

            pieces[i].base = to > from ? body + from : NULL;
            pieces[i].length = to - from;
            legal = legal && !(i + 1 < count && cut_inside_range(&ranges, to));
            from = to;
        }
        vsr_io_bump_init(&region, region_memory, region.size);
        rc = decode_pieces(pieces, count, limits, &region, &decoded);
        if (legal) {
            CHECK(rc == VSR_OK);
            CHECK(same_message(message, decoded));
        } else {
            CHECK(rc == VSR_EINVAL);
        }
    }

    /* Every truncation fails; none crashes. */
    for (size_t cut = 0; cut < length;
         cut += length > 512 ? 1 + below(97) : 1) {
        vsr_io_bump_init(&region, region_memory, region.size);
        vsr_io_cursor_init_one(&cursor, body, cut);
        CHECK(vsr_io_codec_decode_message(&cursor, limits, &region, &decoded) !=
              VSR_OK);
    }

    /* A flipped byte anywhere is caught by a CRC; the decoder survives it. */
    for (unsigned round = 0; round < 16; ++round) {
        size_t at = below((uint32_t)total);
        unsigned char flip = (unsigned char)(1u << below(8));

        memcpy(random_frame, reference_frame, total);
        random_frame[at] ^= flip;
        vsr_io_cursor_init_one(&cursor, random_frame, total);
        if (at < VSR_IO_FRAME_HEADER_BYTES) {
            CHECK(vsr_io_codec_get_frame(&cursor, (uint32_t)total, &header) ==
                  VSR_EINVAL);
        } else {
            CHECK(vsr_io_codec_get_frame(&cursor, (uint32_t)total, &header) ==
                  VSR_OK);
            body_crc = 0;
            CHECK(vsr_io_cursor_crc(&cursor, length, &body_crc));
            CHECK(body_crc != crc);
            vsr_io_bump_init(&region, region_memory, region.size);
            (void)vsr_io_codec_decode_message(&cursor, limits, &region,
                                              &decoded);
        }
    }
}

static void test_messages(void)
{
    for (size_t set = 0; set < LIMIT_SETS; ++set) {
        const struct vsr_limits *limits = &limit_sets[set];

        for (int mode = 0; mode < 3; ++mode) {
            unsigned rounds = mode == 2 ? 6 : 1;

            for (uint32_t type = 0; type <= VSR_MSG_READ_ACK; ++type) {
                for (unsigned round = 0; round < rounds; ++round) {
                    struct builder b;

                    arena_reset();
                    builder_init(&b, limits, mode, -1);
                    check_message(build_message(&b, type), limits,
                                  set < 2 || mode == 0);
                }
            }
        }
    }
}

/* A hand-written encoding pins the layout of wire.h independently of the
 * codec's own walk. */
static void test_layout(void)
{
    static const unsigned char command[3] = {'a', 'b', 'c'};
    struct vsr_span span = {command, sizeof(command)};
    struct vsr_blob blob = {&span, sizeof(command), 1, 0};
    struct vsr_member members[2] = {{10, VSR_MEMBER_FULL, 0},
                                    {20, VSR_MEMBER_WITNESS, 0}};
    struct vsr_membership membership = {2, members, 2, 0};
    struct vsr_check_epoch check = {5};
    struct vsr_entry entries[4] = {
        {30, 1, 2, {{100, 101}, 7}, VSR_REQUEST_COMMAND, 0, &blob},
        {31, 1, 2, {{100, 102}, 8}, VSR_REQUEST_RECONFIGURE, 0, &membership},
        {32, 1, 2, {{100, 103}, 9}, VSR_REQUEST_CHECK_EPOCH, 0, &check},
        {33, 1, 2, {{0, 0}, 0}, VSR_REQUEST_NOOP, 0, NULL},
    };
    struct vsr_prepare prepare = {{entries, 4, 0}, 29};
    struct vsr_message message = {{1, 2},          3, 4,  5,
                                  VSR_MSG_PREPARE, 0, 33, &prepare};
    struct vsr_nonce nonce = {{7, 8}, 9};
    struct vsr_message probe = {{1, 2}, 3, 4,     5, VSR_MSG_READ_PROBE,
                                0,      6, &nonce};
    unsigned char expected[512];
    unsigned char *at = expected;
    size_t length;
    uint32_t digest_length;
    uint32_t digest_crc;
    const struct vsr_limits *limits = &limit_sets[1];

    /* READ_PROBE: envelope then nonce, 80 bytes. */
    vsr_io_put_u64(at, 1);
    vsr_io_put_u64(at + 8, 2);
    vsr_io_put_u64(at + 16, 3);
    vsr_io_put_u64(at + 24, 4);
    vsr_io_put_u64(at + 32, 5);
    vsr_io_put_u32(at + 40, VSR_MSG_READ_PROBE);
    vsr_io_put_u32(at + 44, 0);
    vsr_io_put_u64(at + 48, 6);
    vsr_io_put_u64(at + 56, 7);
    vsr_io_put_u64(at + 64, 8);
    vsr_io_put_u64(at + 72, 9);
    CHECK(vsr_io_codec_message_digest(&probe, limits, &digest_length,
                                      &digest_crc) == VSR_OK);
    CHECK(digest_length == 80);
    CHECK(digest_crc == vsr_io_crc32c(0, expected, 80));
    length =
        encode_reference(&probe, digest_length, digest_crc, reference_frame);
    CHECK(length == 104);
    CHECK(memcmp(reference_frame + 24, expected, 80) == 0);
    /* The frame header: magic, version, kind, length, CRCs, reserved. */
    CHECK(memcmp(reference_frame, "FRM1", 4) == 0);
    CHECK(vsr_io_get_u32(reference_frame + 4) ==
          (VSR_IO_WIRE_VERSION | (uint32_t)VSR_IO_FRAME_MESSAGE << 16));
    CHECK(vsr_io_get_u32(reference_frame + 8) == 80);
    CHECK(vsr_io_get_u32(reference_frame + 12) == digest_crc);
    CHECK(vsr_io_get_u32(reference_frame + 16) ==
          vsr_io_crc32c(0, reference_frame, 16));
    CHECK(vsr_io_get_u32(reference_frame + 20) == 0);

    /* PREPARE with one entry of each type. */
    at = expected;
    vsr_io_put_u64(at, 1);
    vsr_io_put_u64(at + 8, 2);
    vsr_io_put_u64(at + 16, 3);
    vsr_io_put_u64(at + 24, 4);
    vsr_io_put_u64(at + 32, 5);
    vsr_io_put_u32(at + 40, VSR_MSG_PREPARE);
    vsr_io_put_u32(at + 44, 0);
    vsr_io_put_u64(at + 48, 33);
    at += 56;
    vsr_io_put_u64(at, 29);    /* prepare.committed */
    vsr_io_put_u32(at + 8, 4); /* entries.count */
    vsr_io_put_u32(at + 12, 0);
    at += 16;
    for (uint32_t i = 0; i < 4; ++i) {
        static const uint32_t body_lengths[4] = {16, 48, 8, 0};

        vsr_io_put_u64(at, entries[i].op);
        vsr_io_put_u64(at + 8, entries[i].epoch);
        vsr_io_put_u64(at + 16, entries[i].view);
        vsr_io_put_u64(at + 24, entries[i].request.client.hi);
        vsr_io_put_u64(at + 32, entries[i].request.client.lo);
        vsr_io_put_u64(at + 40, entries[i].request.number);
        vsr_io_put_u32(at + 48, entries[i].type);
        vsr_io_put_u32(at + 52, body_lengths[i]);
        at += 56;
        switch (i) {
        case 0:
            vsr_io_put_u64(at, 3);
            /* NOLINTNEXTLINE(bugprone-not-null-terminated-result): raw */
            memcpy(at + 8, "abc\0\0\0\0\0", 8);
            at += 16;
            break;
        case 1:
            vsr_io_put_u64(at, 2);
            vsr_io_put_u32(at + 8, 2);
            vsr_io_put_u32(at + 12, 0);
            vsr_io_put_u64(at + 16, 10);
            vsr_io_put_u32(at + 24, VSR_MEMBER_FULL);
            vsr_io_put_u32(at + 28, 0);
            vsr_io_put_u64(at + 32, 20);
            vsr_io_put_u32(at + 40, VSR_MEMBER_WITNESS);
            vsr_io_put_u32(at + 44, 0);
            at += 48;
            break;
        case 2:
            vsr_io_put_u64(at, 5);
            at += 8;
            break;
        default:
            break;
        }
    }
    length = (size_t)(at - expected);
    CHECK(length == 56 + 16 + 4 * 56 + 16 + 48 + 8);
    CHECK(vsr_io_codec_message_digest(&message, limits, &digest_length,
                                      &digest_crc) == VSR_OK);
    CHECK(digest_length == length);
    CHECK(digest_crc == vsr_io_crc32c(0, expected, length));
    CHECK(encode_reference(&message, digest_length, digest_crc,
                           reference_frame) == 24 + length);
    CHECK(memcmp(reference_frame + 24, expected, length) == 0);
    check_message(&message, limits, true);
    check_message(&probe, limits, true);

    /* The same entries inside an APPEND record encode identically. */
    {
        struct vsr_change change = {VSR_STORE_APPEND, 4, 30, entries};
        struct vsr_store transaction = {1, &change, 1, 0};
        size_t written;

        CHECK(vsr_io_codec_put_record(&transaction, 1, 1, 0, random_frame,
                                      sizeof(random_frame),
                                      &written) == VSR_OK);
        CHECK(written == 48 + 24 + (length - 56 - 16));
        CHECK(memcmp(random_frame + 72, expected + 72, length - 72) == 0);
    }
}

/* Rejections of frame headers and the small fixed bodies. */
static void test_frames(void)
{
    unsigned char bytes[64];
    struct vsr_io_cursor cursor;
    struct vsr_io_wire_frame frame;
    struct vsr_io_wire_hello hello;
    struct vsr_io_wire_stream_end end;
    struct vsr_io_wire_library_request request = {
        0, 0, VSR_IO_LIBRARY_CLIENTS, 1, 2, 3, 4, 5};
    struct vsr_io_wire_library_request decoded;
    struct vsr_span span;
    uint64_t offset;

    vsr_io_codec_put_frame(bytes, VSR_IO_FRAME_HELLO, 32, 0x12345678u);
    vsr_io_cursor_init_one(&cursor, bytes, 24);
    CHECK(vsr_io_codec_get_frame(&cursor, 32, &frame) == VSR_OK);
    CHECK(frame.magic == VSR_IO_FRAME_MAGIC && frame.version == 1 &&
          frame.kind == VSR_IO_FRAME_HELLO && frame.length == 32 &&
          frame.body_crc == 0x12345678u && frame.reserved == 0);
    CHECK(vsr_io_cursor_remaining(&cursor) == 0);
    /* Too small a limit, a short header, every flipped bit. */
    vsr_io_cursor_init_one(&cursor, bytes, 24);
    CHECK(vsr_io_codec_get_frame(&cursor, 24, &frame) == VSR_EINVAL);
    CHECK(cursor.position == 0);
    vsr_io_cursor_init_one(&cursor, bytes, 23);
    CHECK(vsr_io_codec_get_frame(&cursor, 32, &frame) == VSR_EINVAL);
    for (unsigned bit = 0; bit < 24 * 8; ++bit) {
        unsigned char copy[24];

        memcpy(copy, bytes, 24);
        copy[bit / 8] ^= (unsigned char)(1u << (bit % 8));
        vsr_io_cursor_init_one(&cursor, copy, 24);
        CHECK(vsr_io_codec_get_frame(&cursor, 32, &frame) == VSR_EINVAL);
    }
    /* A valid CRC over a bad kind, version or length still fails. */
    for (unsigned variant = 0; variant < 5; ++variant) {
        unsigned char copy[24];

        memcpy(copy, bytes, 24);
        switch (variant) {
        case 0:
            vsr_io_put_u32(copy + 4, 1u); /* version 1, kind 0 */
            break;
        case 1:
            vsr_io_put_u32(copy + 4, 1u | 6u << 16); /* kind 6 */
            break;
        case 2:
            vsr_io_put_u32(copy + 4, 2u | 1u << 16); /* version 2 */
            break;
        case 3:
            vsr_io_put_u32(copy + 8, 36); /* not a multiple of 8 */
            break;
        default:
            /* NOLINTNEXTLINE(bugprone-not-null-terminated-result): tag */
            memcpy(copy, "FRM2", 4);
            break;
        }
        vsr_io_put_u32(copy + 16, vsr_io_crc32c(0, copy, 16));
        vsr_io_cursor_init_one(&cursor, copy, 24);
        CHECK(vsr_io_codec_get_frame(&cursor, 64, &frame) == VSR_EINVAL);
    }
    /* Split header. */
    for (size_t cut = 0; cut <= 24; ++cut) {
        struct vsr_io_piece pieces[2] = {{bytes, cut}, {bytes + cut, 24 - cut}};

        vsr_io_cursor_init(&cursor, pieces, 2);
        CHECK(vsr_io_codec_get_frame(&cursor, 32, &frame) == VSR_OK);
        CHECK(frame.length == 32);
    }

    /* HELLO. */
    vsr_io_codec_put_hello(bytes, VSR_IO_HANDSHAKE_TRUSTED,
                           VSR_IO_PURPOSE_STREAM, 77, 0xabcdefu);
    vsr_io_cursor_init_one(&cursor, bytes, 32);
    CHECK(vsr_io_codec_get_hello(&cursor, &hello) == VSR_OK);
    CHECK(hello.handshake == VSR_IO_HANDSHAKE_TRUSTED &&
          hello.purpose == VSR_IO_PURPOSE_STREAM && hello.node == 77 &&
          hello.nonce == 0xabcdefu && hello.reserved == 0);
    vsr_io_cursor_init_one(&cursor, bytes, 31);
    CHECK(vsr_io_codec_get_hello(&cursor, &hello) == VSR_EINVAL);
    vsr_io_cursor_init_one(&cursor, bytes, 40);
    CHECK(vsr_io_codec_get_hello(&cursor, &hello) == VSR_EINVAL);
    vsr_io_codec_put_hello(bytes, VSR_IO_HANDSHAKE_TRUSTED, 3, 77, 1);
    vsr_io_cursor_init_one(&cursor, bytes, 32);
    CHECK(vsr_io_codec_get_hello(&cursor, &hello) == VSR_EINVAL);
    vsr_io_codec_put_hello(bytes, 3, VSR_IO_PURPOSE_PEER, 77, 1);
    vsr_io_cursor_init_one(&cursor, bytes, 32);
    CHECK(vsr_io_codec_get_hello(&cursor, &hello) == VSR_EINVAL);
    vsr_io_codec_put_hello(bytes, 0, VSR_IO_PURPOSE_PEER, 77, 1);
    bytes[24] = 1;
    vsr_io_cursor_init_one(&cursor, bytes, 32);
    CHECK(vsr_io_codec_get_hello(&cursor, &hello) == VSR_EINVAL);

    /* Stream request: header, 5 bytes, zero padding. */
    memset(bytes, 0, sizeof(bytes));
    vsr_io_codec_put_stream_request(bytes, 5);
    memcpy(bytes + 8, "hello", 5);
    vsr_io_cursor_init_one(&cursor, bytes, 16);
    CHECK(vsr_io_codec_get_stream_request(&cursor, &span) == VSR_OK);
    CHECK(span.size == 5 && span.data == bytes + 8);
    CHECK(vsr_io_cursor_remaining(&cursor) == 0);
    bytes[14] = 1; /* padding */
    vsr_io_cursor_init_one(&cursor, bytes, 16);
    CHECK(vsr_io_codec_get_stream_request(&cursor, &span) == VSR_EINVAL);
    bytes[14] = 0;
    vsr_io_cursor_init_one(&cursor, bytes, 24);
    CHECK(vsr_io_codec_get_stream_request(&cursor, &span) == VSR_EINVAL);
    vsr_io_cursor_init_one(&cursor, bytes, 8);
    CHECK(vsr_io_codec_get_stream_request(&cursor, &span) == VSR_EINVAL);
    vsr_io_codec_put_stream_request(bytes, VSR_IO_STREAM_REQUEST_BYTES + 1);
    vsr_io_cursor_init_one(&cursor, bytes, 16);
    CHECK(vsr_io_codec_get_stream_request(&cursor, &span) == VSR_EINVAL);
    vsr_io_codec_put_stream_request(bytes, 0);
    vsr_io_cursor_init_one(&cursor, bytes, 8);
    CHECK(vsr_io_codec_get_stream_request(&cursor, &span) == VSR_OK);
    CHECK(span.size == 0 && span.data == NULL);
    {
        /* A request split across pieces is not one span. */
        struct vsr_io_piece pieces[2] = {{bytes, 10}, {bytes + 10, 6}};

        vsr_io_codec_put_stream_request(bytes, 5);
        vsr_io_cursor_init(&cursor, pieces, 2);
        CHECK(vsr_io_codec_get_stream_request(&cursor, &span) == VSR_EINVAL);
    }

    /* Stream chunk. */
    memset(bytes, 0, sizeof(bytes));
    vsr_io_codec_put_stream_chunk(bytes, 1000, 9);
    memcpy(bytes + 16, "chunkdata", 9);
    vsr_io_cursor_init_one(&cursor, bytes, 32);
    CHECK(vsr_io_codec_get_stream_chunk(&cursor, &offset, &span) == VSR_OK);
    CHECK(offset == 1000 && span.size == 9 && span.data == bytes + 16);
    vsr_io_cursor_init_one(&cursor, bytes, 30);
    CHECK(vsr_io_codec_get_stream_chunk(&cursor, &offset, &span) == VSR_EINVAL);
    bytes[12] = 1; /* reserved */
    vsr_io_cursor_init_one(&cursor, bytes, 32);
    CHECK(vsr_io_codec_get_stream_chunk(&cursor, &offset, &span) == VSR_EINVAL);

    /* Stream end. */
    vsr_io_codec_put_stream_end(bytes, 12345, VSR_IO_FAILED);
    vsr_io_cursor_init_one(&cursor, bytes, 16);
    CHECK(vsr_io_codec_get_stream_end(&cursor, &end) == VSR_OK);
    CHECK(end.bytes == 12345 && end.status == VSR_IO_FAILED &&
          end.reserved == 0);
    vsr_io_codec_put_stream_end(bytes, 12345, -1);
    vsr_io_cursor_init_one(&cursor, bytes, 16);
    CHECK(vsr_io_codec_get_stream_end(&cursor, &end) == VSR_EINVAL);
    vsr_io_codec_put_stream_end(bytes, 12345, VSR_IO_CANCELLED + 1);
    vsr_io_cursor_init_one(&cursor, bytes, 16);
    CHECK(vsr_io_codec_get_stream_end(&cursor, &end) == VSR_EINVAL);

    /* Library request. */
    vsr_io_codec_put_library_request(bytes, &request);
    span.data = bytes;
    span.size = 56;
    CHECK(vsr_io_codec_get_library_request(&span, &decoded) == VSR_OK);
    CHECK(decoded.magic == VSR_IO_LIBRARY_MAGIC &&
          decoded.version == VSR_IO_LIBRARY_REQUEST_VERSION &&
          decoded.kind == VSR_IO_LIBRARY_CLIENTS && decoded.cluster_hi == 1 &&
          decoded.cluster_lo == 2 && decoded.replica == 3 &&
          decoded.snapshot_hi == 4 && decoded.snapshot_lo == 5);
    span.size = 55;
    CHECK(vsr_io_codec_get_library_request(&span, &decoded) == VSR_EINVAL);
    span.size = 57;
    CHECK(vsr_io_codec_get_library_request(&span, &decoded) == VSR_EINVAL);
    span.size = 56;
    bytes[0] ^= 1;
    CHECK(vsr_io_codec_get_library_request(&span, &decoded) == VSR_EINVAL);
    bytes[0] ^= 1;
    vsr_io_put_u32(bytes + 12, 2); /* kind */
    CHECK(vsr_io_codec_get_library_request(&span, &decoded) == VSR_EINVAL);
    seed_corpus(0, bytes, 0);
}

/* -------------------------------------------------------------------------
 * Records
 * ---------------------------------------------------------------------- */

struct transaction {
    struct vsr_store store;
    struct vsr_change changes[VSR_MAX_STORE_CHANGES];
    const struct vsr_entry *entries;
    const struct vsr_client_record *clients;
    const struct vsr_hard_state *hard;
    const struct vsr_checkpoint *publish;
    const struct vsr_checkpoint *restore;
    const struct vsr_store_identity *identity;
};

/* Builds the changes selected by mask, in the canonical order. */
static void build_transaction(struct builder *b, unsigned mask,
                              struct transaction *t)
{
    uint32_t count = 0;

    memset(t, 0, sizeof(*t));
    if (mask & 1u) {
        t->identity = build_identity(b);
        t->changes[count++] =
            (struct vsr_change){VSR_STORE_IDENTITY, 1, 0, t->identity};
    }
    if (mask & 2u) {
        uint32_t n = 1 + pick(b, b->limits->batch_entries - 1);

        b->payload = b->limits->message_bytes;
        t->entries = build_entry_array(b, n);
        t->changes[count++] =
            (struct vsr_change){VSR_STORE_APPEND, n, pick64(b), t->entries};
    }
    if (mask & 4u) {
        t->changes[count++] =
            (struct vsr_change){VSR_STORE_TRUNCATE, 0, pick64(b), NULL};
    }
    if (mask & 8u) {
        uint32_t n = 1 + pick(b, b->limits->batch_entries - 1);
        struct vsr_client_record *records =
            NEW_ARRAY(struct vsr_client_record, n);

        for (uint32_t i = 0; i < n; ++i) {
            build_client_record(b, &records[i]);
        }
        t->clients = records;
        t->changes[count++] =
            (struct vsr_change){VSR_STORE_CLIENTS, n, 0, records};
    }
    if (mask & 16u) {
        t->hard = build_hard_state(b);
        t->changes[count++] =
            (struct vsr_change){VSR_STORE_HARD_STATE, 1, 0, t->hard};
    }
    if (mask & 32u) {
        b->payload = UINT64_MAX;
        t->publish = build_checkpoint(b);
        t->changes[count++] =
            (struct vsr_change){VSR_STORE_PUBLISH_CHECKPOINT, 1, 0, t->publish};
    }
    if (mask & 64u) {
        b->payload = UINT64_MAX;
        t->restore = build_checkpoint(b);
        t->changes[count++] =
            (struct vsr_change){VSR_STORE_RESTORE_CHECKPOINT, 1, 0, t->restore};
    }
    if (mask & 128u) {
        t->changes[count++] =
            (struct vsr_change){VSR_STORE_TRIM, 0, pick64(b), NULL};
    }
    t->store.sequence = pick64(b);
    t->store.changes = t->changes;
    t->store.count = count;
    t->store.reserved = 0;
}

/* Decodes one change payload at the cursor and compares it with the
 * original; the region is a load region of exactly the computed size. */
static void check_change(const struct transaction *t,
                         const struct vsr_change *change,
                         const struct vsr_io_wire_change *wire,
                         const unsigned char *record,
                         const struct vsr_limits *limits)
{
    struct vsr_io_cursor cursor;
    struct vsr_io_bump region;
    size_t region_bytes;

    CHECK(vsr_io_codec_load_region(limits, &region_bytes) == VSR_OK);
    CHECK(region_bytes <= REGION_BYTES);
    vsr_io_bump_init(&region, region_memory, region_bytes);
    CHECK(wire->type == change->type && wire->count == change->count &&
          wire->first == change->first);
    vsr_io_cursor_init_one(&cursor, record + wire->offset, wire->length);
    switch (change->type) {
    case VSR_STORE_APPEND: {
        struct vsr_entry *entries = NULL;
        struct vsr_entry one;

        CHECK(vsr_io_codec_get_entries(&cursor, wire->count, limits, &region,
                                       &entries) == VSR_OK);
        CHECK(vsr_io_cursor_remaining(&cursor) == 0);
        CHECK(same_entry_array(t->entries, entries, wire->count));
        for (uint32_t i = 0; i < wire->count; ++i) {
            vsr_io_bump_init(&region, region_memory, region_bytes);
            vsr_io_cursor_init_one(&cursor, record + wire->offset,
                                   wire->length);
            CHECK(vsr_io_codec_get_entry_at(&cursor, i, limits, &region,
                                            &one) == VSR_OK);
            CHECK(same_entry(&t->entries[i], &one));
        }
        vsr_io_cursor_init_one(&cursor, record + wire->offset, wire->length);
        CHECK(vsr_io_codec_get_entry_at(&cursor, wire->count, limits, &region,
                                        &one) == VSR_EINVAL);
        break;
    }
    case VSR_STORE_CLIENTS:
        for (uint32_t i = 0; i < wire->count; ++i) {
            struct vsr_client_record decoded;
            struct vsr_io_cursor skip = cursor;
            struct vsr_io_wire_client_record fixed;

            CHECK(vsr_io_codec_skip_client_record(&skip, &fixed) == VSR_OK);
            CHECK(fixed.client_hi == t->clients[i].request.client.hi &&
                  fixed.number == t->clients[i].request.number &&
                  fixed.op == t->clients[i].op &&
                  fixed.length == t->clients[i].result.data.size);
            vsr_io_bump_init(&region, region_memory, region_bytes);
            CHECK(vsr_io_codec_get_client_record(&cursor, limits, &region,
                                                 &decoded) == VSR_OK);
            CHECK(same_client_record(&t->clients[i], &decoded));
            CHECK(skip.position == cursor.position);
        }
        CHECK(vsr_io_cursor_remaining(&cursor) == 0);
        break;
    case VSR_STORE_HARD_STATE: {
        struct vsr_hard_state hard;

        CHECK(vsr_io_codec_get_hard_state(&cursor, limits, &region, &hard) ==
              VSR_OK);
        CHECK(vsr_io_cursor_remaining(&cursor) == 0);
        CHECK(same_hard_state(t->hard, &hard));
        break;
    }
    case VSR_STORE_PUBLISH_CHECKPOINT:
    case VSR_STORE_RESTORE_CHECKPOINT: {
        struct vsr_checkpoint *checkpoint = NULL;

        CHECK(vsr_io_codec_get_checkpoint(&cursor, limits, &region,
                                          &checkpoint) == VSR_OK);
        CHECK(vsr_io_cursor_remaining(&cursor) == 0);
        CHECK(same_checkpoint(change->data, checkpoint));
        break;
    }
    case VSR_STORE_IDENTITY: {
        struct vsr_store_identity identity;

        CHECK(vsr_io_codec_get_identity(&cursor, &identity) == VSR_OK);
        CHECK(vsr_io_cursor_remaining(&cursor) == 0);
        CHECK(same_identity(t->identity, &identity));
        break;
    }
    default:
        CHECK(wire->length == 0);
        break;
    }
}

static void check_record(const struct transaction *t,
                         const struct vsr_limits *limits)
{
    struct vsr_io_cursor cursor;
    struct vsr_io_wire_record header;
    struct vsr_io_wire_change changes[VSR_MAX_STORE_CHANGES];
    uint64_t generation = random64();
    uint64_t flushed = random64();
    uint32_t run = test_random_next(&rng);
    uint64_t limit;
    uint32_t kind;
    size_t bytes;
    size_t written;
    size_t expected_offset;

    CHECK(vsr_io_codec_record_limit(limits, &limit) == VSR_OK);
    CHECK(vsr_io_codec_record_bytes(&t->store, limits, &bytes) == VSR_OK);
    CHECK(bytes <= limit);
    CHECK(vsr_io_codec_put_record(&t->store, generation, run, flushed,
                                  reference_frame, sizeof(reference_frame),
                                  &written) == VSR_OK);
    CHECK(written == bytes);
    CHECK(written % 8 == 0);
    seed_corpus(1, reference_frame, written);
    /* Too small a buffer. */
    CHECK(vsr_io_codec_put_record(&t->store, generation, run, flushed,
                                  random_frame, bytes - 1,
                                  &written) == VSR_ELIMIT);

    vsr_io_cursor_init_one(&cursor, reference_frame, written);
    CHECK(vsr_io_codec_get_record(&cursor, limit, &header, &kind) == VSR_OK);
    CHECK(kind == VSR_IO_SCAN_RECORD);
    CHECK(header.magic == VSR_IO_RECORD_MAGIC && header.length == written &&
          header.sequence == t->store.sequence &&
          header.generation == generation && header.flushed == flushed &&
          header.count == t->store.count && header.run == run);
    CHECK(cursor.position == 48);
    CHECK(vsr_io_codec_check_record(&cursor, &header));
    expected_offset = 48 + 24 * t->store.count;
    for (uint32_t i = 0; i < t->store.count; ++i) {
        CHECK(vsr_io_codec_get_change(&cursor, &changes[i]) == VSR_OK);
        CHECK(changes[i].offset == expected_offset);
        expected_offset += changes[i].length;
    }
    CHECK(expected_offset == written);
    for (uint32_t i = 0; i < t->store.count; ++i) {
        check_change(t, &t->changes[i], &changes[i], reference_frame, limits);
    }

    /* Below the limit is EINVAL, not END: the header is well formed. */
    vsr_io_cursor_init_one(&cursor, reference_frame, written);
    CHECK(vsr_io_codec_get_record(&cursor, written - 8, &header, &kind) ==
          VSR_EINVAL);
    CHECK(cursor.position == 0);
    /* Short: fewer than 48 bytes is END; a whole header with a short
     * payload passes get_record and fails check_record. */
    vsr_io_cursor_init_one(&cursor, reference_frame, 47);
    CHECK(vsr_io_codec_get_record(&cursor, limit, &header, &kind) == VSR_OK);
    CHECK(kind == VSR_IO_SCAN_END && cursor.position == 0);
    vsr_io_cursor_init_one(&cursor, reference_frame, written - 8);
    CHECK(vsr_io_codec_get_record(&cursor, limit, &header, &kind) == VSR_OK);
    CHECK(kind == VSR_IO_SCAN_RECORD);
    CHECK(!vsr_io_codec_check_record(&cursor, &header));
    /* Flips: in the header EINVAL or END, in the payload a CRC mismatch. */
    for (unsigned round = 0; round < 24; ++round) {
        size_t at = round < 8 ? below(48) : below((uint32_t)written);
        int rc;

        memcpy(random_frame, reference_frame, written);
        random_frame[at] ^= (unsigned char)(1u << below(8));
        vsr_io_cursor_init_one(&cursor, random_frame, written);
        rc = vsr_io_codec_get_record(&cursor, limit, &header, &kind);
        if (at < 4) {
            CHECK(rc == VSR_OK && kind == VSR_IO_SCAN_END);
        } else if (at < 48) {
            CHECK(rc == VSR_EINVAL);
        } else {
            CHECK(rc == VSR_OK && kind == VSR_IO_SCAN_RECORD);
            CHECK(!vsr_io_codec_check_record(&cursor, &header));
        }
    }
    /* A header whose length is not a multiple of 8, with a valid CRC. */
    memcpy(random_frame, reference_frame, written);
    vsr_io_put_u32(random_frame + 4, (uint32_t)written + 4);
    vsr_io_put_u32(random_frame + 44, vsr_io_crc32c(0, random_frame, 44));
    vsr_io_cursor_init_one(&cursor, random_frame, written);
    CHECK(vsr_io_codec_get_record(&cursor, limit, &header, &kind) ==
          VSR_EINVAL);
    /* Too many descriptors for the length, or none at all. */
    memcpy(random_frame, reference_frame, written);
    vsr_io_put_u32(random_frame + 32, (uint32_t)(written - 48) / 24 + 1);
    vsr_io_put_u32(random_frame + 44, vsr_io_crc32c(0, random_frame, 44));
    vsr_io_cursor_init_one(&cursor, random_frame, written);
    CHECK(vsr_io_codec_get_record(&cursor, limit, &header, &kind) ==
          VSR_EINVAL);
    vsr_io_put_u32(random_frame + 32, 0);
    vsr_io_put_u32(random_frame + 44, vsr_io_crc32c(0, random_frame, 44));
    vsr_io_cursor_init_one(&cursor, random_frame, written);
    CHECK(vsr_io_codec_get_record(&cursor, limit, &header, &kind) ==
          VSR_EINVAL);
}

static void test_records(void)
{
    for (size_t set = 0; set < LIMIT_SETS; ++set) {
        const struct vsr_limits *limits = &limit_sets[set];

        for (unsigned mask = 1; mask < 256; ++mask) {
            for (int mode = 0; mode < 3; ++mode) {
                struct builder b;
                struct transaction t;

                arena_reset();
                builder_init(&b, limits, mode, -1);
                build_transaction(&b, mask, &t);
                check_record(&t, limits);
            }
        }
    }
}

/* PAD, zero fill and foreign magic; the rejections of get_change and of
 * malformed transactions. */
static void test_scanning(void)
{
    unsigned char bytes[64];
    struct vsr_io_cursor cursor;
    struct vsr_io_wire_record header;
    struct vsr_io_wire_change change;
    uint32_t kind = 99;

    vsr_io_codec_put_pad(bytes, 512);
    vsr_io_cursor_init_one(&cursor, bytes, 8);
    CHECK(vsr_io_codec_get_record(&cursor, 4096, &header, &kind) == VSR_OK);
    CHECK(kind == VSR_IO_SCAN_PAD && header.length == 512 &&
          header.magic == VSR_IO_PAD_MAGIC && cursor.position == 8);
    vsr_io_codec_put_pad(bytes, 4);
    vsr_io_cursor_init_one(&cursor, bytes, 8);
    CHECK(vsr_io_codec_get_record(&cursor, 4096, &header, &kind) == VSR_EINVAL);
    vsr_io_codec_put_pad(bytes, 12);
    vsr_io_cursor_init_one(&cursor, bytes, 8);
    CHECK(vsr_io_codec_get_record(&cursor, 4096, &header, &kind) == VSR_EINVAL);
    vsr_io_cursor_init_one(&cursor, bytes, 6);
    CHECK(vsr_io_codec_get_record(&cursor, 4096, &header, &kind) == VSR_OK);
    CHECK(kind == VSR_IO_SCAN_END);

    memset(bytes, 0, sizeof(bytes));
    vsr_io_cursor_init_one(&cursor, bytes, sizeof(bytes));
    CHECK(vsr_io_codec_get_record(&cursor, 4096, &header, &kind) == VSR_OK);
    CHECK(kind == VSR_IO_SCAN_END && cursor.position == 0);
    vsr_io_cursor_init_one(&cursor, bytes, 3);
    CHECK(vsr_io_codec_get_record(&cursor, 4096, &header, &kind) == VSR_OK);
    CHECK(kind == VSR_IO_SCAN_END);
    vsr_io_cursor_init_one(&cursor, bytes, 0);
    CHECK(vsr_io_codec_get_record(&cursor, 4096, &header, &kind) == VSR_OK);
    CHECK(kind == VSR_IO_SCAN_END);
    /* NOLINTNEXTLINE(bugprone-not-null-terminated-result): tag */
    memcpy(bytes, "SEG1", 4);
    vsr_io_cursor_init_one(&cursor, bytes, sizeof(bytes));
    CHECK(vsr_io_codec_get_record(&cursor, 4096, &header, &kind) == VSR_OK);
    CHECK(kind == VSR_IO_SCAN_END);

    /* Change descriptors: type, alignment and shape. */
    for (unsigned variant = 0; variant < 12; ++variant) {
        uint32_t type = VSR_STORE_APPEND;
        uint32_t count = 1;
        uint32_t offset = 72;
        uint32_t length = 64;
        int expected = VSR_OK;

        switch (variant) {
        case 1:
            type = VSR_STORE_IDENTITY + 1;
            expected = VSR_EINVAL;
            break;
        case 2:
            offset = 76;
            expected = VSR_EINVAL;
            break;
        case 3:
            length = 60;
            expected = VSR_EINVAL;
            break;
        case 4:
            offset = 40;
            expected = VSR_EINVAL;
            break;
        case 5:
            count = 0;
            expected = VSR_EINVAL;
            break;
        case 6:
            type = VSR_STORE_TRUNCATE;
            count = 0;
            length = 0;
            break;
        case 7:
            type = VSR_STORE_TRIM;
            count = 0;
            expected = VSR_EINVAL; /* length 64 */
            break;
        case 8:
            type = VSR_STORE_HARD_STATE;
            count = 2;
            expected = VSR_EINVAL;
            break;
        case 9:
            type = VSR_STORE_RESTORE_CHECKPOINT;
            break;
        case 10:
            offset = UINT32_MAX - 7;
            expected = VSR_EINVAL;
            break;
        default:
            break;
        }
        vsr_io_put_u32(bytes, type);
        vsr_io_put_u32(bytes + 4, count);
        vsr_io_put_u64(bytes + 8, 5);
        vsr_io_put_u32(bytes + 16, offset);
        vsr_io_put_u32(bytes + 20, length);
        vsr_io_cursor_init_one(&cursor, bytes, 24);
        CHECK(vsr_io_codec_get_change(&cursor, &change) == expected);
        if (expected == VSR_OK) {
            CHECK(change.type == type && change.count == count &&
                  change.first == 5 && change.offset == offset &&
                  change.length == length && cursor.position == 24);
        } else {
            CHECK(cursor.position == 0);
        }
    }
    vsr_io_cursor_init_one(&cursor, bytes, 23);
    CHECK(vsr_io_codec_get_change(&cursor, &change) == VSR_EINVAL);

    /* Malformed transactions. */
    {
        const struct vsr_limits *limits = &limit_sets[1];
        struct vsr_store_identity identity = {{1, 2}, 3, 0, 0};
        struct vsr_change good = {VSR_STORE_IDENTITY, 1, 0, &identity};
        struct vsr_change changes[VSR_MAX_STORE_CHANGES + 1];
        struct vsr_store store = {1, &good, 1, 0};
        size_t size;

        CHECK(vsr_io_codec_record_bytes(&store, limits, &size) == VSR_OK);
        CHECK(size == 48 + 24 + 32);
        store.count = 0;
        CHECK(vsr_io_codec_record_bytes(&store, limits, &size) == VSR_EINVAL);
        for (uint32_t i = 0; i <= VSR_MAX_STORE_CHANGES; ++i) {
            changes[i] = good;
        }
        store.changes = changes;
        store.count = VSR_MAX_STORE_CHANGES + 1;
        CHECK(vsr_io_codec_record_bytes(&store, limits, &size) == VSR_EINVAL);
        store.count = VSR_MAX_STORE_CHANGES;
        CHECK(vsr_io_codec_record_bytes(&store, limits, &size) == VSR_OK);
        store.reserved = 1;
        CHECK(vsr_io_codec_record_bytes(&store, limits, &size) == VSR_EINVAL);
        store.reserved = 0;
        store.changes = &good;
        store.count = 1;
        good.count = 2;
        CHECK(vsr_io_codec_record_bytes(&store, limits, &size) == VSR_EINVAL);
        good.count = 1;
        good.data = NULL;
        CHECK(vsr_io_codec_record_bytes(&store, limits, &size) == VSR_EINVAL);
        good.data = &identity;
        good.type = VSR_STORE_TRUNCATE;
        CHECK(vsr_io_codec_record_bytes(&store, limits, &size) == VSR_EINVAL);
        good.type = VSR_STORE_IDENTITY + 1;
        CHECK(vsr_io_codec_record_bytes(&store, limits, &size) == VSR_EINVAL);
        good.type = VSR_STORE_IDENTITY;
        identity.reserved = 1;
        CHECK(vsr_io_codec_record_bytes(&store, limits, &size) == VSR_EINVAL);
        identity.reserved = 0;
        CHECK(vsr_io_codec_put_record(&store, 1, 1, 0, bytes, sizeof(bytes),
                                      &size) == VSR_ELIMIT);
    }
}

/* -------------------------------------------------------------------------
 * Superblocks, segment headers, clients file
 * ---------------------------------------------------------------------- */

static void test_superblock(void)
{
    struct vsr_io_wire_superblock in = {0, 0,           0x1111, 7,        1, 2,
                                        3, VSR_DURABLE, 4096,   1u << 20, 2, 16,
                                        5, 4,           9,      123456,   0, 0};
    struct vsr_io_wire_superblock out;
    unsigned char block[4096];

    memset(block, 0xff, sizeof(block));
    vsr_io_codec_put_superblock(&in, block, sizeof(block));
    CHECK(vsr_io_codec_get_superblock(block, sizeof(block), &out) == VSR_OK);
    CHECK(out.magic == VSR_IO_SUPERBLOCK_MAGIC && out.format == 1 &&
          out.generation == 0x1111 && out.revision == 7 &&
          out.cluster_hi == 1 && out.cluster_lo == 2 && out.replica == 3 &&
          out.durability == VSR_DURABLE && out.block_bytes == 4096 &&
          out.segment_bytes == 1u << 20 && out.header_blocks == 2 &&
          out.slots == 16 && out.start_segment == 5 && out.start_slot == 4 &&
          out.run == 9 && out.durable_floor == 123456 && out.reserved == 0);
    CHECK(out.crc == vsr_io_crc32c(0, block, 96));
    seed_corpus(2, block, 512);
    for (unsigned bit = 0; bit < 104 * 8; ++bit) {
        unsigned char copy[4096];

        memcpy(copy, block, sizeof(copy));
        copy[bit / 8] ^= (unsigned char)(1u << (bit % 8));
        CHECK(vsr_io_codec_get_superblock(copy, sizeof(copy), &out) ==
              VSR_EINVAL);
    }
    block[4095] = 1;
    CHECK(vsr_io_codec_get_superblock(block, sizeof(block), &out) ==
          VSR_EINVAL);
    block[4095] = 0;
    CHECK(vsr_io_codec_get_superblock(block, 104, &out) == VSR_OK);
    CHECK(vsr_io_codec_get_superblock(block, 103, &out) == VSR_EINVAL);
    vsr_io_put_u32(block + 4, 2); /* format */
    vsr_io_put_u32(block + 96, vsr_io_crc32c(0, block, 96));
    CHECK(vsr_io_codec_get_superblock(block, sizeof(block), &out) ==
          VSR_EINVAL);
}

static void check_segment(const struct vsr_limits *limits, int mode,
                          bool with_state, bool with_anchor)
{
    struct builder b;
    struct vsr_io_wire_segment fixed = {0, 0, 0x2222, 3, 0, 0, 4,
                                        0, 0, 0,      0, 0, 0};
    struct vsr_io_wire_segment decoded;
    struct vsr_io_segment_state state;
    struct vsr_store_identity identity;
    struct vsr_hard_state hard;
    /* Not NULL, so the decoder must be the one to clear it. */
    static struct vsr_checkpoint poison;
    struct vsr_checkpoint *checkpoint = &poison;
    struct vsr_io_bump region;
    size_t limit;
    size_t written;
    size_t header_bytes;

    arena_reset();
    builder_init(&b, limits, mode, -1);
    b.payload = UINT64_MAX;
    state.identity = with_state ? build_identity(&b) : NULL;
    state.hard = with_state ? build_hard_state(&b) : NULL;
    state.checkpoint = with_anchor ? build_checkpoint(&b) : NULL;
    state.log_begin = random64();
    state.log_end = random64();
    state.client_base = random64();
    state.last_sequence = random64();
    state.durable_floor = random64();
    CHECK(vsr_io_codec_segment_limit(limits, &limit) == VSR_OK);
    header_bytes = (limit + 511) / 512 * 512;
    CHECK(header_bytes <= BUFFER_BYTES);
    memset(reference_frame, 0xee, header_bytes);
    CHECK(vsr_io_codec_put_segment(&fixed, &state, reference_frame,
                                   header_bytes, &written) == VSR_OK);
    CHECK(written <= limit && written % 4 == 0 && written >= 84);
    CHECK(vsr_io_get_u32(reference_frame + 24) == written);
    seed_corpus(3, reference_frame, header_bytes);
    /* A header area of exactly the meaningful bytes suffices; one byte
     * fewer does not. */
    CHECK(vsr_io_codec_put_segment(&fixed, &state, random_frame, written,
                                   &written) == VSR_OK);
    CHECK(memcmp(random_frame, reference_frame, written) == 0);
    CHECK(vsr_io_codec_put_segment(&fixed, &state, random_frame, written - 1,
                                   &written) == VSR_ELIMIT);

    CHECK(vsr_io_codec_load_region(limits, &limit) == VSR_OK);
    vsr_io_bump_init(&region, region_memory, limit);
    CHECK(vsr_io_codec_get_segment(reference_frame, header_bytes, limits,
                                   &region, &decoded, &identity, &hard,
                                   &checkpoint) == VSR_OK);
    CHECK(decoded.magic == VSR_IO_SEGMENT_MAGIC && decoded.format == 1 &&
          decoded.generation == 0x2222 && decoded.segment == 3 &&
          decoded.length == written && decoded.run == 4 &&
          decoded.reserved == 0 &&
          decoded.last_sequence == state.last_sequence &&
          decoded.durable_floor == state.durable_floor &&
          decoded.client_base == state.client_base &&
          decoded.log_begin == state.log_begin &&
          decoded.log_end == state.log_end);
    CHECK(decoded.flags == ((with_state ? VSR_IO_SEGMENT_STATE : 0u) |
                            (with_anchor ? VSR_IO_SEGMENT_ANCHOR : 0u)));
    if (with_state) {
        CHECK(same_identity(state.identity, &identity));
        CHECK(same_hard_state(state.hard, &hard));
    } else {
        CHECK(identity.cluster.hi == 0 && identity.cluster.lo == 0 &&
              hard.epoch == NULL);
    }
    if (with_anchor) {
        CHECK(same_checkpoint(state.checkpoint, checkpoint));
    } else {
        CHECK(checkpoint == NULL);
    }
    /* Corruption: every byte of the meaningful part, a nonzero tail, a
     * length beyond the area, a truncated area. */
    for (unsigned round = 0; round < 64; ++round) {
        size_t at = round < 32 ? below(84) : below((uint32_t)written);

        memcpy(random_frame, reference_frame, header_bytes);
        random_frame[at] ^= (unsigned char)(1u << below(8));
        vsr_io_bump_init(&region, region_memory, limit);
        CHECK(vsr_io_codec_get_segment(random_frame, header_bytes, limits,
                                       &region, &decoded, &identity, &hard,
                                       &checkpoint) == VSR_EINVAL);
    }
    if (header_bytes > written) {
        memcpy(random_frame, reference_frame, header_bytes);
        random_frame[header_bytes - 1] = 1;
        vsr_io_bump_init(&region, region_memory, limit);
        CHECK(vsr_io_codec_get_segment(random_frame, header_bytes, limits,
                                       &region, &decoded, &identity, &hard,
                                       &checkpoint) == VSR_EINVAL);
    }
    vsr_io_bump_init(&region, region_memory, limit);
    CHECK(vsr_io_codec_get_segment(reference_frame, written - 1, limits,
                                   &region, &decoded, &identity, &hard,
                                   &checkpoint) == VSR_EINVAL);
    vsr_io_bump_init(&region, region_memory, limit);
    CHECK(vsr_io_codec_get_segment(reference_frame, written, limits, &region,
                                   &decoded, &identity, &hard,
                                   &checkpoint) == VSR_OK);
    /* Flags claiming state that is not there, with a valid CRC. */
    memcpy(random_frame, reference_frame, header_bytes);
    vsr_io_put_u32(random_frame + 28, VSR_IO_SEGMENT_ANCHOR);
    vsr_io_put_u32(random_frame + written - 4,
                   vsr_io_crc32c(0, random_frame, written - 4));
    vsr_io_bump_init(&region, region_memory, limit);
    CHECK(vsr_io_codec_get_segment(random_frame, header_bytes, limits, &region,
                                   &decoded, &identity, &hard,
                                   &checkpoint) == VSR_EINVAL);
    vsr_io_put_u32(random_frame + 28, decoded.flags ^ VSR_IO_SEGMENT_STATE);
    vsr_io_put_u32(random_frame + written - 4,
                   vsr_io_crc32c(0, random_frame, written - 4));
    vsr_io_bump_init(&region, region_memory, limit);
    CHECK(vsr_io_codec_get_segment(random_frame, header_bytes, limits, &region,
                                   &decoded, &identity, &hard,
                                   &checkpoint) == VSR_EINVAL);
}

static void test_segments(void)
{
    struct vsr_io_wire_segment fixed = {0};
    struct vsr_io_segment_state state = {0};
    struct vsr_hard_state hard = {0};
    struct vsr_store_identity identity = {0};
    size_t written;

    for (size_t set = 0; set < LIMIT_SETS; ++set) {
        for (int mode = 0; mode < 3; ++mode) {
            check_segment(&limit_sets[set], mode, false, false);
            check_segment(&limit_sets[set], mode, true, false);
            check_segment(&limit_sets[set], mode, true, true);
        }
    }
    /* Half a state is malformed. */
    state.identity = &identity;
    CHECK(vsr_io_codec_put_segment(&fixed, &state, reference_frame, 4096,
                                   &written) == VSR_EINVAL);
    state.identity = NULL;
    state.hard = &hard;
    CHECK(vsr_io_codec_put_segment(&fixed, &state, reference_frame, 4096,
                                   &written) == VSR_EINVAL);
}

static void test_clients_file(void)
{
    const struct vsr_limits *limits = &limit_sets[2];
    struct vsr_io_wire_clients_header header = {0, 0, 1, 2, 3, 4, 5, 6, 0, 0};
    struct vsr_io_wire_clients_header decoded;
    struct vsr_client_record records[8];
    struct vsr_io_cursor cursor;
    struct vsr_io_bump region;
    struct builder b;
    unsigned char *at = reference_frame;
    char name[VSR_IO_CLIENTS_NAME_BYTES];
    size_t region_bytes;
    size_t total;
    uint32_t count;

    vsr_io_codec_clients_name((struct vsr_id){0x0123456789abcdefu, 0xfedcba98u},
                              name);
    CHECK(strcmp(name, "clients-0123456789abcdef00000000fedcba98") == 0);
    CHECK(strlen(name) + 1 == VSR_IO_CLIENTS_NAME_BYTES);

    arena_reset();
    builder_init(&b, limits, 2, -1);
    header.count = 8;
    vsr_io_codec_put_clients_header(&header, at);
    at += 64;
    for (uint32_t i = 0; i < 8; ++i) {
        size_t bytes;

        b.mode = (int)(i % 3);
        build_client_record(&b, &records[i]);
        bytes = vsr_io_codec_clients_record_bytes(records[i].result.data.size);
        CHECK(bytes == 44 + (records[i].result.data.size + 7) / 8 * 8);
        vsr_io_codec_put_clients_record(&records[i], at);
        at += bytes;
    }
    vsr_io_codec_put_clients_trailer(8, at);
    at += 8;
    total = (size_t)(at - reference_frame);
    seed_corpus(4, reference_frame, total);

    CHECK(vsr_io_codec_load_region(limits, &region_bytes) == VSR_OK);
    vsr_io_cursor_init_one(&cursor, reference_frame, total);
    CHECK(vsr_io_codec_get_clients_header(&cursor, &decoded) == VSR_OK);
    CHECK(decoded.magic == VSR_IO_CLIENTS_MAGIC && decoded.format == 1 &&
          decoded.cluster_hi == 1 && decoded.cluster_lo == 2 &&
          decoded.snapshot_hi == 3 && decoded.snapshot_lo == 4 &&
          decoded.op == 5 && decoded.sequence == 6 && decoded.count == 8);
    for (uint32_t i = 0; i < 8; ++i) {
        struct vsr_client_record record;

        vsr_io_bump_init(&region, region_memory, region_bytes);
        CHECK(vsr_io_codec_get_clients_record(&cursor, limits, &region,
                                              &record) == VSR_OK);
        CHECK(same_client_record(&records[i], &record));
    }
    CHECK(vsr_io_codec_get_clients_trailer(&cursor, &count) == VSR_OK);
    CHECK(count == 8 && vsr_io_cursor_remaining(&cursor) == 0);

    /* Every flipped bit of the header and of the first record. */
    for (unsigned bit = 0; bit < 64 * 8; ++bit) {
        memcpy(random_frame, reference_frame, total);
        random_frame[bit / 8] ^= (unsigned char)(1u << (bit % 8));
        vsr_io_cursor_init_one(&cursor, random_frame, total);
        CHECK(vsr_io_codec_get_clients_header(&cursor, &decoded) == VSR_EINVAL);
        CHECK(cursor.position == 0);
    }
    {
        size_t first =
            vsr_io_codec_clients_record_bytes(records[0].result.data.size);

        for (unsigned bit = 0; bit < first * 8; ++bit) {
            struct vsr_client_record record;

            memcpy(random_frame, reference_frame, total);
            random_frame[64 + bit / 8] ^= (unsigned char)(1u << (bit % 8));
            vsr_io_cursor_init_one(&cursor, random_frame + 64, total - 64);
            vsr_io_bump_init(&region, region_memory, region_bytes);
            CHECK(vsr_io_codec_get_clients_record(&cursor, limits, &region,
                                                  &record) != VSR_OK);
        }
        /* Truncated record. */
        vsr_io_cursor_init_one(&cursor, reference_frame + 64, first - 1);
        vsr_io_bump_init(&region, region_memory, region_bytes);
        CHECK(vsr_io_codec_get_clients_record(&cursor, limits, &region,
                                              &records[0]) == VSR_EINVAL);
        /* A missing cursor is EINVAL, not a dereference. */
        CHECK(vsr_io_codec_get_clients_record(NULL, limits, &region,
                                              &records[0]) == VSR_EINVAL);
    }
    /* Trailer. */
    vsr_io_cursor_init_one(&cursor, reference_frame + total - 8, 8);
    CHECK(vsr_io_codec_get_clients_trailer(&cursor, &count) == VSR_OK);
    vsr_io_cursor_init_one(&cursor, reference_frame + total - 8, 7);
    CHECK(vsr_io_codec_get_clients_trailer(&cursor, &count) == VSR_EINVAL);
    memcpy(random_frame, reference_frame + total - 8, 8);
    random_frame[1] ^= 1;
    vsr_io_cursor_init_one(&cursor, random_frame, 8);
    CHECK(vsr_io_codec_get_clients_trailer(&cursor, &count) == VSR_EINVAL);
    /* A result above the limit is ELIMIT even with a valid CRC. */
    {
        struct vsr_client_record big = records[0];
        struct vsr_blob blob = {NULL, 0, 0, 0};
        struct vsr_span span;
        struct vsr_client_record record;

        span.data = scratch_a;
        span.size = limits->result_bytes + 1;
        blob.spans = &span;
        blob.size = span.size;
        blob.count = 1;
        big.result.data = blob;
        vsr_io_codec_put_clients_record(&big, random_frame);
        vsr_io_cursor_init_one(&cursor, random_frame,
                               vsr_io_codec_clients_record_bytes(span.size));
        vsr_io_bump_init(&region, region_memory, region_bytes);
        CHECK(vsr_io_codec_get_clients_record(&cursor, limits, &region,
                                              &record) == VSR_ELIMIT);
    }
}

/* -------------------------------------------------------------------------
 * Limits and sizing
 * ---------------------------------------------------------------------- */

/* The largest state chunk the limits allow, with every entry of one type. */
static struct vsr_message *build_largest(struct builder *b, int entry_type)
{
    b->entry_type = entry_type;
    b->payload = b->limits->message_bytes;
    return build_message(b, VSR_MSG_NEW_STATE);
}

static void test_sizing(void)
{
    for (size_t set = 0; set < LIMIT_SETS; ++set) {
        const struct vsr_limits *limits = &limit_sets[set];
        static const int types[3] = {VSR_REQUEST_COMMAND,
                                     VSR_REQUEST_RECONFIGURE,
                                     VSR_REQUEST_CHECK_EPOCH};
        uint64_t frame_limit;
        uint64_t record_limit;
        size_t message_region;
        size_t load_region;
        size_t segment_limit;

        CHECK(vsr_io_codec_frame_limit(limits, &frame_limit) == VSR_OK);
        CHECK(vsr_io_codec_record_limit(limits, &record_limit) == VSR_OK);
        CHECK(vsr_io_codec_message_region(limits, &message_region) == VSR_OK);
        CHECK(vsr_io_codec_load_region(limits, &load_region) == VSR_OK);
        CHECK(vsr_io_codec_segment_limit(limits, &segment_limit) == VSR_OK);
        CHECK(record_limit % 8 == 0);
        for (unsigned i = 0; i < 3; ++i) {
            struct builder b;
            struct vsr_message *message;
            struct vsr_io_bump region;
            struct vsr_io_cursor cursor;
            struct vsr_message *decoded = NULL;
            uint32_t length;
            uint32_t crc;
            size_t total;

            arena_reset();
            builder_init(&b, limits, 1, types[i]);
            message = build_largest(&b, types[i]);
            CHECK(vsr_io_codec_message_digest(message, limits, &length, &crc) ==
                  VSR_OK);
            total = encode_reference(message, length, crc, reference_frame);
            CHECK(total <= frame_limit);
            vsr_io_bump_init(&region, region_memory, message_region);
            vsr_io_cursor_init_one(&cursor, reference_frame + 24, length);
            CHECK(vsr_io_codec_decode_message(&cursor, limits, &region,
                                              &decoded) == VSR_OK);
            CHECK(same_message(message, decoded));
            /* An APPEND of the same entries fits a load region. */
            {
                const struct vsr_state_chunk *chunk = message->body;
                struct vsr_change change = {VSR_STORE_APPEND,
                                            chunk->state.entries.count, 1,
                                            chunk->state.entries.entries};
                struct vsr_store store = {1, &change, 1, 0};
                struct vsr_entry *entries = NULL;
                size_t written;

                CHECK(vsr_io_codec_put_record(&store, 1, 1, 0, random_frame,
                                              sizeof(random_frame),
                                              &written) == VSR_OK);
                CHECK(written <= record_limit);
                vsr_io_bump_init(&region, region_memory, load_region);
                vsr_io_cursor_init_one(&cursor, random_frame + 72,
                                       written - 72);
                CHECK(vsr_io_codec_get_entries(&cursor, change.count, limits,
                                               &region, &entries) == VSR_OK);
            }
        }
        /* The largest transaction: every change at its maximum. */
        {
            struct builder b;
            struct transaction t;
            struct vsr_io_bump region;
            struct vsr_io_cursor cursor;
            struct vsr_io_wire_record header;
            struct vsr_io_wire_change change;
            struct vsr_hard_state hard;
            struct vsr_checkpoint *checkpoint;
            struct vsr_loaded *loaded;
            struct vsr_recovered *recovered;
            size_t bytes;
            size_t written;
            uint32_t kind;

            arena_reset();
            builder_init(&b, limits, 1, VSR_REQUEST_COMMAND);
            build_transaction(&b, 255, &t);
            CHECK(t.store.count == VSR_MAX_STORE_CHANGES);
            CHECK(vsr_io_codec_record_bytes(&t.store, limits, &bytes) ==
                  VSR_OK);
            CHECK(bytes <= record_limit);
            CHECK(vsr_io_codec_put_record(&t.store, 1, 1, 0, reference_frame,
                                          sizeof(reference_frame),
                                          &written) == VSR_OK);
            CHECK(written == bytes);
            /* A recovered row's graph: loaded, recovered, the hard state's
             * epoch and the anchor, in one load region. */
            vsr_io_bump_init(&region, region_memory, load_region);
            loaded = vsr_io_bump_alloc(&region, sizeof(*loaded),
                                       alignof(struct vsr_loaded));
            recovered = vsr_io_bump_alloc(&region, sizeof(*recovered),
                                          alignof(struct vsr_recovered));
            CHECK(loaded != NULL && recovered != NULL);
            vsr_io_cursor_init_one(&cursor, reference_frame, written);
            CHECK(vsr_io_codec_get_record(&cursor, record_limit, &header,
                                          &kind) == VSR_OK);
            for (uint32_t i = 0; i < header.count; ++i) {
                struct vsr_io_cursor payload;

                CHECK(vsr_io_codec_get_change(&cursor, &change) == VSR_OK);
                vsr_io_cursor_init_one(
                    &payload, reference_frame + change.offset, change.length);
                if (change.type == VSR_STORE_HARD_STATE) {
                    CHECK(vsr_io_codec_get_hard_state(&payload, limits, &region,
                                                      &hard) == VSR_OK);
                } else if (change.type == VSR_STORE_PUBLISH_CHECKPOINT) {
                    CHECK(vsr_io_codec_get_checkpoint(&payload, limits, &region,
                                                      &checkpoint) == VSR_OK);
                }
            }
            /* The largest segment header fits the segment limit. */
            {
                struct vsr_io_wire_segment fixed = {0};
                struct vsr_io_segment_state state = {
                    t.identity, t.hard, t.publish, 1, 2, 3, 4, 5};

                CHECK(vsr_io_codec_put_segment(&fixed, &state, random_frame,
                                               segment_limit,
                                               &written) == VSR_OK);
                CHECK(written == segment_limit);
            }
        }
        /* Over the limits: ELIMIT from the digest and from the decoder
         * (the same bytes decoded under smaller limits). */
        {
            struct builder b;
            struct vsr_limits larger = *limits;
            uint32_t length;
            uint32_t crc;

            larger.batch_entries++;
            larger.members++;
            larger.command_bytes++;
            larger.manifest_bytes++;
            larger.message_bytes += 2;
            for (unsigned variant = 0; variant < 4; ++variant) {
                struct vsr_io_bump region;
                struct vsr_io_cursor cursor;
                struct vsr_message *message;
                struct vsr_message *decoded;
                struct vsr_limits mixed = *limits;

                arena_reset();
                switch (variant) {
                case 0:
                    mixed.batch_entries = larger.batch_entries;
                    break;
                case 1:
                    mixed.members = larger.members;
                    break;
                case 2:
                    mixed.command_bytes = larger.command_bytes;
                    mixed.message_bytes = larger.message_bytes;
                    break;
                default:
                    mixed.manifest_bytes = larger.manifest_bytes;
                    mixed.message_bytes = larger.message_bytes;
                    break;
                }
                builder_init(&b, &mixed, 1, VSR_REQUEST_COMMAND);
                message =
                    build_largest(&b, variant == 1 ? VSR_REQUEST_RECONFIGURE
                                                   : VSR_REQUEST_COMMAND);
                CHECK(vsr_io_codec_message_digest(message, limits, &length,
                                                  &crc) == VSR_ELIMIT);
                CHECK(vsr_io_codec_message_digest(message, &mixed, &length,
                                                  &crc) == VSR_OK);
                encode_reference(message, length, crc, reference_frame);
                vsr_io_bump_init(&region, region_memory, REGION_BYTES);
                vsr_io_cursor_init_one(&cursor, reference_frame + 24, length);
                CHECK(vsr_io_codec_decode_message(&cursor, limits, &region,
                                                  &decoded) == VSR_ELIMIT);
            }
        }
        /* Overflowing limits. */
        {
            struct vsr_limits huge = *limits;
            uint64_t bytes64;
            size_t bytes;

            huge.message_bytes = UINT64_MAX;
            CHECK(vsr_io_codec_frame_limit(&huge, &bytes64) == VSR_ELIMIT);
            CHECK(vsr_io_codec_record_limit(&huge, &bytes64) == VSR_ELIMIT);
            huge = *limits;
            huge.batch_entries = UINT32_MAX;
            huge.members = UINT32_MAX;
            CHECK(vsr_io_codec_message_region(&huge, &bytes) == VSR_ELIMIT);
            CHECK(vsr_io_codec_load_region(&huge, &bytes) == VSR_ELIMIT);
            huge = *limits;
            huge.manifest_bytes = UINT64_MAX - 3;
            CHECK(vsr_io_codec_segment_limit(&huge, &bytes) == VSR_ELIMIT);
            CHECK(vsr_io_codec_record_limit(&huge, &bytes64) == VSR_ELIMIT);
        }
    }
}

/* Malformed graphs are EINVAL from the digest. */
static void test_malformed(void)
{
    const struct vsr_limits *limits = &limit_sets[1];
    struct vsr_nonce nonce = {{1, 2}, 3};
    struct vsr_message message = {{1, 2}, 0, 0,     5, VSR_MSG_READ_PROBE,
                                  0,      1, &nonce};
    struct vsr_member members[1] = {{1, VSR_MEMBER_FULL, 0}};
    struct vsr_membership membership = {0, members, 1, 0};
    struct vsr_epoch epoch = {&membership, NULL, 0, 0, 0};
    struct vsr_span span = {"x", 1};
    struct vsr_blob blob = {&span, 1, 1, 0};
    struct vsr_entry entry = {1, 0,    0, {{1, 1}, 1}, VSR_REQUEST_COMMAND,
                              0, &blob};
    struct vsr_prepare prepare = {{&entry, 1, 0}, 0};
    uint32_t length;
    uint32_t crc;

    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_OK);
    message.flags = 1;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_EINVAL);
    message.flags = 0;
    message.type = VSR_MSG_READ_ACK + 1;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_EINVAL);
    message.type = VSR_MSG_COMMIT;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_EINVAL);
    message.body = NULL;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_OK);
    message.type = VSR_MSG_START_EPOCH;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_EINVAL);
    message.body = &epoch;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_OK);
    epoch.reserved = 1;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_EINVAL);
    epoch.reserved = 0;
    members[0].reserved = 1;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_EINVAL);
    members[0].reserved = 0;
    membership.count = 0;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_EINVAL);
    membership.count = 1;
    epoch.current = NULL;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_EINVAL);
    epoch.current = &membership;

    message.type = VSR_MSG_PREPARE;
    message.body = &prepare;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_OK);
    entry.reserved = 1;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_EINVAL);
    entry.reserved = 0;
    entry.type = VSR_REQUEST_NOOP;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_EINVAL);
    entry.body = NULL;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_OK);
    entry.type = VSR_REQUEST_CHECK_EPOCH;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_EINVAL);
    entry.type = VSR_REQUEST_NOOP + 1;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_EINVAL);
    entry.type = VSR_REQUEST_COMMAND;
    entry.body = &blob;
    blob.size = 2;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_EINVAL);
    blob.size = 1;
    blob.reserved = 1;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_EINVAL);
    blob.reserved = 0;
    span.size = 0;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_EINVAL);
    span.size = 1;
    prepare.batch.reserved = 1;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_EINVAL);
    prepare.batch.reserved = 0;
    prepare.batch.count = 0;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_EINVAL);
    prepare.batch.entries = NULL;
    CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
          VSR_OK);
    CHECK(length == 56 + 8 + 8);

    /* Decoder: unknown flags, reserved bits, a body length that lies. */
    {
        struct vsr_io_bump region;
        struct vsr_io_cursor cursor;
        struct vsr_message *decoded;
        unsigned char body[256];

        prepare.batch.entries = &entry;
        prepare.batch.count = 1;
        CHECK(vsr_io_codec_message_digest(&message, limits, &length, &crc) ==
              VSR_OK);
        encode_reference(&message, length, crc, reference_frame);
        CHECK(length == 56 + 16 + 56 + 16);
        for (unsigned variant = 0; variant < 5; ++variant) {
            memcpy(body, reference_frame + 24, length);
            switch (variant) {
            case 0:
                vsr_io_put_u32(body + 44, 1); /* message flags */
                break;
            case 1:
                vsr_io_put_u32(body + 68, 1); /* entries reserved */
                break;
            case 2:
                vsr_io_put_u32(body + 72 + 52, 8); /* body length short */
                break;
            case 3:
                vsr_io_put_u32(body + 72 + 52, 24); /* body length long */
                break;
            default:
                body[length - 1] = 1; /* blob padding */
                break;
            }
            vsr_io_bump_init(&region, region_memory, REGION_BYTES);
            vsr_io_cursor_init_one(&cursor, body, length);
            CHECK(vsr_io_codec_decode_message(&cursor, limits, &region,
                                              &decoded) == VSR_EINVAL);
        }
        /* Too small a region is ELIMIT. */
        vsr_io_bump_init(&region, region_memory, 64);
        vsr_io_cursor_init_one(&cursor, reference_frame + 24, length);
        CHECK(vsr_io_codec_decode_message(&cursor, limits, &region, &decoded) ==
              VSR_ELIMIT);
    }
}

/* -------------------------------------------------------------------------
 * Review additions: differential round trips, region bounds at every
 * shape, over-limit refusals by digest and decoder alike
 * ---------------------------------------------------------------------- */

/* decode(encode(x)) == x and encode(decode(bytes)) == bytes for random
 * valid graphs of every message type, into a region of exactly the computed
 * size at every misalignment of its base. */
static void test_differential(void)
{
    const uint64_t seed = 0x9e3779b97f4a7c15ull;

    printf("codec: differential seed %llx\n", (unsigned long long)seed);
    test_random_seed(&rng, seed, 11);
    for (size_t set = 0; set < LIMIT_SETS; ++set) {
        const struct vsr_limits *limits = &limit_sets[set];
        size_t region_bytes;

        CHECK(vsr_io_codec_message_region(limits, &region_bytes) == VSR_OK);
        for (uint32_t type = 0; type <= VSR_MSG_READ_ACK; ++type) {
            for (unsigned round = 0; round < 4; ++round) {
                struct builder b;
                struct vsr_message *message;
                struct vsr_message *decoded = NULL;
                struct vsr_io_bump region;
                struct vsr_io_cursor cursor;
                uint32_t length;
                uint32_t crc;
                uint32_t length2;
                uint32_t crc2;
                size_t total;
                size_t skew = round % 8;

                arena_reset();
                builder_init(&b, limits, 2, -1);
                message = build_message(&b, type);
                CHECK(vsr_io_codec_message_digest(message, limits, &length,
                                                  &crc) == VSR_OK);
                total = encode_reference(message, length, crc, reference_frame);
                /* Exact region, base skewed by round bytes. */
                vsr_io_bump_init(&region, region_memory + skew, region_bytes);
                vsr_io_cursor_init_one(&cursor, reference_frame + 24, length);
                CHECK(vsr_io_codec_decode_message(&cursor, limits, &region,
                                                  &decoded) == VSR_OK);
                CHECK(same_message(message, decoded));
                /* The decoded graph digests and encodes to the same bytes. */
                CHECK(vsr_io_codec_message_digest(decoded, limits, &length2,
                                                  &crc2) == VSR_OK);
                CHECK(length2 == length && crc2 == crc);
                CHECK(encode_reference(decoded, length2, crc2, random_frame) ==
                      total);
                CHECK(memcmp(random_frame, reference_frame, total) == 0);
            }
        }
    }
}

/* Maximal counts with minimal bodies: the region bound holds for shapes the
 * random builder rarely produces (empty commands, one-member memberships,
 * empty manifests, every entry type at batch_entries). */
static void test_region_shapes(void)
{
    for (size_t set = 0; set < LIMIT_SETS; ++set) {
        const struct vsr_limits *limits = &limit_sets[set];
        size_t region_bytes;

        CHECK(vsr_io_codec_message_region(limits, &region_bytes) == VSR_OK);
        for (int entry_type = 0; entry_type <= VSR_REQUEST_NOOP; ++entry_type) {
            for (unsigned variant = 0; variant < 3; ++variant) {
                struct builder b;
                struct vsr_message *message;
                struct vsr_message *decoded = NULL;
                struct vsr_io_bump region;
                struct vsr_io_cursor cursor;
                uint32_t length;
                uint32_t crc;

                arena_reset();
                /* mode 1 fills every count to its limit; mode 0 empties
                 * every body; variant 2 mixes maximal counts with empty
                 * bodies by building maximal then shrinking blobs. */
                builder_init(&b, limits, variant == 0 ? 1 : 0, entry_type);
                if (variant != 2) {
                    message = build_message(&b, VSR_MSG_NEW_STATE);
                } else {
                    struct vsr_prepare *prepare = NEW(struct vsr_prepare);
                    struct vsr_entry *entries =
                        build_entry_array(&b, limits->batch_entries);

                    b.mode = 1;
                    message = build_message(&b, VSR_MSG_PREPARE);
                    for (uint32_t i = 0; i < limits->batch_entries; ++i) {
                        if (entries[i].type == VSR_REQUEST_COMMAND) {
                            struct vsr_blob *blob = NEW(struct vsr_blob);

                            blob->spans = NULL;
                            blob->size = 0;
                            blob->count = 0;
                            blob->reserved = 0;
                            entries[i].body = blob;
                        }
                    }
                    prepare->batch.entries = entries;
                    prepare->batch.count = limits->batch_entries;
                    prepare->batch.reserved = 0;
                    prepare->committed = 1;
                    message->body = prepare;
                }
                CHECK(vsr_io_codec_message_digest(message, limits, &length,
                                                  &crc) == VSR_OK);
                encode_reference(message, length, crc, reference_frame);
                vsr_io_bump_init(&region, region_memory + 7, region_bytes);
                vsr_io_cursor_init_one(&cursor, reference_frame + 24, length);
                CHECK(vsr_io_codec_decode_message(&cursor, limits, &region,
                                                  &decoded) == VSR_OK);
                CHECK(same_message(message, decoded));
            }
        }
    }
}

/* Just over each limit: refused by the digest, by record_bytes and by the
 * decoders, for the aggregate and the per-object limits the earlier sizing
 * test does not exercise on their own. */
static void test_over_limits(void)
{
    const struct vsr_limits *limits = &limit_sets[3];
    struct vsr_limits larger = *limits;
    struct builder b;
    struct vsr_io_bump region;
    struct vsr_io_cursor cursor;
    uint32_t length;
    uint32_t crc;
    size_t bytes;

    CHECK(limits->command_bytes * limits->batch_entries >
          limits->message_bytes);
    /* Aggregate: every command within command_bytes, the sum one over. */
    {
        struct vsr_message *message;
        struct vsr_message *decoded;
        const struct vsr_prepare *prepare;
        uint64_t sum = 0;

        larger.message_bytes = limits->message_bytes + 1;
        arena_reset();
        builder_init(&b, &larger, 1, VSR_REQUEST_COMMAND);
        message = build_message(&b, VSR_MSG_PREPARE);
        prepare = message->body;
        for (uint32_t i = 0; i < prepare->batch.count; ++i) {
            const struct vsr_blob *blob = prepare->batch.entries[i].body;

            CHECK(blob->size <= limits->command_bytes);
            sum += blob->size;
        }
        CHECK(sum == limits->message_bytes + 1);
        CHECK(vsr_io_codec_message_digest(message, limits, &length, &crc) ==
              VSR_ELIMIT);
        CHECK(vsr_io_codec_message_digest(message, &larger, &length, &crc) ==
              VSR_OK);
        encode_reference(message, length, crc, reference_frame);
        vsr_io_bump_init(&region, region_memory, REGION_BYTES);
        vsr_io_cursor_init_one(&cursor, reference_frame + 24, length);
        CHECK(vsr_io_codec_decode_message(&cursor, limits, &region, &decoded) ==
              VSR_ELIMIT);
        /* The same entries as an APPEND: record_bytes and get_entries. */
        {
            struct vsr_change change = {VSR_STORE_APPEND, prepare->batch.count,
                                        1, prepare->batch.entries};
            struct vsr_store store = {1, &change, 1, 0};
            struct vsr_entry *entries = NULL;
            size_t written;

            CHECK(vsr_io_codec_record_bytes(&store, limits, &bytes) ==
                  VSR_ELIMIT);
            CHECK(vsr_io_codec_record_bytes(&store, &larger, &bytes) == VSR_OK);
            CHECK(vsr_io_codec_put_record(&store, 1, 1, 0, random_frame,
                                          sizeof(random_frame),
                                          &written) == VSR_OK);
            vsr_io_bump_init(&region, region_memory, REGION_BYTES);
            vsr_io_cursor_init_one(&cursor, random_frame + 72, written - 72);
            CHECK(vsr_io_codec_get_entries(&cursor, change.count, limits,
                                           &region, &entries) == VSR_ELIMIT);
        }
    }
    /* Members: a HARD_STATE record with members + 1. */
    {
        struct vsr_hard_state *hard;
        struct vsr_hard_state decoded;
        struct vsr_change change;
        struct vsr_store store = {1, &change, 1, 0};
        size_t written;

        larger = *limits;
        larger.members++;
        arena_reset();
        builder_init(&b, &larger, 1, -1);
        hard = build_hard_state(&b);
        CHECK(hard->epoch->current->count == larger.members);
        change = (struct vsr_change){VSR_STORE_HARD_STATE, 1, 0, hard};
        CHECK(vsr_io_codec_record_bytes(&store, limits, &bytes) == VSR_ELIMIT);
        CHECK(vsr_io_codec_record_bytes(&store, &larger, &bytes) == VSR_OK);
        CHECK(vsr_io_codec_put_record(&store, 1, 1, 0, random_frame,
                                      sizeof(random_frame),
                                      &written) == VSR_OK);
        vsr_io_bump_init(&region, region_memory, REGION_BYTES);
        vsr_io_cursor_init_one(&cursor, random_frame + 72, written - 72);
        CHECK(vsr_io_codec_get_hard_state(&cursor, limits, &region, &decoded) ==
              VSR_ELIMIT);
    }
    /* Batch: an APPEND of batch_entries + 1 NOOPs. */
    {
        struct vsr_entry *entries;
        struct vsr_entry *decoded = NULL;
        struct vsr_change change;
        struct vsr_store store = {1, &change, 1, 0};
        size_t written;

        larger = *limits;
        larger.batch_entries++;
        arena_reset();
        builder_init(&b, &larger, 0, VSR_REQUEST_NOOP);
        entries = build_entry_array(&b, larger.batch_entries);
        change = (struct vsr_change){VSR_STORE_APPEND, larger.batch_entries, 1,
                                     entries};
        CHECK(vsr_io_codec_record_bytes(&store, limits, &bytes) == VSR_ELIMIT);
        CHECK(vsr_io_codec_put_record(&store, 1, 1, 0, random_frame,
                                      sizeof(random_frame),
                                      &written) == VSR_OK);
        vsr_io_bump_init(&region, region_memory, REGION_BYTES);
        vsr_io_cursor_init_one(&cursor, random_frame + 72, written - 72);
        CHECK(vsr_io_codec_get_entries(&cursor, change.count, limits, &region,
                                       &decoded) == VSR_ELIMIT);
        /* get_entry_at reaches the last one regardless of the batch rule,
         * and fails one past it without moving out of the payload. */
        vsr_io_cursor_init_one(&cursor, random_frame + 72, written - 72);
        CHECK(vsr_io_codec_get_entry_at(
                  &cursor, larger.batch_entries - 1, limits, &region,
                  decoded == NULL ? &entries[0] : decoded) == VSR_OK);
        CHECK(vsr_io_cursor_remaining(&cursor) == 0);
        vsr_io_cursor_init_one(&cursor, random_frame + 72, written - 72);
        CHECK(vsr_io_codec_get_entry_at(&cursor, larger.batch_entries, limits,
                                        &region, &entries[0]) == VSR_EINVAL);
        /* A hostile body_length in a skipped entry: not a multiple of 8,
         * or beyond the payload. */
        vsr_io_put_u32(random_frame + 72 + 52, 4);
        vsr_io_cursor_init_one(&cursor, random_frame + 72, written - 72);
        CHECK(vsr_io_codec_get_entry_at(&cursor, 1, limits, &region,
                                        &entries[0]) == VSR_EINVAL);
        vsr_io_put_u32(random_frame + 72 + 52, UINT32_MAX - 7);
        vsr_io_cursor_init_one(&cursor, random_frame + 72, written - 72);
        CHECK(vsr_io_codec_get_entry_at(&cursor, 1, limits, &region,
                                        &entries[0]) == VSR_EINVAL);
    }
    /* Result: a CLIENTS record with result_bytes + 1. */
    {
        struct vsr_client_record record;
        struct vsr_client_record decoded;
        struct vsr_change change;
        struct vsr_store store = {1, &change, 1, 0};
        size_t written;

        larger = *limits;
        larger.result_bytes++;
        arena_reset();
        builder_init(&b, &larger, 1, -1);
        build_client_record(&b, &record);
        CHECK(record.result.data.size == larger.result_bytes);
        change = (struct vsr_change){VSR_STORE_CLIENTS, 1, 0, &record};
        CHECK(vsr_io_codec_record_bytes(&store, limits, &bytes) == VSR_ELIMIT);
        CHECK(vsr_io_codec_record_bytes(&store, &larger, &bytes) == VSR_OK);
        CHECK(vsr_io_codec_put_record(&store, 1, 1, 0, random_frame,
                                      sizeof(random_frame),
                                      &written) == VSR_OK);
        vsr_io_bump_init(&region, region_memory, REGION_BYTES);
        vsr_io_cursor_init_one(&cursor, random_frame + 72, written - 72);
        CHECK(vsr_io_codec_get_client_record(&cursor, limits, &region,
                                             &decoded) == VSR_ELIMIT);
    }
}

/* The vectored encoder never references bytes that move: every vector lies
 * in the writer or in a payload span, and only spans of at least
 * VSR_IO_INLINE_BYTES are referenced. */
static void test_vector_origins(void)
{
    const struct vsr_limits *limits = &limit_sets[3];
    struct builder b;
    struct vsr_message *message;
    const struct vsr_prepare *prepare;
    struct vsr_io_encoder encoder;
    uint32_t length;
    uint32_t crc;
    bool done = false;
    unsigned referenced = 0;

    arena_reset();
    builder_init(&b, limits, 1, VSR_REQUEST_COMMAND);
    message = build_message(&b, VSR_MSG_PREPARE);
    prepare = message->body;
    CHECK(vsr_io_codec_message_digest(message, limits, &length, &crc) ==
          VSR_OK);
    vsr_io_encoder_begin(&encoder, message, length, crc);
    while (!done) {
        struct vsr_io_writer writer = {writer_memory, 100, 0};
        uint32_t count = 0;

        CHECK(vsr_io_encoder_emit(&encoder, &writer, vectors, 3, &count,
                                  UINT64_MAX, &done) == VSR_OK);
        for (uint32_t i = 0; i < count; ++i) {
            const unsigned char *base = vectors[i].base;
            bool in_writer =
                base >= writer_memory &&
                base + vectors[i].length <= writer_memory + writer.used;
            bool in_span = false;

            /* The builder lays a blob's spans out contiguously, so a
             * vector may cover several adjacent referenced spans; it must
             * never cover a span below the inline threshold. */
            for (uint32_t e = 0; e < prepare->batch.count && !in_span; ++e) {
                const struct vsr_blob *blob = prepare->batch.entries[e].body;
                const unsigned char *first;
                const unsigned char *end;

                if (blob->count == 0) {
                    continue;
                }
                first = blob->spans[0].data;
                end = (const unsigned char *)blob->spans[blob->count - 1].data +
                      blob->spans[blob->count - 1].size;
                if (base < first || base + vectors[i].length > end) {
                    continue;
                }
                in_span = true;
                for (uint32_t s = 0; s < blob->count; ++s) {
                    const unsigned char *data = blob->spans[s].data;

                    if (blob->spans[s].size < VSR_IO_INLINE_BYTES &&
                        base < data + blob->spans[s].size &&
                        data < base + vectors[i].length) {
                        in_span = false;
                    }
                }
            }
            CHECK(in_writer || in_span);
            referenced += in_span;
        }
    }
    CHECK(referenced > 0);
}

/* APPEND payloads of batch_entries entries of each type, and of mixed
 * types, at the minimal, maximal and random shapes under the limits of
 * tests/fuzzy/entry: get_entry_at decodes every index to the entry it
 * encodes, and each (payload, index) seeds the entry corpus. */
static void test_entry_seeds(void)
{
    const struct vsr_limits *limits = &limit_sets[2];
    size_t region_bytes;

    CHECK(vsr_io_codec_load_region(limits, &region_bytes) == VSR_OK);
    CHECK(region_bytes <= REGION_BYTES);
    for (int type = -1; type <= VSR_REQUEST_NOOP; ++type) {
        /* Mixed types come only from the random shape. */
        for (int mode = type < 0 ? 2 : 0; mode < 3; ++mode) {
            struct builder b;
            struct vsr_change change;
            struct vsr_store store = {1, &change, 1, 0};
            struct vsr_io_cursor cursor;
            struct vsr_io_wire_change wire;
            struct vsr_entry *entries;
            size_t written;

            arena_reset();
            builder_init(&b, limits, mode, type);
            entries = build_entry_array(&b, limits->batch_entries);
            change = (struct vsr_change){VSR_STORE_APPEND,
                                         limits->batch_entries, 1, entries};
            CHECK(vsr_io_codec_put_record(&store, 1, 1, 0, reference_frame,
                                          sizeof(reference_frame),
                                          &written) == VSR_OK);
            vsr_io_cursor_init_one(
                &cursor, reference_frame + sizeof(struct vsr_io_wire_record),
                written - sizeof(struct vsr_io_wire_record));
            CHECK(vsr_io_codec_get_change(&cursor, &wire) == VSR_OK);
            CHECK((size_t)wire.offset + wire.length <= written);
            for (uint32_t i = 0; i < change.count; ++i) {
                struct vsr_io_bump region;
                struct vsr_entry one;

                vsr_io_cursor_init_one(&cursor, reference_frame + wire.offset,
                                       wire.length);
                vsr_io_bump_init(&region, region_memory, region_bytes);
                CHECK(vsr_io_codec_get_entry_at(&cursor, i, limits, &region,
                                                &one) == VSR_OK);
                CHECK(same_entry(&entries[i], &one));
                seed_entry_corpus(i, reference_frame + wire.offset,
                                  wire.length);
            }
        }
    }
}

static void test_bump(void)
{
    struct vsr_io_bump bump;
    unsigned char memory[64];
    void *a;
    void *b;

    vsr_io_bump_init(&bump, memory + 1, 63);
    a = vsr_io_bump_alloc(&bump, 1, 1);
    CHECK(a == memory + 1 && bump.used == 1);
    b = vsr_io_bump_alloc(&bump, 8, 8);
    CHECK(b != NULL && (uintptr_t)b % 8 == 0 && (unsigned char *)b > memory);
    CHECK(vsr_io_bump_alloc(&bump, 64, 1) == NULL);
    CHECK(vsr_io_bump_alloc(&bump, 63 - bump.used, 1) != NULL);
    CHECK(bump.used == 63);
    CHECK(vsr_io_bump_alloc(&bump, 1, 1) == NULL);
    CHECK(vsr_io_bump_alloc(&bump, 0, 1) != NULL);
}

int main(void)
{
    corpus_dir = getenv("VSR_CODEC_CORPUS");
    entry_corpus_dir = getenv("VSR_CODEC_ENTRY_CORPUS");
    test_random_seed(&rng, 0x5eed, 7);
    test_bump();
    test_layout();
    test_frames();
    test_malformed();
    test_sizing();
    test_messages();
    test_records();
    test_scanning();
    test_superblock();
    test_segments();
    test_clients_file();
    test_differential();
    test_region_shapes();
    test_over_limits();
    test_vector_origins();
    test_entry_seeds();
    printf("codec: ok\n");
    return 0;
}
