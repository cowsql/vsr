#ifndef VSR_EXAMPLE_KV_H
#define VSR_EXAMPLE_KV_H

/*
 * The sample application: a tiny deterministic in-memory key-value store.
 *
 * The core never looks inside commands or results; both are opaque bytes. This
 * store interprets short text commands and answers with short text results:
 *
 *   set KEY VALUE   -> VALUE
 *   get KEY         -> VALUE, or "(nil)" when the key is absent
 *   del KEY         -> "1" when a key was removed, else "0"
 *   incr KEY        -> the incremented integer (an absent key counts as 0)
 *
 * Every replica executes the same committed commands in the same order, so
 * every replica's store holds the same map. A checkpoint image is the map
 * serialized as text; installing an image replaces the map wholesale. The
 * store also counts how many commands it executed itself, which tells replay
 * apart from snapshot installation in the examples.
 */

#include "lib/memory_cluster.h"

#include <stddef.h>
#include <stdint.h>

enum {
    KV_KEYS = 32,
    KV_KEY_BYTES = 16,
    KV_VALUE_BYTES = 32,
    KV_RESULT_OK = 0, /* Result code of an executed command. */
    KV_RESULT_ERROR = -1
};

struct kv_entry {
    char key[KV_KEY_BYTES];
    char value[KV_VALUE_BYTES];
};

struct kv {
    struct kv_entry entries[KV_KEYS];
    uint32_t count;
    uint64_t commands; /* Executed here since genesis or the last install. */
};

/* Host callbacks; pass to mem_cluster_set_application before adding nodes. */
extern const struct mem_application kv_application;

/* The value stored under key, or NULL. */
const char *kv_get(const struct kv *store, const char *key);
/* Renders the whole map as "a=1 b=2", or "(empty)". */
const char *kv_format(const struct kv *store, char *buffer, size_t size);

#endif
