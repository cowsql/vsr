#include "config.h"

#include "kv.h"

#include "lib/check.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { KV_COMMAND_BYTES = 128 };

static struct kv_entry *kv_find(struct kv *store, const char *key)
{
    for (uint32_t i = 0; i < store->count; i++)
        if (strcmp(store->entries[i].key, key) == 0)
            return &store->entries[i];
    return NULL;
}

static void kv_set(struct kv *store, const char *key, const char *value)
{
    struct kv_entry *entry = kv_find(store, key);
    if (entry == NULL) {
        CHECK(store->count < KV_KEYS);
        entry = &store->entries[store->count++];
        snprintf(entry->key, sizeof(entry->key), "%s", key);
    }
    snprintf(entry->value, sizeof(entry->value), "%s", value);
}

static bool kv_del(struct kv *store, const char *key)
{
    struct kv_entry *entry = kv_find(store, key);
    if (entry == NULL)
        return false;
    /* Keep insertion order so every replica formats the map identically. */
    memmove(entry, entry + 1,
            (size_t)(&store->entries[store->count] - entry - 1) *
                sizeof(*entry));
    store->count--;
    return true;
}

/* Copies the command's scatter/gather bytes into one NUL-terminated string. */
static bool kv_flatten(const struct vsr_blob *command, char *text, size_t size)
{
    size_t length = 0;
    if (command->size >= size)
        return false;
    for (uint32_t i = 0; i < command->count; i++) {
        memcpy(text + length, command->spans[i].data, command->spans[i].size);
        length += command->spans[i].size;
    }
    text[length] = '\0';
    return true;
}

static int32_t kv_execute(struct kv *store, char *text, char *result,
                          size_t capacity)
{
    const char *verb = strtok(text, " ");
    const char *key = strtok(NULL, " ");
    const char *value = strtok(NULL, "");
    const size_t limit = capacity < KV_VALUE_BYTES ? capacity : KV_VALUE_BYTES;
    store->commands++;
    if (verb == NULL || key == NULL || strlen(key) >= KV_KEY_BYTES) {
        snprintf(result, capacity, "ERR malformed command");
        return KV_RESULT_ERROR;
    }
    if (strcmp(verb, "set") == 0 && value != NULL) {
        kv_set(store, key, value);
        snprintf(result, limit, "%s", value);
        return KV_RESULT_OK;
    }
    if (strcmp(verb, "get") == 0) {
        const char *found = kv_get(store, key);
        snprintf(result, limit, "%s", found == NULL ? "(nil)" : found);
        return KV_RESULT_OK;
    }
    if (strcmp(verb, "del") == 0) {
        snprintf(result, capacity, "%d", kv_del(store, key) ? 1 : 0);
        return KV_RESULT_OK;
    }
    if (strcmp(verb, "incr") == 0) {
        const char *current = kv_get(store, key);
        char *end = NULL;
        long long number = current == NULL ? 0 : strtoll(current, &end, 10);
        char digits[KV_VALUE_BYTES];
        if (current != NULL && (*current == '\0' || *end != '\0')) {
            snprintf(result, capacity, "ERR value is not an integer");
            return KV_RESULT_ERROR;
        }
        snprintf(digits, sizeof(digits), "%lld", number + 1);
        kv_set(store, key, digits);
        snprintf(result, limit, "%s", digits);
        return KV_RESULT_OK;
    }
    snprintf(result, capacity, "ERR unknown command '%s'", verb);
    return KV_RESULT_ERROR;
}

/* --- host application callbacks --------------------------------------- */

static void *kv_create(void)
{
    struct kv *store = calloc(1, sizeof(*store));
    CHECK(store != NULL);
    return store;
}

static void kv_destroy(void *state)
{
    free(state);
}

static void kv_reset(void *state)
{
    memset(state, 0, sizeof(struct kv));
}

/* One log entry. Reconfiguration and epoch checks are logged too, but carry
 * no application command, so they produce no result bytes. */
static int32_t kv_apply(void *state, const struct vsr_entry *entry,
                        char *result, size_t capacity, size_t *length)
{
    char text[KV_COMMAND_BYTES];
    int32_t code = KV_RESULT_OK;
    *length = 0;
    if (entry->type != VSR_REQUEST_COMMAND)
        return KV_RESULT_OK;
    if (!kv_flatten(entry->body, text, sizeof(text))) {
        snprintf(result, capacity, "ERR command too long");
        code = KV_RESULT_ERROR;
    } else {
        code = kv_execute(state, text, result, capacity);
    }
    *length = strlen(result);
    return code;
}

/* Serializes the map as "key=value\n" lines: this is the checkpoint image. */
static void *kv_capture(const void *state, size_t *size)
{
    const struct kv *store = state;
    const size_t capacity = (size_t)KV_KEYS * (KV_KEY_BYTES + KV_VALUE_BYTES);
    char *image = malloc(capacity + 1);
    size_t length = 0;
    CHECK(image != NULL);
    for (uint32_t i = 0; i < store->count; i++)
        length +=
            (size_t)snprintf(image + length, capacity + 1 - length, "%s=%s\n",
                             store->entries[i].key, store->entries[i].value);
    *size = length;
    return image;
}

/* Replaces the map with the serialized one; the command count is local. */
static void kv_install(void *state, const void *image, size_t size)
{
    struct kv *store = state;
    const char *bytes = image;
    const uint64_t commands = store->commands;
    memset(store, 0, sizeof(*store));
    store->commands = commands;
    for (size_t at = 0; at < size;) {
        const char *equals = memchr(bytes + at, '=', size - at);
        const char *newline = memchr(bytes + at, '\n', size - at);
        char key[KV_KEY_BYTES];
        char value[KV_VALUE_BYTES];
        CHECK(equals != NULL && newline != NULL && equals < newline);
        snprintf(key, sizeof(key), "%.*s", (int)(equals - (bytes + at)),
                 bytes + at);
        snprintf(value, sizeof(value), "%.*s", (int)(newline - equals - 1),
                 equals + 1);
        kv_set(store, key, value);
        at = (size_t)(newline - bytes) + 1;
    }
}

const struct mem_application kv_application = {
    kv_create, kv_destroy, kv_reset, kv_apply, kv_capture, kv_install};

const char *kv_get(const struct kv *store, const char *key)
{
    for (uint32_t i = 0; i < store->count; i++)
        if (strcmp(store->entries[i].key, key) == 0)
            return store->entries[i].value;
    return NULL;
}

const char *kv_format(const struct kv *store, char *buffer, size_t size)
{
    size_t length = 0;
    if (store->count == 0)
        return "(empty)";
    for (uint32_t i = 0; i < store->count && length < size; i++)
        length += (size_t)snprintf(buffer + length, size - length, "%s%s=%s",
                                   i == 0 ? "" : " ", store->entries[i].key,
                                   store->entries[i].value);
    return buffer;
}
