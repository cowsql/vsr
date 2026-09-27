#ifndef VSR_EXTENSION_H
#define VSR_EXTENSION_H

#include "internal.h"

/* Cold, independently bounded protocol modules. Their arenas share the parent
 * instance but no module allocates, invokes host callbacks, or owns global data. */
struct vsr_transition;
struct vsr_checkpoint_state;
struct vsr_reads;
struct vsr_extension {
    struct vsr_transition *transition;
    struct vsr_checkpoint_state *checkpoint;
    void *epochs;
    struct vsr_reads *reads;
};

enum vsr_extension_tag {
    VSR_TAG_TRANSITION_FIRST = 256,
    VSR_TAG_CHECKPOINT_FIRST = 512,
    VSR_TAG_EPOCH_FIRST = 768,
    VSR_TAG_READ_FIRST = 1024
};

int vsr_extension_size(const struct vsr_options *options, size_t *size,
                       size_t *alignment);
void vsr_extension_init(struct vsr *v, void *memory, size_t size);
int vsr_extension_event(struct vsr *v, const struct vsr_event *event,
                        uint32_t lease, bool *handled);
bool vsr_extension_poll(struct vsr *v);
void vsr_extension_complete(struct vsr *v, struct vsr_operation *operation,
                            const struct vsr_event *event, uint32_t lease);
uint64_t vsr_extension_deadline(const struct vsr *v);
void vsr_extension_stop(struct vsr *v);
void vsr_extension_normal(struct vsr *v);

/* Called after the normal engine has restored the logical store frontiers and
 * configuration. true means extension-controlled recovery/view establishment;
 * application installation/replay still proceeds, but must not independently
 * switch the replica into NORMAL. Missing NEW is ordinary genesis startup;
 * missing RECOVER/JOIN uses discovery/recovery without inventing a new group. */
bool vsr_extension_boot_loaded(struct vsr *v,
                               const struct vsr_recovered *recovered,
                               uint32_t lease);
bool vsr_extension_boot_missing(struct vsr *v);

/* Hooks for handoff/read mutation fences. A committed notification does not
 * grant permission to apply; every application batch also checks eligibility. */
bool vsr_extension_apply_allowed(struct vsr *v, const struct vsr_entry *entry);
void vsr_extension_committed(struct vsr *v, const struct vsr_entry *entry);

uint64_t vsr_extension_min_sequence(const struct vsr *v);

#endif
