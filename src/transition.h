#ifndef VSR_TRANSITION_H
#define VSR_TRANSITION_H

#include "protocol.h"

struct vsr_transition;
int vsr_transition_size(const struct vsr_options *options, size_t *size,
                        size_t *alignment);
void vsr_transition_init(struct vsr *v, void *memory, size_t size);
int vsr_transition_event(struct vsr *v, const struct vsr_event *event,
                         uint32_t lease, bool *handled);
bool vsr_transition_poll(struct vsr *v);
void vsr_transition_complete(struct vsr *v, struct vsr_operation *operation,
                             const struct vsr_event *event, uint32_t lease);
uint64_t vsr_transition_deadline(const struct vsr *v);
void vsr_transition_stop(struct vsr *v);
bool vsr_transition_boot_loaded(struct vsr *v,
                                const struct vsr_recovered *recovered,
                                uint32_t lease);
bool vsr_transition_boot_missing(struct vsr *v);
bool vsr_transition_apply_allowed(const struct vsr *v);
uint64_t vsr_transition_min_sequence(const struct vsr *v);

bool vsr_transition_epoch_fence(struct vsr *v);
bool vsr_transition_epoch(struct vsr *v, uint64_t peer, uint64_t boundary);
bool vsr_transition_epoch_recover(struct vsr *v);
bool vsr_transition_busy(const struct vsr *v);
void vsr_transition_epoch_entered(struct vsr *v);
void vsr_transition_hard(struct vsr *v, struct vsr_hard_state *hard);

#endif
