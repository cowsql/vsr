#ifndef VSR_EPOCHS_H
#define VSR_EPOCHS_H

#include "protocol.h"

struct vsr_epochs;
int vsr_epochs_size(const struct vsr_options *options, size_t *size,
                    size_t *alignment);
void vsr_epochs_init(struct vsr *v, void *memory, size_t size);
int vsr_epochs_event(struct vsr *v, const struct vsr_event *event,
                     uint32_t lease, bool *handled);
bool vsr_epochs_poll(struct vsr *v);
void vsr_epochs_complete(struct vsr *v, struct vsr_operation *operation,
                         const struct vsr_event *event, uint32_t lease);
uint64_t vsr_epochs_deadline(const struct vsr *v);
void vsr_epochs_stop(struct vsr *v);
void vsr_epochs_recovered(struct vsr *v);
void vsr_epochs_normal(struct vsr *v);
void vsr_epochs_committed(struct vsr *v, const struct vsr_entry *entry);
bool vsr_epochs_apply_allowed(const struct vsr *v,
                              const struct vsr_entry *entry);
bool vsr_epochs_ready(const struct vsr *v, uint64_t target);

#endif
