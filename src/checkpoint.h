#ifndef VSR_CHECKPOINT_H
#define VSR_CHECKPOINT_H

#include "protocol.h"

/* This module owns extension tags [512,768). The extension allocator supplies
 * its one fixed subarena; snapshot data always remains in adapter-owned leases. */
struct vsr_checkpoint_state;
int vsr_checkpoint_size(const struct vsr_options *options, size_t *size,
                        size_t *alignment);
void vsr_checkpoint_init(struct vsr *v, void *memory, size_t size);
int vsr_checkpoint_event(struct vsr *v, const struct vsr_event *event,
                         uint32_t lease);
bool vsr_checkpoint_poll(struct vsr *v);
void vsr_checkpoint_complete(struct vsr *v, struct vsr_operation *operation,
                             const struct vsr_event *event, uint32_t lease);
uint64_t vsr_checkpoint_deadline(const struct vsr *v);
uint64_t vsr_checkpoint_min_sequence(const struct vsr *v);
void vsr_checkpoint_stop(struct vsr *v);

/* Offer owners must pin the local image until their immutable offer expires;
 * holding its payload lease alone does not preserve the adapter's snapshot. */
const struct vsr_checkpoint *vsr_checkpoint_published(const struct vsr *v,
                                                      uint32_t *lease);
bool vsr_checkpoint_pin(struct vsr *v, struct vsr_id id);
void vsr_checkpoint_unpin(struct vsr *v, struct vsr_id id);
/* Blocks local DROP while an epoch donor obligation is outstanding. */
void vsr_checkpoint_guard(struct vsr *v, bool protected);

/* Recovery acquires an existing local hold on full replicas and queues INSTALL;
 * a witness records only the remote anchor. The recovery graph remains pinned.
 * Completion is visible through busy/adoption_status below. */
bool vsr_checkpoint_recover(struct vsr *v,
                            const struct vsr_checkpoint *checkpoint,
                            uint32_t lease, uint32_t role);
/* The transition engine must first validate the selected suffix and truncate
 * divergent uncommitted entries. RESTORE preserves the remaining suffix by
 * reference. This module installs only the checkpoint; shared APPLY replays
 * every committed suffix entry afterwards. NULL selects genesis INSTALL.
 * AGAIN is returned before retaining input or changing the caller's state. */
int vsr_checkpoint_adopt(struct vsr *v, const struct vsr_checkpoint *checkpoint,
                         uint32_t lease, uint64_t peer, uint32_t role);
/* Abandon a selected source. Unissued adoption is cancelled immediately;
 * issued FETCH/SYNC drains before releasing its fence. Once RESTORE was
 * submitted its publication/INSTALL must finish to keep both state bases
 * consistent. busy() therefore may remain true after cancellation. */
void vsr_checkpoint_cancel_adoption(struct vsr *v);
bool vsr_checkpoint_busy(const struct vsr *v);
int vsr_checkpoint_adoption_status(const struct vsr *v);
/* Immutable indexed offers must not observe a safe store revision before its
 * checkpoint/log-bound metadata has been reconciled by the checkpoint poll. */
bool vsr_checkpoint_revision_ready(const struct vsr *v);

#endif /* VSR_CHECKPOINT_H */
