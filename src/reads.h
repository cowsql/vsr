#ifndef VSR_READS_H
#define VSR_READS_H

#include "internal.h"

struct vsr_reads;

int vsr_reads_size(const struct vsr_options *options, size_t *size,
                   size_t *alignment);
void vsr_reads_init(struct vsr *v, void *memory, size_t size);
int vsr_reads_event(struct vsr *v, const struct vsr_event *event,
                    uint32_t lease, bool *handled);
bool vsr_reads_poll(struct vsr *v);
void vsr_reads_complete(struct vsr *v, struct vsr_operation *operation,
                        const struct vsr_event *event, uint32_t lease);
uint64_t vsr_reads_deadline(const struct vsr *v);
void vsr_reads_stop(struct vsr *v);
/* A new NORMAL installation inherits its entire current log. A subsequent
 * current-view committed entry establishes the prerequisite for read probes. */
void vsr_reads_normal(struct vsr *v);

#endif
