#include "config.h"

#include "extension.h"

#include "checked.h"
#include "checkpoint.h"
#include "epochs.h"
#include "protocol.h"
#include "reads.h"
#include "transition.h"

#include <stdalign.h>
#include <string.h>

struct extension_plan {
    size_t size;
    size_t alignment;
    size_t transition;
    size_t transition_size;
    size_t checkpoint;
    size_t checkpoint_size;
    size_t reads;
    size_t reads_size;
    size_t epochs;
    size_t epochs_size;
};

static bool add_module(struct extension_plan *plan, size_t size,
                       size_t alignment, size_t *offset)
{
    if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
        return false;
    }
    if (alignment > plan->alignment) {
        plan->alignment = alignment;
    }
    size_t padding = (alignment - plan->size % alignment) % alignment;
    return vsr_size_add(plan->size, padding, offset) &&
           vsr_size_add(*offset, size, &plan->size);
}

static int extension_plan(const struct vsr_options *options,
                          struct extension_plan *plan)
{
    memset(plan, 0, sizeof(*plan));
    plan->size = sizeof(struct vsr_extension);
    plan->alignment = alignof(max_align_t);
    size_t alignment;
    int result =
        vsr_transition_size(options, &plan->transition_size, &alignment);
    if (result != VSR_OK)
        return result;
    if (!add_module(plan, plan->transition_size, alignment, &plan->transition))
        return VSR_ELIMIT;
    result = vsr_checkpoint_size(options, &plan->checkpoint_size, &alignment);
    if (result != VSR_OK)
        return result;
    if (!add_module(plan, plan->checkpoint_size, alignment, &plan->checkpoint))
        return VSR_ELIMIT;
    result = vsr_reads_size(options, &plan->reads_size, &alignment);
    if (result != VSR_OK)
        return result;
    if (!add_module(plan, plan->reads_size, alignment, &plan->reads))
        return VSR_ELIMIT;
    result = vsr_epochs_size(options, &plan->epochs_size, &alignment);
    if (result != VSR_OK)
        return result;
    if (!add_module(plan, plan->epochs_size, alignment, &plan->epochs))
        return VSR_ELIMIT;
    return VSR_OK;
}

int vsr_extension_size(const struct vsr_options *options, size_t *size,
                       size_t *alignment)
{
    struct extension_plan plan;
    int result = extension_plan(options, &plan);
    if (result == VSR_OK) {
        *size = plan.size;
        *alignment = plan.alignment;
    }
    return result;
}

void vsr_extension_init(struct vsr *v, void *memory, size_t size)
{
    (void)size;
    struct extension_plan plan;
    if (extension_plan(&v->options, &plan) != VSR_OK)
        return;
    unsigned char *base = memory;
    struct vsr_extension *extension = memory;
    vsr_protocol(v)->extension = extension;
    extension->transition = (void *)(base + plan.transition);
    extension->checkpoint = (void *)(base + plan.checkpoint);
    extension->reads = (void *)(base + plan.reads);
    extension->epochs = (void *)(base + plan.epochs);
    vsr_transition_init(v, extension->transition, plan.transition_size);
    vsr_checkpoint_init(v, extension->checkpoint, plan.checkpoint_size);
    vsr_reads_init(v, extension->reads, plan.reads_size);
    vsr_epochs_init(v, extension->epochs, plan.epochs_size);
}

int vsr_extension_event(struct vsr *v, const struct vsr_event *event,
                        uint32_t lease, bool *handled)
{
    *handled = false;
    if (event->type == VSR_EVENT_MESSAGE &&
        v->status.state == VSR_STATE_STARTING) {
        *handled = true;
        return VSR_OK;
    }
    int result = vsr_epochs_event(v, event, lease, handled);
    if (result != VSR_OK || *handled)
        return result;
    result = vsr_transition_event(v, event, lease, handled);
    if (result != VSR_OK || *handled)
        return result;
    if (event->type == VSR_EVENT_CHECKPOINT ||
        (event->type == VSR_EVENT_MESSAGE &&
         ((const struct vsr_message *)event->data)->type ==
             VSR_MSG_CHECKPOINT)) {
        *handled = true;
        return vsr_checkpoint_event(v, event, lease);
    }
    return vsr_reads_event(v, event, lease, handled);
}

bool vsr_extension_poll(struct vsr *v)
{
    return vsr_epochs_poll(v) || vsr_transition_poll(v) ||
           vsr_checkpoint_poll(v) || vsr_reads_poll(v);
}

void vsr_extension_complete(struct vsr *v, struct vsr_operation *operation,
                            const struct vsr_event *event, uint32_t lease)
{
    uint32_t tag = (uint32_t)operation->tag;
    if (tag < VSR_TAG_CHECKPOINT_FIRST) {
        vsr_transition_complete(v, operation, event, lease);
    } else if (tag < VSR_TAG_EPOCH_FIRST) {
        vsr_checkpoint_complete(v, operation, event, lease);
    } else if (tag < VSR_TAG_READ_FIRST) {
        vsr_epochs_complete(v, operation, event, lease);
    } else {
        vsr_reads_complete(v, operation, event, lease);
    }
}

uint64_t vsr_extension_deadline(const struct vsr *v)
{
    uint64_t result = vsr_transition_deadline(v);
    uint64_t other = vsr_checkpoint_deadline(v);
    if (other < result)
        result = other;
    other = vsr_epochs_deadline(v);
    if (other < result)
        result = other;
    other = vsr_reads_deadline(v);
    return other < result ? other : result;
}

void vsr_extension_stop(struct vsr *v)
{
    vsr_epochs_stop(v);
    vsr_transition_stop(v);
    vsr_reads_stop(v);
    vsr_checkpoint_stop(v);
}

bool vsr_extension_boot_loaded(struct vsr *v,
                               const struct vsr_recovered *recovered,
                               uint32_t lease)
{
    bool owned = vsr_transition_boot_loaded(v, recovered, lease);
    vsr_epochs_recovered(v);
    return owned;
}

bool vsr_extension_boot_missing(struct vsr *v)
{
    return vsr_transition_boot_missing(v);
}

bool vsr_extension_apply_allowed(struct vsr *v, const struct vsr_entry *entry)
{
    return vsr_epochs_apply_allowed(v, entry) &&
           vsr_transition_apply_allowed(v);
}

bool vsr_extension_apply_ready(const struct vsr *v)
{
    return vsr_transition_apply_allowed(v);
}

void vsr_extension_committed(struct vsr *v, const struct vsr_entry *entry)
{
    vsr_epochs_committed(v, entry);
}

void vsr_extension_normal(struct vsr *v)
{
    vsr_epochs_normal(v);
    vsr_reads_normal(v);
}

uint64_t vsr_extension_min_sequence(const struct vsr *v)
{
    uint64_t transition_min = vsr_transition_min_sequence(v);
    uint64_t checkpoint_min = vsr_checkpoint_min_sequence(v);
    return transition_min < checkpoint_min ? transition_min : checkpoint_min;
}
