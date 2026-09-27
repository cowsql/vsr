#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

/* Exercise the planner's progress reserves with one effect slot, one log cache
 * slot, one client slot, one transfer, and one output descriptor. */
static void minimum_capacity(void)
{
    for (unsigned policy = 0; policy < 2; policy++) {
        struct vsr_member members[] = {{1, VSR_MEMBER_FULL, 0},
                                       {2, VSR_MEMBER_FULL, 0},
                                       {3, VSR_MEMBER_WITNESS, 0}};
        struct vsr_membership group = {0, members, 3, 1};
        struct mem_cluster *c = mem_cluster_create();
        struct mem_node *nodes[3];
        for (unsigned i = 0; i < 3; i++) {
            struct vsr_options o = mem_options(i + 1, &group);
            o.durability = policy;
            o.limits.operations = 1;
            o.limits.transfers = 1;
            o.limits.input_leases = 9;
            o.limits.log_cache_entries = 1;
            o.limits.batch_entries = 1;
            o.limits.client_cache_entries = 1;
            o.limits.pinned_payload_bytes = 11520;
            nodes[i] = mem_cluster_add(c, &o);
            mem_node_output_capacity(nodes[i], 1);
            CHECK(mem_node_time(nodes[i], 0).consumed == 1);
        }
        CHECK(mem_cluster_run(c, 100000) < 100000);
        for (unsigned i = 0; i < 12; i++) {
            struct vsr_blob body = {0};
            struct vsr_request req = {
                {{4, i + 1}, 1}, 0, VSR_REQUEST_COMMAND, 0, &body};
            struct vsr_event e = {VSR_EVENT_REQUEST, 0, i + 1, &req, 1};
            struct mem_step s = mem_node_event(nodes[0], &e);
            CHECK(s.consumed == 1);
            CHECK(mem_cluster_run(c, 100000) < 100000);
            struct vsr_status st;
            vsr_get_status(mem_node_core(nodes[0]), &st);
            CHECK(st.failure.code == 0);
            CHECK(st.applied == i + 1);
            CHECK(mem_node_replies(nodes[0]) == i + 1);
        }
        mem_cluster_check(c);
        mem_cluster_destroy(c);
    }
}

/* Readability must wait for the first missing STORE completion even when all
 * later physical transactions have already become readable in the adapter. */
static void reversed_stores(void)
{
    struct vsr_member m = {1, VSR_MEMBER_FULL, 0};
    struct vsr_membership group = {0, &m, 1, 0};
    struct mem_cluster *c = mem_cluster_create();
    struct vsr_options o = mem_options(1, &group);
    struct mem_node *n = mem_cluster_add(c, &o);
    mem_node_time(n, 0);
    CHECK(mem_cluster_run(c, 100000) < 100000);
    for (unsigned i = 0; i < 8; i++) {
        struct vsr_blob b = {0};
        struct vsr_request r = {{{2, i + 1}, 1}, 0, VSR_REQUEST_COMMAND, 0, &b};
        struct vsr_event e = {VSR_EVENT_REQUEST, 0, i + 1, &r, 1};
        CHECK(mem_node_event(n, &e).consumed == 1);
    }
    for (unsigned tries = 0; tries < 100; tries++) {
        bool progress = false;
        for (size_t i = 0; i < mem_node_effects(n); i++) {
            const struct vsr_op *op = mem_node_effect(n, i);
            if (op->type == VSR_OP_LOAD) {
                CHECK(mem_node_execute(n, i, VSR_IO_OK));
                CHECK(mem_node_complete(n, i, VSR_IO_OK));
                progress = true;
                break;
            }
        }
        if (!progress)
            break;
    }
    size_t stores = 0;
    for (size_t i = 0; i < mem_node_effects(n); i++) {
        const struct vsr_op *op = mem_node_effect(n, i);
        if (op->type == VSR_OP_STORE) {
            stores++;
            CHECK(mem_node_execute(n, i, VSR_IO_OK));
        }
    }
    CHECK(stores >= 2);
    struct vsr_status before, after;
    vsr_get_status(mem_node_core(n), &before);
    for (size_t i = mem_node_effects(n); i > 0; i--) {
        const struct vsr_op *op = mem_node_effect(n, i - 1);
        if (op->type == VSR_OP_STORE) {
            uint64_t seq = ((const struct vsr_store *)op->data)->sequence;
            CHECK(mem_node_complete(n, i - 1, VSR_IO_OK));
            vsr_get_status(mem_node_core(n), &after);
            if (seq > before.stored_sequence + 1)
                CHECK(after.stored_sequence == before.stored_sequence);
        }
    }
    CHECK(mem_cluster_run(c, 100000) < 100000);
    CHECK(mem_node_replies(n) == 8);
    mem_cluster_check(c);
    mem_cluster_destroy(c);
}

int main(void)
{
    minimum_capacity();
    reversed_stores();
    return 0;
}
