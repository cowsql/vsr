#include "config.h"

#define _GNU_SOURCE 1

#include "sim/sim.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#define NAME_MAX_BYTES 255u

/* ------------------------------------------------------------------------
 * Entries and inodes
 * --------------------------------------------------------------------- */

static struct vsr_sim_entry *entry_at(const struct vsr_sim_disk *disk,
                                      uint32_t index)
{
    if (index >= disk->entries_count || !disk->entries[index]->used) {
        return NULL;
    }
    return disk->entries[index];
}

static struct vsr_sim_inode *inode_at(const struct vsr_sim_disk *disk,
                                      uint32_t index)
{
    if (index >= disk->inodes_count || !disk->inodes[index]->used) {
        return NULL;
    }
    return disk->inodes[index];
}

static uint32_t entry_new(struct vsr_sim_disk *disk, uint32_t parent,
                          const char *name, size_t length, bool directory)
{
    uint32_t index = 0;
    struct vsr_sim_entry *entry;

    while (index < disk->entries_count && disk->entries[index]->used) {
        ++index;
    }
    if (index == disk->entries_count) {
        disk->entries = vsr_sim_grow_table(disk->entries, (size_t)index + 1);
        disk->entries[index] = vsr_sim_alloc(sizeof(struct vsr_sim_entry));
        ++disk->entries_count;
    }
    entry = disk->entries[index];
    memset(entry, 0, sizeof(*entry));
    entry->used = 1;
    entry->parent = parent;
    entry->directory = directory;
    entry->inode = VSR_SIM_NONE;
    entry->name = vsr_sim_alloc(length + 1);
    memcpy(entry->name, name, length);
    entry->name[length] = '\0';
    return index;
}

static void entry_free(struct vsr_sim_disk *disk, uint32_t index)
{
    struct vsr_sim_entry *entry = disk->entries[index];

    free(entry->name);
    entry->name = NULL;
    entry->used = 0;
}

static uint32_t inode_new(struct vsr_sim_disk *disk, uint32_t mode)
{
    uint32_t index = 0;
    struct vsr_sim_inode *inode;

    while (index < disk->inodes_count && disk->inodes[index]->used) {
        ++index;
    }
    if (index == disk->inodes_count) {
        disk->inodes = vsr_sim_grow_table(disk->inodes, (size_t)index + 1);
        disk->inodes[index] = vsr_sim_alloc(sizeof(struct vsr_sim_inode));
        ++disk->inodes_count;
    }
    inode = disk->inodes[index];
    memset(inode, 0, sizeof(*inode));
    inode->used = 1;
    inode->mode = mode;
    return index;
}

static void inode_truncate(struct vsr_sim_disk *disk,
                           struct vsr_sim_inode *inode)
{
    for (uint64_t i = 0; i < inode->block_count; ++i) {
        if (inode->blocks[i].bytes != NULL) {
            disk->used_bytes -= disk->block_bytes;
        }
        free(inode->blocks[i].bytes);
        free(inode->blocks[i].durable);
    }
    free(inode->blocks);
    inode->blocks = NULL;
    inode->block_count = 0;
    inode->size = 0;
    inode->truncated_at = ++disk->version;
}

static void inode_maybe_free(struct vsr_sim_disk *disk, uint32_t index)
{
    struct vsr_sim_inode *inode = inode_at(disk, index);

    if (inode == NULL || inode->links > 0 || inode->opens > 0) {
        return;
    }
    inode_truncate(disk, inode);
    inode->used = 0;
}

int vsr_sim_disk_init(struct vsr_sim_disk *disk, uint64_t block_bytes)
{
    struct vsr_sim_entry *root;

    memset(disk, 0, sizeof(*disk));
    disk->block_bytes = block_bytes;
    /* The root is the one allocation create reports instead of aborting. */
    disk->entries = calloc(1, sizeof(void *)); /* A table of pointers. */
    root = calloc(1, sizeof(*root));
    if (disk->entries == NULL || root == NULL) {
        free(disk->entries);
        free(root);
        disk->entries = NULL;
        return -ENOMEM;
    }
    root->name = calloc(1, 1);
    if (root->name == NULL) {
        free(disk->entries);
        free(root);
        disk->entries = NULL;
        return -ENOMEM;
    }
    root->used = 1;
    root->directory = 1;
    root->inode = VSR_SIM_NONE;
    root->mode = 0755;
    disk->entries[0] = root;
    disk->entries_count = 1;
    return 0;
}

void vsr_sim_disk_free(struct vsr_sim_disk *disk)
{
    for (uint32_t i = 0; i < disk->entries_count; ++i) {
        free(disk->entries[i]->name);
        free(disk->entries[i]);
    }
    free(disk->entries);
    for (uint32_t i = 0; i < disk->inodes_count; ++i) {
        struct vsr_sim_inode *inode = disk->inodes[i];

        for (uint64_t b = 0; b < inode->block_count; ++b) {
            free(inode->blocks[b].bytes);
            free(inode->blocks[b].durable);
        }
        free(inode->blocks);
        free(inode);
    }
    free(disk->inodes);
    memset(disk, 0, sizeof(*disk));
}

/* ------------------------------------------------------------------------
 * Paths
 * --------------------------------------------------------------------- */

static uint32_t child(const struct vsr_sim_disk *disk, uint32_t parent,
                      const char *name, size_t length)
{
    for (uint32_t i = 1; i < disk->entries_count; ++i) {
        const struct vsr_sim_entry *entry = disk->entries[i];

        if (entry->used && !entry->removed && entry->parent == parent &&
            strlen(entry->name) == length &&
            memcmp(entry->name, name, length) == 0) {
            return i;
        }
    }
    return VSR_SIM_NONE;
}

static bool has_children(const struct vsr_sim_disk *disk, uint32_t parent)
{
    for (uint32_t i = 1; i < disk->entries_count; ++i) {
        const struct vsr_sim_entry *entry = disk->entries[i];

        if (i != parent && entry->used && !entry->removed &&
            entry->parent == parent) {
            return true;
        }
    }
    return false;
}

struct lookup {
    uint32_t parent; /* Directory holding the last component. */
    uint32_t entry;  /* The named entry, or NONE. */
    const char *leaf;
    size_t leaf_length; /* 0: the path names a directory by "."/"..". */
};

static const char *next_component(const char *at, size_t *length)
{
    while (*at == '/') {
        ++at;
    }
    *length = 0;
    while (at[*length] != '\0' && at[*length] != '/') {
        ++*length;
    }
    return at;
}

/* Resolves every component but the last, which may be missing. */
static int lookup(const struct vsr_sim_disk *disk, uint32_t start,
                  const char *path, struct lookup *out)
{
    const struct vsr_sim_entry *base = entry_at(disk, start);
    uint32_t directory = path[0] == '/' ? 0 : start;
    const char *at = path;
    size_t length;

    if (path[0] == '\0') {
        return -ENOENT;
    }
    if (base == NULL || base->removed || !base->directory) {
        return -ENOENT;
    }
    out->parent = directory;
    out->entry = directory;
    out->leaf = NULL;
    out->leaf_length = 0;
    at = next_component(at, &length);
    while (length > 0) {
        const char *following;
        size_t following_length;
        uint32_t found;

        if (length > NAME_MAX_BYTES) {
            return -ENAMETOOLONG;
        }
        following = next_component(at + length, &following_length);
        if (length == 1 && at[0] == '.') {
            found = directory;
        } else if (length == 2 && at[0] == '.' && at[1] == '.') {
            found = disk->entries[directory]->parent;
        } else {
            found = child(disk, directory, at, length);
        }
        if (following_length == 0) {
            bool dots = (length == 1 && at[0] == '.') ||
                        (length == 2 && at[0] == '.' && at[1] == '.');

            out->parent = dots ? disk->entries[found]->parent : directory;
            out->entry = found;
            out->leaf = dots ? NULL : at;
            out->leaf_length = dots ? 0 : length;
            return 0;
        }
        if (found == VSR_SIM_NONE) {
            return -ENOENT;
        }
        if (!disk->entries[found]->directory) {
            return -ENOTDIR;
        }
        directory = found;
        at = following;
        length = following_length;
    }
    /* "/" alone, or only slashes. */
    out->parent = 0;
    out->entry = directory;
    return 0;
}

/* The directory a path record starts from: the node root or an open
 * directory descriptor. */
static int start_directory(struct vsr_sim_node *node, int32_t fd,
                           uint32_t *entry, uint32_t *object)
{
    const struct vsr_sim_object *target;
    int error;

    *object = VSR_SIM_NONE;
    if (fd == VSR_SIM_ROOT) {
        *entry = 0;
        return 0;
    }
    error = vsr_sim_exec_resolve(node, fd, false, object);
    if (error != 0) {
        return error;
    }
    target = vsr_sim_object(node, *object);
    if (target->kind != VSR_SIM_OBJECT_DIR) {
        return -ENOTDIR;
    }
    *entry = target->inode;
    return 0;
}

static void remove_entry(struct vsr_sim_disk *disk, uint32_t index)
{
    struct vsr_sim_entry *entry = disk->entries[index];

    if (entry->directory) {
        if (entry->opens > 0) {
            entry->removed = 1;
            return;
        }
        entry_free(disk, index);
        return;
    }
    {
        uint32_t inode = entry->inode;

        entry_free(disk, index);
        --disk->inodes[inode]->links;
        inode_maybe_free(disk, inode);
    }
}

/* ------------------------------------------------------------------------
 * Blocks
 * --------------------------------------------------------------------- */

static void reserve_blocks(struct vsr_sim_inode *inode, uint64_t count)
{
    if (count <= inode->block_count) {
        return;
    }
    inode->blocks =
        vsr_sim_grow(inode->blocks, (size_t)count, sizeof(*inode->blocks));
    memset(&inode->blocks[inode->block_count], 0,
           sizeof(*inode->blocks) * (size_t)(count - inode->block_count));
    inode->block_count = count;
}

static uint64_t missing_blocks(const struct vsr_sim_disk *disk,
                               const struct vsr_sim_inode *inode,
                               uint64_t offset, uint64_t length)
{
    uint64_t first;
    uint64_t last;
    uint64_t missing = 0;

    if (length == 0) {
        return 0;
    }
    first = offset / disk->block_bytes;
    last = (offset + length - 1) / disk->block_bytes;
    for (uint64_t b = first; b <= last; ++b) {
        if (b >= inode->block_count || inode->blocks[b].bytes == NULL) {
            ++missing;
        }
    }
    return missing;
}

static bool fits(const struct vsr_sim_node *node, uint64_t blocks)
{
    uint64_t capacity = node->world->faults.disk.capacity_bytes;
    const struct vsr_sim_disk *disk = &node->disk;

    if (capacity == 0 || blocks == 0) {
        return true;
    }
    return blocks <= (UINT64_MAX - disk->used_bytes) / disk->block_bytes &&
           disk->used_bytes + blocks * disk->block_bytes <= capacity;
}

static unsigned char *block_bytes(struct vsr_sim_disk *disk,
                                  struct vsr_sim_inode *inode, uint64_t b)
{
    reserve_blocks(inode, b + 1);
    if (inode->blocks[b].bytes == NULL) {
        inode->blocks[b].bytes = vsr_sim_alloc((size_t)disk->block_bytes);
        disk->used_bytes += disk->block_bytes;
    }
    return inode->blocks[b].bytes;
}

static void make_durable(const struct vsr_sim_disk *disk,
                         struct vsr_sim_block *block)
{
    if (block->bytes == NULL) {
        return;
    }
    if (block->durable == NULL) {
        block->durable = vsr_sim_alloc((size_t)disk->block_bytes);
    }
    memcpy(block->durable, block->bytes, (size_t)disk->block_bytes);
    block->synced_at = block->written_at;
    block->dirty = 0;
}

/* Copies [offset, offset + size) of the file's current or durable content
 * into bytes; returns the count, short at the end of the file. */
static size_t read_range(const struct vsr_sim_disk *disk,
                         const struct vsr_sim_inode *inode, uint64_t offset,
                         unsigned char *bytes, size_t size, bool durable)
{
    size_t count;
    size_t done = 0;

    if (offset >= inode->size) {
        return 0;
    }
    count = inode->size - offset < size ? (size_t)(inode->size - offset) : size;
    while (done < count) {
        uint64_t at = offset + done;
        uint64_t b = at / disk->block_bytes;
        size_t within = (size_t)(at % disk->block_bytes);
        size_t take = (size_t)disk->block_bytes - within;
        const unsigned char *source = NULL;

        if (take > count - done) {
            take = count - done;
        }
        if (b < inode->block_count) {
            source =
                durable ? inode->blocks[b].durable : inode->blocks[b].bytes;
        }
        if (source == NULL) {
            memset(bytes + done, 0, take);
        } else {
            memcpy(bytes + done, source + within, take);
        }
        done += take;
    }
    return count;
}

static void write_range(struct vsr_sim_disk *disk, struct vsr_sim_inode *inode,
                        uint64_t offset, const unsigned char *bytes,
                        size_t size, bool sync)
{
    size_t done = 0;

    while (done < size) {
        uint64_t at = offset + done;
        uint64_t b = at / disk->block_bytes;
        size_t within = (size_t)(at % disk->block_bytes);
        size_t take = (size_t)disk->block_bytes - within;
        unsigned char *target = block_bytes(disk, inode, b);
        struct vsr_sim_block *block = &inode->blocks[b];

        if (take > size - done) {
            take = size - done;
        }
        memcpy(target + within, bytes + done, take);
        block->written_at = ++disk->version;
        block->dirty = 1;
        if (sync) {
            make_durable(disk, block);
        }
        done += take;
    }
    if (offset + size > inode->size) {
        inode->size = offset + size;
    }
}

/* ------------------------------------------------------------------------
 * Record starts
 * --------------------------------------------------------------------- */

static bool aligned(uint64_t value, uint64_t alignment)
{
    return value % alignment == 0;
}

static int check_data(struct vsr_sim_node *node, const struct vsr_sim_op *op,
                      const struct vsr_sim_object *object)
{
    const struct vsr_io_sqe *sqe = &op->sqe;
    bool vectored =
        sqe->opcode == VSR_IO_SQE_READV || sqe->opcode == VSR_IO_SQE_WRITEV;
    bool direct = (object->flags & O_DIRECT) != 0;
    uint64_t block = node->disk.block_bytes;
    uint64_t total = 0;

    if (vectored) {
        for (uint32_t i = 0; i < op->vec_count; ++i) {
            const struct vsr_io_vec *vec = &op->vecs[i];

            if (vec->base == NULL && vec->length > 0) {
                return -EFAULT;
            }
            if ((sqe->flags & VSR_IO_SQE_FIXED_BUFFER) != 0 &&
                vsr_sim_exec_region(node, sqe, vec->base, vec->length) != 0) {
                return -EFAULT;
            }
            /* O_DIRECT: offset and length must be block multiples; a
             * misaligned address is served, as the kernel bounces it
             * (decision 65). */
            if (direct && !aligned(vec->length, block)) {
                return -EINVAL;
            }
            total += vec->length;
        }
    } else {
        if (sqe->addr == NULL && sqe->length > 0) {
            return -EFAULT;
        }
        if ((sqe->flags & VSR_IO_SQE_FIXED_BUFFER) != 0 &&
            vsr_sim_exec_region(node, sqe, sqe->addr, sqe->length) != 0) {
            return -EFAULT;
        }
        if (direct && !aligned(sqe->length, block)) {
            return -EINVAL;
        }
        total = sqe->length;
    }
    if (direct && !aligned(sqe->offset, block)) {
        return -EINVAL;
    }
    if (total > INT32_MAX || sqe->offset > INT64_MAX - total) {
        return -EINVAL;
    }
    return 0;
}

static void capture(struct vsr_sim_node *node, struct vsr_sim_op *op,
                    const struct vsr_sim_inode *inode)
{
    uint64_t count = 0;

    for (uint64_t b = 0; b < inode->block_count; ++b) {
        if (inode->blocks[b].dirty && inode->blocks[b].bytes != NULL) {
            ++count;
        }
    }
    op->captures = vsr_sim_alloc(sizeof(*op->captures) * (size_t)(count + 1));
    op->capture_count = 0;
    for (uint64_t b = 0; b < inode->block_count; ++b) {
        const struct vsr_sim_block *block = &inode->blocks[b];
        struct vsr_sim_capture *copy;

        if (!block->dirty || block->bytes == NULL) {
            continue;
        }
        copy = &op->captures[op->capture_count++];
        copy->block = b;
        copy->version = block->written_at;
        copy->bytes = vsr_sim_alloc((size_t)node->disk.block_bytes);
        memcpy(copy->bytes, block->bytes, (size_t)node->disk.block_bytes);
    }
}

static void arm(struct vsr_sim_node *node, uint32_t index, bool sync)
{
    struct vsr_sim *sim = node->world;
    const struct vsr_sim_disk_faults *faults = &sim->faults.disk;
    uint64_t latency =
        sync ? vsr_sim_range(sim, faults->fsync_min_ns, faults->fsync_max_ns)
             : vsr_sim_range(sim, faults->latency_min_ns,
                             faults->latency_max_ns);

    vsr_sim_exec_arm(node, index, vsr_sim_add(sim->now_ns, latency),
                     VSR_SIM_ACTION_DISK);
}

void vsr_sim_disk_start(struct vsr_sim_node *node, uint32_t index)
{
    struct vsr_sim_op *op = vsr_sim_op(node, index);
    const struct vsr_io_sqe *sqe = &op->sqe;
    uint32_t object_index;
    const struct vsr_sim_object *object;
    uint32_t access;
    int error;

    switch (sqe->opcode) {
    case VSR_IO_SQE_OPENAT:
    case VSR_IO_SQE_RENAMEAT:
    case VSR_IO_SQE_UNLINKAT:
    case VSR_IO_SQE_MKDIRAT:
    case VSR_IO_SQE_STATX: {
        uint32_t entry = 0;

        if ((sqe->flags & VSR_IO_SQE_FIXED_FILE) != 0) {
            vsr_sim_exec_complete(node, index, -EINVAL, 0, 0);
            return;
        }
        error = start_directory(node, sqe->fd, &entry, &object_index);
        if (error == -ENOTDIR && sqe->opcode == VSR_IO_SQE_STATX &&
            (sqe->op_flags & AT_EMPTY_PATH) != 0) {
            error = 0;
        }
        if (error != 0) {
            vsr_sim_exec_complete(node, index, error, 0, 0);
            return;
        }
        if (object_index != VSR_SIM_NONE) {
            vsr_sim_exec_hold(node, index, object_index);
        }
        arm(node, index, false);
        return;
    }
    default:
        break;
    }
    error = vsr_sim_exec_resolve(node, sqe->fd,
                                 (sqe->flags & VSR_IO_SQE_FIXED_FILE) != 0,
                                 &object_index);
    if (error != 0) {
        vsr_sim_exec_complete(node, index, error, 0, 0);
        return;
    }
    object = vsr_sim_object(node, object_index);
    access = object->flags & O_ACCMODE;
    if (sqe->opcode == VSR_IO_SQE_FSYNC) {
        if (object->kind == VSR_SIM_OBJECT_SOCKET ||
            (sqe->op_flags & ~(uint32_t)VSR_IO_FSYNC_DATASYNC) != 0) {
            vsr_sim_exec_complete(node, index, -EINVAL, 0, 0);
            return;
        }
        vsr_sim_exec_hold(node, index, object_index);
        if (object->kind == VSR_SIM_OBJECT_FILE) {
            capture(node, op, node->disk.inodes[object->inode]);
        }
        arm(node, index, true);
        return;
    }
    if (object->kind == VSR_SIM_OBJECT_DIR) {
        vsr_sim_exec_complete(node, index, -EISDIR, 0, 0);
        return;
    }
    if (object->kind != VSR_SIM_OBJECT_FILE) {
        vsr_sim_exec_complete(node, index, -EINVAL, 0, 0);
        return;
    }
    switch (sqe->opcode) {
    case VSR_IO_SQE_READ:
    case VSR_IO_SQE_READV:
        error = access == O_WRONLY ? -EBADF : check_data(node, op, object);
        break;
    case VSR_IO_SQE_WRITE:
    case VSR_IO_SQE_WRITEV:
        error = access == O_RDONLY ? -EBADF : check_data(node, op, object);
        break;
    case VSR_IO_SQE_FALLOCATE:
        if (access == O_RDONLY) {
            error = -EBADF;
        } else if (sqe->op_flags != 0 || sqe->length == 0 ||
                   sqe->offset > (uint64_t)INT64_MAX - sqe->length) {
            error = -EINVAL;
        }
        break;
    default:
        error = -EINVAL;
        break;
    }
    if (error != 0) {
        vsr_sim_exec_complete(node, index, error, 0, 0);
        return;
    }
    vsr_sim_exec_hold(node, index, object_index);
    arm(node, index, false);
}

/* ------------------------------------------------------------------------
 * Record effects, applied when the latency elapses
 * --------------------------------------------------------------------- */

static int finish_read(struct vsr_sim_node *node, const struct vsr_sim_op *op,
                       struct vsr_sim_inode *inode)
{
    struct vsr_sim *sim = node->world;
    const struct vsr_io_sqe *sqe = &op->sqe;
    const struct vsr_sim_disk *disk = &node->disk;
    uint64_t offset = sqe->offset;
    size_t total = 0;
    uint32_t count = sqe->opcode == VSR_IO_SQE_READ ? 1 : op->vec_count;

    for (uint32_t i = 0; i < count; ++i) {
        unsigned char *target =
            sqe->opcode == VSR_IO_SQE_READ
                ? (unsigned char *)vsr_sim_mutable(sqe->addr)
                : (unsigned char *)op->vecs[i].base;
        size_t size =
            sqe->opcode == VSR_IO_SQE_READ ? sqe->length : op->vecs[i].length;
        size_t got = size == 0 ? 0
                               : read_range(disk, inode, offset + total, target,
                                            size, false);

        /* Bit rot per block read: flipped bytes, no error. */
        for (size_t done = 0; done < got;) {
            uint64_t at = offset + total + done;
            size_t within = (size_t)(at % disk->block_bytes);
            size_t take = (size_t)disk->block_bytes - within;

            if (take > got - done) {
                take = got - done;
            }
            if (vsr_sim_chance(sim, sim->faults.disk.bitrot_ppm)) {
                uint64_t pick = vsr_sim_pcg_below(&sim->random, take);
                uint64_t flip = 1 + vsr_sim_pcg_below(&sim->random, 255);

                target[done + pick] =
                    (unsigned char)(target[done + pick] ^ flip);
                vsr_sim_emit(sim, VSR_SIM_TRACE_BITROT, node->index,
                             VSR_SIM_NO_NODE, at - within);
            }
            done += take;
        }
        total += got;
        if (got < size) {
            break;
        }
    }
    return (int)total;
}

static int finish_write(struct vsr_sim_node *node, const struct vsr_sim_op *op,
                        const struct vsr_sim_object *object,
                        struct vsr_sim_inode *inode)
{
    struct vsr_sim *sim = node->world;
    const struct vsr_io_sqe *sqe = &op->sqe;
    bool sync = (object->flags & (O_DSYNC | O_SYNC)) != 0;
    uint64_t length = op->requested;
    uint64_t offset = sqe->offset;

    if (vsr_sim_chance(sim, sim->faults.disk.enospc_ppm) ||
        !fits(node, missing_blocks(&node->disk, inode, offset, length))) {
        return -ENOSPC;
    }
    if (sqe->opcode == VSR_IO_SQE_WRITE) {
        write_range(&node->disk, inode, offset, sqe->addr, sqe->length, sync);
        return (int)sqe->length;
    }
    for (uint32_t i = 0; i < op->vec_count; ++i) {
        write_range(&node->disk, inode, offset, op->vecs[i].base,
                    op->vecs[i].length, sync);
        offset += op->vecs[i].length;
    }
    return (int)length;
}

static int finish_fsync(struct vsr_sim_node *node, const struct vsr_sim_op *op,
                        struct vsr_sim_inode *inode)
{
    for (uint64_t i = 0; i < op->capture_count; ++i) {
        struct vsr_sim_capture *copy = &op->captures[i];
        struct vsr_sim_block *block;

        if (copy->block >= inode->block_count ||
            copy->version <= inode->truncated_at) {
            continue;
        }
        block = &inode->blocks[copy->block];
        if (block->bytes == NULL || copy->version <= block->synced_at) {
            continue;
        }
        free(block->durable);
        block->durable = copy->bytes;
        copy->bytes = NULL;
        block->synced_at = copy->version;
        if (block->written_at == copy->version) {
            block->dirty = 0;
        }
    }
    (void)node;
    return 0;
}

static int finish_fallocate(struct vsr_sim_node *node,
                            const struct vsr_sim_op *op,
                            struct vsr_sim_inode *inode)
{
    struct vsr_sim *sim = node->world;
    uint64_t offset = op->sqe.offset;
    uint64_t length = op->sqe.length;

    if (vsr_sim_chance(sim, sim->faults.disk.enospc_ppm) ||
        !fits(node, missing_blocks(&node->disk, inode, offset, length))) {
        return -ENOSPC;
    }
    for (uint64_t b = offset / node->disk.block_bytes;
         b <= (offset + length - 1) / node->disk.block_bytes; ++b) {
        (void)block_bytes(&node->disk, inode, b);
    }
    if (offset + length > inode->size) {
        inode->size = offset + length;
    }
    return 0;
}

static int finish_openat(struct vsr_sim_node *node, const struct vsr_sim_op *op,
                         uint32_t start, int *result)
{
    struct vsr_sim_disk *disk = &node->disk;
    const struct vsr_io_sqe *sqe = &op->sqe;
    uint32_t flags = sqe->op_flags;
    uint32_t access = flags & O_ACCMODE;
    struct lookup found;
    uint32_t object;
    int error = lookup(disk, start, op->path, &found);

    if (error != 0) {
        return error;
    }
    error = vsr_sim_exec_install(node, sqe, 0, true);
    if (error != 0) {
        return error;
    }
    if (found.entry != VSR_SIM_NONE) {
        struct vsr_sim_entry *entry = disk->entries[found.entry];

        if ((flags & O_CREAT) != 0 && (flags & O_EXCL) != 0) {
            return -EEXIST;
        }
        if (entry->directory) {
            if (access != O_RDONLY || (flags & O_TRUNC) != 0) {
                return -EISDIR;
            }
            object = vsr_sim_object_new(node, VSR_SIM_OBJECT_DIR);
            vsr_sim_object(node, object)->inode = found.entry;
            vsr_sim_object(node, object)->flags = flags;
            ++entry->opens;
            *result = vsr_sim_exec_install(node, sqe, object, false);
            return 0;
        }
        if ((flags & O_DIRECTORY) != 0) {
            return -ENOTDIR;
        }
        if ((flags & O_TRUNC) != 0 && access != O_RDONLY) {
            inode_truncate(disk, disk->inodes[entry->inode]);
        }
        object = vsr_sim_object_new(node, VSR_SIM_OBJECT_FILE);
        vsr_sim_object(node, object)->inode = entry->inode;
        vsr_sim_object(node, object)->flags = flags;
        ++disk->inodes[entry->inode]->opens;
        *result = vsr_sim_exec_install(node, sqe, object, false);
        return 0;
    }
    if ((flags & O_CREAT) == 0 || found.leaf == NULL) {
        return -ENOENT;
    }
    if ((flags & O_DIRECTORY) != 0) {
        return -EINVAL;
    }
    {
        uint32_t inode = inode_new(disk, sqe->length & 07777u);
        uint32_t entry =
            entry_new(disk, found.parent, found.leaf, found.leaf_length, false);

        disk->entries[entry]->inode = inode;
        disk->inodes[inode]->links = 1;
        disk->inodes[inode]->opens = 1;
        object = vsr_sim_object_new(node, VSR_SIM_OBJECT_FILE);
        vsr_sim_object(node, object)->inode = inode;
        vsr_sim_object(node, object)->flags = flags;
        *result = vsr_sim_exec_install(node, sqe, object, false);
    }
    return 0;
}

static bool inside(const struct vsr_sim_disk *disk, uint32_t entry,
                   uint32_t ancestor)
{
    for (;;) {
        if (entry == ancestor) {
            return true;
        }
        if (entry == 0) {
            return false;
        }
        entry = disk->entries[entry]->parent;
    }
}

static int finish_rename(struct vsr_sim_node *node, const struct vsr_sim_op *op,
                         uint32_t start)
{
    struct vsr_sim_disk *disk = &node->disk;
    struct lookup from;
    struct lookup to;
    struct vsr_sim_entry *source;
    int error;

    if (op->sqe.op_flags != 0) {
        return -EINVAL;
    }
    error = lookup(disk, start, op->path, &from);
    if (error == 0) {
        error = lookup(disk, start, op->path2, &to);
    }
    if (error != 0) {
        return error;
    }
    if (from.entry == VSR_SIM_NONE) {
        return -ENOENT;
    }
    if (from.leaf == NULL || to.leaf == NULL || from.entry == 0) {
        return -EBUSY;
    }
    if (from.entry == to.entry) {
        return 0;
    }
    source = disk->entries[from.entry];
    if (source->directory && inside(disk, to.parent, from.entry)) {
        return -EINVAL;
    }
    if (to.entry != VSR_SIM_NONE) {
        const struct vsr_sim_entry *target = disk->entries[to.entry];

        if (source->directory && !target->directory) {
            return -ENOTDIR;
        }
        if (!source->directory && target->directory) {
            return -EISDIR;
        }
        if (target->directory && has_children(disk, to.entry)) {
            return -ENOTEMPTY;
        }
        remove_entry(disk, to.entry);
    }
    source = disk->entries[from.entry];
    free(source->name);
    source->name = vsr_sim_alloc(to.leaf_length + 1);
    memcpy(source->name, to.leaf, to.leaf_length);
    source->name[to.leaf_length] = '\0';
    source->parent = to.parent;
    return 0;
}

static int finish_unlink(struct vsr_sim_node *node, const struct vsr_sim_op *op,
                         uint32_t start)
{
    struct vsr_sim_disk *disk = &node->disk;
    bool directory = (op->sqe.op_flags & AT_REMOVEDIR) != 0;
    struct lookup found;
    int error;

    if ((op->sqe.op_flags & ~(uint32_t)AT_REMOVEDIR) != 0) {
        return -EINVAL;
    }
    error = lookup(disk, start, op->path, &found);
    if (error != 0) {
        return error;
    }
    if (found.entry == VSR_SIM_NONE) {
        return -ENOENT;
    }
    if (found.entry == 0 || found.leaf == NULL) {
        return -EBUSY;
    }
    if (disk->entries[found.entry]->directory != directory) {
        return directory ? -ENOTDIR : -EISDIR;
    }
    if (directory && has_children(disk, found.entry)) {
        return -ENOTEMPTY;
    }
    remove_entry(disk, found.entry);
    return 0;
}

static int finish_mkdir(struct vsr_sim_node *node, const struct vsr_sim_op *op,
                        uint32_t start)
{
    struct vsr_sim_disk *disk = &node->disk;
    struct lookup found;
    uint32_t entry;
    int error = lookup(disk, start, op->path, &found);

    if (error != 0) {
        return error;
    }
    if (found.entry != VSR_SIM_NONE || found.leaf == NULL) {
        return -EEXIST;
    }
    entry = entry_new(disk, found.parent, found.leaf, found.leaf_length, true);
    disk->entries[entry]->mode = op->sqe.length & 07777u;
    return 0;
}

static int finish_statx(struct vsr_sim_node *node, const struct vsr_sim_op *op,
                        uint32_t start, uint32_t object)
{
    struct vsr_sim_disk *disk = &node->disk;
    struct statx result;
    uint32_t entry = VSR_SIM_NONE;
    uint32_t inode = VSR_SIM_NONE;

    if (op->sqe.addr2 == NULL) {
        return -EFAULT;
    }
    if (op->path[0] == '\0' && (op->sqe.op_flags & AT_EMPTY_PATH) != 0) {
        const struct vsr_sim_object *target = vsr_sim_object(node, object);

        if (target == NULL) {
            entry = start;
        } else if (target->kind == VSR_SIM_OBJECT_DIR) {
            entry = target->inode;
        } else if (target->kind == VSR_SIM_OBJECT_FILE) {
            inode = target->inode;
        } else {
            return -EINVAL;
        }
    } else {
        struct lookup found;
        int error;

        if (object != VSR_SIM_NONE &&
            vsr_sim_object(node, object)->kind != VSR_SIM_OBJECT_DIR) {
            return -ENOTDIR;
        }
        error = lookup(disk, start, op->path, &found);
        if (error != 0) {
            return error;
        }
        if (found.entry == VSR_SIM_NONE) {
            return -ENOENT;
        }
        entry = found.entry;
    }
    if (entry != VSR_SIM_NONE && !disk->entries[entry]->directory) {
        inode = disk->entries[entry]->inode;
    }
    memset(&result, 0, sizeof(result));
    result.stx_mask = STATX_TYPE | STATX_MODE | STATX_SIZE;
    result.stx_blksize = (uint32_t)disk->block_bytes;
    if (inode != VSR_SIM_NONE) {
        result.stx_mode = (uint16_t)(S_IFREG | disk->inodes[inode]->mode);
        result.stx_size = disk->inodes[inode]->size;
    } else {
        result.stx_mode = (uint16_t)(S_IFDIR | disk->entries[entry]->mode);
    }
    memcpy(vsr_sim_mutable(op->sqe.addr2), &result, sizeof(result));
    return 0;
}

void vsr_sim_disk_finish(struct vsr_sim_node *node, uint32_t index)
{
    struct vsr_sim *sim = node->world;
    struct vsr_sim_op *op = vsr_sim_op(node, index);
    const struct vsr_io_sqe *sqe = &op->sqe;
    const struct vsr_sim_object *object = vsr_sim_object(node, op->object);
    struct vsr_sim_inode *inode = NULL;
    int result = 0;

    if (object != NULL && object->kind == VSR_SIM_OBJECT_FILE) {
        inode = node->disk.inodes[object->inode];
    }
    switch (sqe->opcode) {
    case VSR_IO_SQE_READ:
    case VSR_IO_SQE_READV:
    case VSR_IO_SQE_WRITE:
    case VSR_IO_SQE_WRITEV:
    case VSR_IO_SQE_FALLOCATE:
    case VSR_IO_SQE_FSYNC:
        if (vsr_sim_chance(sim, sim->faults.disk.error_ppm)) {
            result = -EIO;
            break;
        }
        if (inode == NULL) {
            result = 0; /* FSYNC of a directory. */
        } else if (sqe->opcode == VSR_IO_SQE_READ ||
                   sqe->opcode == VSR_IO_SQE_READV) {
            result = finish_read(node, op, inode);
        } else if (sqe->opcode == VSR_IO_SQE_FSYNC) {
            result = finish_fsync(node, op, inode);
        } else if (sqe->opcode == VSR_IO_SQE_FALLOCATE) {
            result = finish_fallocate(node, op, inode);
        } else {
            result = finish_write(node, op, object, inode);
        }
        break;
    default: {
        uint32_t start = 0;

        if (object != NULL) {
            start = object->kind == VSR_SIM_OBJECT_DIR ? object->inode : 0;
        }
        switch (sqe->opcode) {
        case VSR_IO_SQE_OPENAT: {
            int error = finish_openat(node, op, start, &result);

            if (error != 0) {
                result = error;
            }
            break;
        }
        case VSR_IO_SQE_RENAMEAT:
            result = finish_rename(node, op, start);
            break;
        case VSR_IO_SQE_UNLINKAT:
            result = finish_unlink(node, op, start);
            break;
        case VSR_IO_SQE_MKDIRAT:
            result = finish_mkdir(node, op, start);
            break;
        case VSR_IO_SQE_STATX:
            result = finish_statx(node, op, start, op->object);
            break;
        default:
            result = -EINVAL;
            break;
        }
        break;
    }
    }
    vsr_sim_exec_complete(node, index, result, 0, 0);
}

void vsr_sim_disk_close(struct vsr_sim_node *node, uint32_t index)
{
    struct vsr_sim_disk *disk = &node->disk;
    const struct vsr_sim_object *object = vsr_sim_object(node, index);

    if (object->kind == VSR_SIM_OBJECT_FILE) {
        struct vsr_sim_inode *inode = inode_at(disk, object->inode);

        if (inode != NULL && inode->opens > 0) {
            --inode->opens;
            inode_maybe_free(disk, object->inode);
        }
    } else if (object->kind == VSR_SIM_OBJECT_DIR) {
        struct vsr_sim_entry *entry = entry_at(disk, object->inode);

        if (entry != NULL && entry->opens > 0) {
            --entry->opens;
            if (entry->removed && entry->opens == 0) {
                entry_free(disk, object->inode);
            }
        }
    }
}

/* ------------------------------------------------------------------------
 * Crash model
 * --------------------------------------------------------------------- */

void vsr_sim_disk_crash(struct vsr_sim *sim, struct vsr_sim_node *node)
{
    struct vsr_sim_disk *disk = &node->disk;
    uint32_t keep = sim->faults.disk.unsynced_keep_ppm;

    for (uint32_t i = 0; i < disk->inodes_count; ++i) {
        struct vsr_sim_inode *inode = disk->inodes[i];

        if (!inode->used) {
            continue;
        }
        for (uint64_t b = 0; b < inode->block_count; ++b) {
            struct vsr_sim_block *block = &inode->blocks[b];

            if (!block->dirty || block->bytes == NULL) {
                continue;
            }
            if (vsr_sim_chance(sim, keep)) {
                make_durable(disk, block);
                continue;
            }
            if (block->durable != NULL) {
                memcpy(block->bytes, block->durable, (size_t)disk->block_bytes);
            } else {
                memset(block->bytes, 0, (size_t)disk->block_bytes);
            }
            block->written_at = block->synced_at;
            block->dirty = 0;
            vsr_sim_emit(sim, VSR_SIM_TRACE_TORN, node->index, VSR_SIM_NO_NODE,
                         b * disk->block_bytes);
        }
        inode->opens = 0;
        inode_maybe_free(disk, i);
    }
    for (uint32_t i = 1; i < disk->entries_count; ++i) {
        struct vsr_sim_entry *entry = disk->entries[i];

        if (!entry->used) {
            continue;
        }
        entry->opens = 0;
        if (entry->removed) {
            entry_free(disk, i);
        }
    }
}

/* ------------------------------------------------------------------------
 * Inspection
 * --------------------------------------------------------------------- */

static int inspect(const struct vsr_sim *sim, uint32_t node, const char *path,
                   const struct vsr_sim_disk **disk, uint32_t *entry)
{
    struct lookup found;
    int error;

    if (sim == NULL || node >= sim->nodes_count || path == NULL) {
        return -EINVAL;
    }
    *disk = &sim->nodes[node].disk;
    error = lookup(*disk, 0, path, &found);
    if (error != 0) {
        return error;
    }
    if (found.entry == VSR_SIM_NONE) {
        return -ENOENT;
    }
    *entry = found.entry;
    return 0;
}

static int inspect_file(const struct vsr_sim *sim, uint32_t node,
                        const char *path, const struct vsr_sim_disk **disk,
                        struct vsr_sim_inode **inode)
{
    uint32_t entry;
    int error = inspect(sim, node, path, disk, &entry);

    if (error != 0) {
        return error;
    }
    if ((*disk)->entries[entry]->directory) {
        return -EISDIR;
    }
    *inode = (*disk)->inodes[(*disk)->entries[entry]->inode];
    return 0;
}

int vsr_sim_file_size(const struct vsr_sim *sim, uint32_t node,
                      const char *path, uint64_t *size)
{
    const struct vsr_sim_disk *disk;
    struct vsr_sim_inode *inode;
    int error;

    if (size == NULL) {
        return -EINVAL;
    }
    error = inspect_file(sim, node, path, &disk, &inode);
    if (error == 0) {
        *size = inode->size;
    }
    return error;
}

static int inspect_read(const struct vsr_sim *sim, uint32_t node,
                        const char *path, uint64_t offset, void *bytes,
                        size_t size, size_t *read, bool durable)
{
    const struct vsr_sim_disk *disk;
    struct vsr_sim_inode *inode;
    int error;

    if (read == NULL || (bytes == NULL && size > 0)) {
        return -EINVAL;
    }
    error = inspect_file(sim, node, path, &disk, &inode);
    if (error == 0) {
        *read = size == 0
                    ? 0
                    : read_range(disk, inode, offset, bytes, size, durable);
    }
    return error;
}

int vsr_sim_file_read(const struct vsr_sim *sim, uint32_t node,
                      const char *path, uint64_t offset, void *bytes,
                      size_t size, size_t *read)
{
    return inspect_read(sim, node, path, offset, bytes, size, read, false);
}

int vsr_sim_file_read_durable(const struct vsr_sim *sim, uint32_t node,
                              const char *path, uint64_t offset, void *bytes,
                              size_t size, size_t *read)
{
    return inspect_read(sim, node, path, offset, bytes, size, read, true);
}

int vsr_sim_file_corrupt(struct vsr_sim *sim, uint32_t node, const char *path,
                         uint64_t offset, size_t size)
{
    const struct vsr_sim_disk *view;
    struct vsr_sim_disk *disk;
    struct vsr_sim_inode *inode;
    int error = inspect_file(sim, node, path, &view, &inode);
    uint64_t end;

    if (error != 0) {
        return error;
    }
    disk = &sim->nodes[node].disk;
    if (offset >= inode->size) {
        return 0;
    }
    end = inode->size - offset < size ? inode->size : offset + size;
    for (uint64_t at = offset; at < end; ++at) {
        uint64_t b = at / disk->block_bytes;
        size_t within = (size_t)(at % disk->block_bytes);
        struct vsr_sim_block *block;

        (void)block_bytes(disk, inode, b);
        block = &inode->blocks[b];
        block->bytes[within] = (unsigned char)(block->bytes[within] ^ 0xffu);
        /* A clean block's corruption is durable; a dirty one's is subject
         * to the crash model like its write. */
        if (!block->dirty) {
            if (block->durable == NULL) {
                block->durable = vsr_sim_alloc((size_t)disk->block_bytes);
            }
            block->durable[within] = block->bytes[within];
        }
    }
    return 0;
}

static uint32_t sorted_children(const struct vsr_sim_disk *disk,
                                uint32_t parent, uint32_t *children)
{
    uint32_t count = 0;

    for (uint32_t i = 1; i < disk->entries_count; ++i) {
        const struct vsr_sim_entry *entry = disk->entries[i];
        uint32_t at;

        if (!entry->used || entry->removed || entry->parent != parent) {
            continue;
        }
        at = count++;
        while (at > 0 &&
               strcmp(disk->entries[children[at - 1]]->name, entry->name) > 0) {
            children[at] = children[at - 1];
            --at;
        }
        children[at] = i;
    }
    return count;
}

int vsr_sim_dir_count(const struct vsr_sim *sim, uint32_t node,
                      const char *path, uint32_t *count)
{
    const struct vsr_sim_disk *disk;
    uint32_t entry;
    uint32_t *children;
    int error;

    if (count == NULL) {
        return -EINVAL;
    }
    error = inspect(sim, node, path, &disk, &entry);
    if (error != 0) {
        return error;
    }
    if (!disk->entries[entry]->directory) {
        return -ENOTDIR;
    }
    children = vsr_sim_alloc(sizeof(*children) * disk->entries_count);
    *count = sorted_children(disk, entry, children);
    free(children);
    return 0;
}

int vsr_sim_dir_entry(const struct vsr_sim *sim, uint32_t node,
                      const char *path, uint32_t index, char *name,
                      size_t name_size, uint64_t *size)
{
    const struct vsr_sim_disk *disk;
    uint32_t entry;
    uint32_t *children;
    uint32_t count;
    const struct vsr_sim_entry *found;
    size_t length;
    int error;

    if (name == NULL) {
        return -EINVAL;
    }
    error = inspect(sim, node, path, &disk, &entry);
    if (error != 0) {
        return error;
    }
    if (!disk->entries[entry]->directory) {
        return -ENOTDIR;
    }
    children = vsr_sim_alloc(sizeof(*children) * disk->entries_count);
    count = sorted_children(disk, entry, children);
    if (index >= count) {
        free(children);
        return -ENOENT;
    }
    found = disk->entries[children[index]];
    free(children);
    length = strlen(found->name);
    if (length + 1 > name_size) {
        return -EINVAL;
    }
    memcpy(name, found->name, length + 1);
    if (size != NULL) {
        *size = found->directory ? 0 : disk->inodes[found->inode]->size;
    }
    return 0;
}
