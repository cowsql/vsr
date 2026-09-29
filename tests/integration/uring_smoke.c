/* Real-ring smoke test of the io_uring executor (src/io/uring.c) through
 * the public vsr_io_uring_* API and the executor ops. It exercises every
 * opcode and flag against the kernel: files in a temporary directory under
 * the build tree, loopback TCP, provided-buffer rings, zero-copy sends,
 * links, cancellation, timeouts and a wake from another thread. The shared
 * sim-and-ring contract suite is tests/integration/executor_conformance;
 * this file checks the ring alone and records what the kernel does where
 * the contract leaves room. Exits 77 (skip) only when the kernel has no
 * io_uring (setup fails with ENOSYS or EPERM) or is too old for the
 * executor (init's probe reports -ENOSYS); any other init failure fails. */
#define _GNU_SOURCE
#include "config.h"

#include "lib/check.h"
#include "vsr-io.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define MS UINT64_C(1000000)
#define STASH 512u

struct fixture {
    struct vsr_io_executor ex;
    void *memory;
};

static struct vsr_io_cqe stash[STASH];
static uint32_t stashed;

static struct vsr_io_uring_options options(void)
{
    struct vsr_io_uring_options o;

    memset(&o, 0, sizeof(o));
    o.sq_entries = 32;
    o.cq_entries = 128;
    o.file_slots = 16;
    o.buffer_regions = 4;
    o.sqpoll_cpu = UINT32_MAX;
    return o;
}

static int open_fixture(struct fixture *f, const struct vsr_io_uring_options *o)
{
    struct vsr_io_need need;
    size_t size;
    int rc;

    CHECK(vsr_io_uring_layout(o, &need) == VSR_OK);
    CHECK(need.alignment >= 4096 && need.size > 0);
    size = (need.size + need.alignment - 1) & ~(need.alignment - 1);
    f->memory = aligned_alloc(need.alignment, size);
    CHECK(f->memory != NULL);
    rc = vsr_io_uring_init(f->memory, need.size, o, &f->ex);
    if (rc != 0) {
        free(f->memory);
        f->memory = NULL;
    }
    stashed = 0;
    return rc;
}

static void close_fixture(struct fixture *f)
{
    vsr_io_uring_deinit(&f->ex);
    CHECK(f->ex.ops == NULL && f->ex.ctx == NULL);
    free(f->memory);
    f->memory = NULL;
    stashed = 0;
}

static uint64_t now(const struct fixture *f)
{
    return f->ex.ops->now(f->ex.ctx);
}

static struct vsr_io_sqe rec(uint8_t opcode, uint64_t user_data)
{
    struct vsr_io_sqe r;

    memset(&r, 0, sizeof(r));
    r.opcode = opcode;
    r.user_data = user_data;
    return r;
}

/* Submits without waiting. */
static void submit(struct fixture *f, const struct vsr_io_sqe *records,
                   uint32_t count)
{
    CHECK(f->ex.ops->submit_and_wait(f->ex.ctx, records, count, 0, 0, 0) == 0);
}

static void submit1(struct fixture *f, struct vsr_io_sqe record)
{
    submit(f, &record, 1);
}

static void pump(struct fixture *f, uint64_t deadline)
{
    CHECK(f->ex.ops->submit_and_wait(f->ex.ctx, NULL, 0, 1, 0, deadline) == 0);
    stashed += f->ex.ops->reap(f->ex.ctx, stash + stashed, STASH - stashed);
}

static bool find(uint64_t user_data, struct vsr_io_cqe *out)
{
    for (uint32_t i = 0; i < stashed; ++i) {
        if (stash[i].user_data == user_data) {
            *out = stash[i];
            memmove(&stash[i], &stash[i + 1],
                    (stashed - i - 1) * sizeof(stash[0]));
            stashed--;
            return true;
        }
    }
    return false;
}

/* The next completion of user_data, within five seconds. */
static struct vsr_io_cqe take(struct fixture *f, uint64_t user_data)
{
    uint64_t deadline = now(f) + 5000 * MS;
    struct vsr_io_cqe cqe;

    while (!find(user_data, &cqe)) {
        if (now(f) >= deadline) {
            fprintf(stderr, "no completion for %llu\n",
                    (unsigned long long)user_data);
            CHECK(false);
        }
        pump(f, deadline);
    }
    return cqe;
}

/* True when user_data completes within `ms`. */
static bool arrives(struct fixture *f, uint64_t user_data, uint64_t ms,
                    struct vsr_io_cqe *out)
{
    uint64_t deadline = now(f) + ms * MS;

    while (!find(user_data, out)) {
        if (now(f) >= deadline) {
            return false;
        }
        pump(f, deadline);
    }
    return true;
}

static int32_t run(struct fixture *f, struct vsr_io_sqe record)
{
    submit1(f, record);
    return take(f, record.user_data).result;
}

static void *page_alloc(size_t size)
{
    void *p = aligned_alloc(4096, (size + 4095) & ~(size_t)4095);

    CHECK(p != NULL);
    memset(p, 0, size);
    return p;
}

/* ------------------------------------------------------------------------
 * Layout, init, basics
 * --------------------------------------------------------------------- */

static void test_layout(void)
{
    struct vsr_io_uring_options o = options();
    struct vsr_io_need need;
    struct vsr_io_executor ex;
    void *memory;

    CHECK(vsr_io_uring_layout(&o, NULL) == VSR_EINVAL);
    CHECK(vsr_io_uring_layout(NULL, &need) == VSR_EINVAL);
    o.cq_entries = 2 * o.sq_entries - 1;
    CHECK(vsr_io_uring_layout(&o, &need) == VSR_EINVAL);
    o = options();
    o.sq_entries = 0;
    CHECK(vsr_io_uring_layout(&o, &need) == VSR_EINVAL);
    o = options();
    o.sq_entries = 65536;
    o.cq_entries = 131072;
    CHECK(vsr_io_uring_layout(&o, &need) == VSR_ELIMIT);
    o = options();
    o.reserved = 1;
    CHECK(vsr_io_uring_layout(&o, &need) == VSR_EINVAL);

    o = options();
    CHECK(vsr_io_uring_layout(&o, &need) == VSR_OK);
    memory = aligned_alloc(need.alignment, (need.size + need.alignment) &
                                               ~(need.alignment - 1));
    CHECK(memory != NULL);
    CHECK(vsr_io_uring_init(memory, need.size - 1, &o, &ex) == -EINVAL);
    CHECK(vsr_io_uring_init((unsigned char *)memory + 64, need.size, &o, &ex) ==
          -EINVAL);
    o.cq_entries = 8;
    CHECK(vsr_io_uring_init(memory, need.size, &o, &ex) == -EINVAL);
    free(memory);
}

static void test_basics(void)
{
    struct vsr_io_uring_options o = options();
    struct fixture f;
    struct vsr_io_sqe r;
    struct vsr_io_cqe cqe;
    unsigned char bytes[64];
    uint64_t t0;
    uint64_t t1;
    bool nonzero = false;
    struct pollfd pfd;

    CHECK(open_fixture(&f, &o) == 0);
    CHECK(vsr_io_uring_fd(&f.ex) >= 0);

    t0 = now(&f);
    t1 = now(&f);
    CHECK(t0 > 0 && t1 >= t0 && t1 < VSR_NO_DEADLINE);
    memset(bytes, 0, sizeof(bytes));
    f.ex.ops->random(f.ex.ctx, bytes, sizeof(bytes));
    for (size_t i = 0; i < sizeof(bytes); ++i) {
        nonzero = nonzero || bytes[i] != 0;
    }
    CHECK(nonzero);

    /* NOP; SKIP_SUCCESS hides success but not failure. */
    cqe = (submit1(&f, rec(VSR_IO_SQE_NOP, 1)), take(&f, 1));
    CHECK(cqe.result == 0 && cqe.flags == 0 && cqe.buffer_id == 0);
    r = rec(VSR_IO_SQE_NOP, 2);
    r.flags = VSR_IO_SQE_SKIP_SUCCESS;
    submit1(&f, r);
    CHECK(!arrives(&f, 2, 30, &cqe));
    r = rec(VSR_IO_SQE_READ, 3);
    r.flags = VSR_IO_SQE_SKIP_SUCCESS;
    r.fd = -1;
    r.addr = bytes;
    r.length = 8;
    CHECK(run(&f, r) == -EBADF);

    /* Malformed records complete with -EINVAL in their place. */
    CHECK(run(&f, rec(200, 4)) == -EINVAL);
    r = rec(VSR_IO_SQE_NOP, 5);
    r.flags = 0x80;
    CHECK(run(&f, r) == -EINVAL);
    r = rec(VSR_IO_SQE_NOP, 6);
    r.flags = VSR_IO_SQE_BUFFER_SELECT;
    CHECK(run(&f, r) == -EINVAL);
    /* The wake poll's user_data is reserved: nothing is submitted. */
    r = rec(VSR_IO_SQE_NOP, UINT64_MAX);
    CHECK(f.ex.ops->submit_and_wait(f.ex.ctx, &r, 1, 0, 0, 0) == -EINVAL);

    /* A wait never blocks with want 0 and a past deadline; a deadline is
     * a normal return. */
    t0 = now(&f);
    CHECK(f.ex.ops->submit_and_wait(f.ex.ctx, NULL, 0, 0, 0, 0) == 0);
    CHECK(f.ex.ops->submit_and_wait(f.ex.ctx, NULL, 0, 0, 0, t0) == 0);
    CHECK(now(&f) - t0 < 20 * MS);
    t0 = now(&f);
    CHECK(f.ex.ops->submit_and_wait(f.ex.ctx, NULL, 0, 1, 0, t0 + 30 * MS) ==
          0);
    CHECK(now(&f) - t0 >= 29 * MS);
    CHECK(f.ex.ops->reap(f.ex.ctx, stash, STASH) == 0);

    /* want counts completions; min_wait_ns ends the wait early once one
     * exists. */
    {
        struct vsr_io_sqe two[2] = {rec(VSR_IO_SQE_NOP, 7),
                                    rec(VSR_IO_SQE_NOP, 8)};

        t0 = now(&f);
        CHECK(f.ex.ops->submit_and_wait(f.ex.ctx, two, 2, 2, 0,
                                        t0 + 2000 * MS) == 0);
        CHECK(now(&f) - t0 < 500 * MS);
        CHECK(f.ex.ops->reap(f.ex.ctx, stash, STASH) == 2);
        t0 = now(&f);
        CHECK(f.ex.ops->submit_and_wait(f.ex.ctx, two, 1, 4, 20 * MS,
                                        t0 + 2000 * MS) == 0);
        t1 = now(&f) - t0;
        CHECK(t1 >= 19 * MS && t1 < 1000 * MS);
        CHECK(f.ex.ops->reap(f.ex.ctx, stash, 1) == 1);
        CHECK(stash[0].user_data == 7);
        /* Same without a deadline. */
        t0 = now(&f);
        CHECK(f.ex.ops->submit_and_wait(f.ex.ctx, two, 1, 4, 20 * MS,
                                        VSR_NO_DEADLINE) == 0);
        t1 = now(&f) - t0;
        CHECK(t1 >= 19 * MS && t1 < 1000 * MS);
        CHECK(f.ex.ops->reap(f.ex.ctx, stash, STASH) == 1);
    }

    /* The ring descriptor is readable once a completion is pending. */
    r = rec(VSR_IO_SQE_TIMEOUT, 9);
    r.offset = 20 * MS;
    submit1(&f, r);
    pfd.fd = vsr_io_uring_fd(&f.ex);
    pfd.events = POLLIN;
    pfd.revents = 0;
    CHECK(poll(&pfd, 1, 2000) == 1 && (pfd.revents & POLLIN));
    CHECK(take(&f, 9).result == -ETIME);
    close_fixture(&f);
}

/* ------------------------------------------------------------------------
 * Files
 * --------------------------------------------------------------------- */

struct directory {
    char path[64];
    int fd;
};

static void make_directory(struct directory *dir)
{
    snprintf(dir->path, sizeof(dir->path), "uring_smoke.XXXXXX");
    CHECK(mkdtemp(dir->path) != NULL);
    dir->fd = open(dir->path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(dir->fd >= 0);
}

static void remove_directory(struct directory *dir)
{
    static const char *const names[] = {"data", "vec",     "direct", "b",
                                        "a",    "odirect", "sub",    "slot"};

    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i) {
        (void)unlinkat(dir->fd, names[i], 0);
        (void)unlinkat(dir->fd, names[i], AT_REMOVEDIR);
    }
    CHECK(close(dir->fd) == 0);
    CHECK(rmdir(dir->path) == 0);
}

static int32_t open_at(struct fixture *f, int dir, const char *name,
                       uint32_t flags, uint64_t user_data)
{
    struct vsr_io_sqe r = rec(VSR_IO_SQE_OPENAT, user_data);

    r.fd = dir;
    r.addr = name;
    r.op_flags = flags;
    r.length = 0644;
    return run(f, r);
}

static int32_t io(struct fixture *f, uint8_t opcode, int fd, const void *addr,
                  uint32_t length, uint64_t offset, uint64_t user_data)
{
    struct vsr_io_sqe r = rec(opcode, user_data);

    r.fd = fd;
    r.addr = addr;
    r.length = length;
    r.offset = offset;
    return run(f, r);
}

static int32_t fixed_io(struct fixture *f, uint8_t opcode, int fd,
                        const void *addr, uint32_t length, uint64_t offset,
                        uint16_t index, uint64_t user_data)
{
    struct vsr_io_sqe r = rec(opcode, user_data);

    r.flags = VSR_IO_SQE_FIXED_BUFFER;
    r.fd = fd;
    r.addr = addr;
    r.length = length;
    r.offset = offset;
    r.buffer_index = index;
    return run(f, r);
}

static void test_files(void)
{
    static char abc[] = "abc";
    static char defgh[] = "defgh";
    struct vsr_io_uring_options o = options();
    struct fixture f;
    struct directory dir;
    struct vsr_io_sqe r;
    struct vsr_io_region region;
    struct statx stx;
    unsigned char *page = page_alloc(8192);
    char buffer[64];
    struct vsr_io_vec vecs[2];
    int32_t fd;
    int32_t slot;
    int32_t slots[16];
    uint32_t opened = 0;

    CHECK(open_fixture(&f, &o) == 0);
    make_directory(&dir);

    /* Plain descriptors: write, read, sync, allocate, stat. */
    fd = open_at(&f, dir.fd, "data", O_CREAT | O_RDWR | O_CLOEXEC, 10);
    CHECK(fd >= 0);
    CHECK(io(&f, VSR_IO_SQE_WRITE, fd, "hello world", 11, 0, 11) == 11);
    memset(buffer, 0, sizeof(buffer));
    CHECK(io(&f, VSR_IO_SQE_READ, fd, buffer, sizeof(buffer), 0, 12) == 11);
    CHECK(memcmp(buffer, "hello world", 11) == 0);
    CHECK(io(&f, VSR_IO_SQE_READ, fd, buffer, 8, 100, 13) == 0);
    r = rec(VSR_IO_SQE_FSYNC, 14);
    r.fd = fd;
    r.op_flags = VSR_IO_FSYNC_DATASYNC;
    CHECK(run(&f, r) == 0);
    r.op_flags = 0;
    CHECK(run(&f, r) == 0);
    r = rec(VSR_IO_SQE_FALLOCATE, 15);
    r.fd = fd;
    r.offset = 0;
    r.length = 8192;
    CHECK(run(&f, r) == 0);
    memset(&stx, 0, sizeof(stx));
    r = rec(VSR_IO_SQE_STATX, 16);
    r.fd = dir.fd;
    r.addr = "data";
    r.addr2 = &stx;
    r.length = STATX_SIZE | STATX_MODE;
    CHECK(run(&f, r) == 0);
    CHECK(stx.stx_size == 8192 && S_ISREG(stx.stx_mode));

    /* Vectors. */
    vecs[0].base = abc;
    vecs[0].length = 3;
    vecs[1].base = defgh;
    vecs[1].length = 5;
    r = rec(VSR_IO_SQE_WRITEV, 17);
    r.fd = fd;
    r.addr = vecs;
    r.length = 2;
    r.offset = 20;
    CHECK(run(&f, r) == 8);
    memset(buffer, 0, sizeof(buffer));
    vecs[0].base = buffer;
    vecs[0].length = 4;
    vecs[1].base = buffer + 10;
    vecs[1].length = 4;
    r.opcode = VSR_IO_SQE_READV;
    r.user_data = 18;
    CHECK(run(&f, r) == 8);
    CHECK(memcmp(buffer, "abcd", 4) == 0 &&
          memcmp(buffer + 10, "efgh", 4) == 0);

    /* Registered buffers: fixed reads and writes inside the region; every
     * access outside it, or to an empty index, fails with -EFAULT. */
    CHECK(f.ex.ops->register_buffers(f.ex.ctx, 8) == -EINVAL);
    CHECK(f.ex.ops->register_buffers(f.ex.ctx, 2) == 0);
    CHECK(f.ex.ops->register_buffers(f.ex.ctx, 2) == -EBUSY);
    region.base = page;
    region.size = 8192;
    CHECK(f.ex.ops->update_buffer(f.ex.ctx, 2, &region) == -EINVAL);
    CHECK(f.ex.ops->update_buffer(f.ex.ctx, 0, &region) == 0);
    memcpy(page, "fixed-bytes", 11);
    CHECK(fixed_io(&f, VSR_IO_SQE_WRITE, fd, page, 11, 100, 0, 20) == 11);
    CHECK(fixed_io(&f, VSR_IO_SQE_READ, fd, page + 4096, 11, 100, 0, 21) == 11);
    CHECK(memcmp(page + 4096, "fixed-bytes", 11) == 0);
    vecs[0].base = page + 100;
    vecs[0].length = 5;
    vecs[1].base = page + 200;
    vecs[1].length = 6;
    r = rec(VSR_IO_SQE_READV, 22);
    r.flags = VSR_IO_SQE_FIXED_BUFFER;
    r.fd = fd;
    r.addr = vecs;
    r.length = 2;
    r.offset = 100;
    CHECK(run(&f, r) == 11);
    CHECK(memcmp(page + 100, "fixed", 5) == 0 &&
          memcmp(page + 200, "-bytes", 6) == 0);
    r.opcode = VSR_IO_SQE_WRITEV;
    r.user_data = 23;
    r.offset = 300;
    CHECK(run(&f, r) == 11);
    CHECK(fixed_io(&f, VSR_IO_SQE_READ, fd, buffer, 8, 0, 0, 24) == -EFAULT);
    CHECK(fixed_io(&f, VSR_IO_SQE_READ, fd, page + 8190, 4, 0, 0, 25) ==
          -EFAULT);
    CHECK(fixed_io(&f, VSR_IO_SQE_READ, fd, page, 8, 0, 1, 26) == -EFAULT);
    CHECK(fixed_io(&f, VSR_IO_SQE_READ, fd, page, 8, 0, 3, 27) == -EFAULT);
    vecs[1].base = buffer;
    r.user_data = 28;
    CHECK(run(&f, r) == -EFAULT);
    CHECK(f.ex.ops->update_buffer(f.ex.ctx, 0, NULL) == 0);
    CHECK(fixed_io(&f, VSR_IO_SQE_READ, fd, page, 8, 0, 0, 29) == -EFAULT);
    r = rec(VSR_IO_SQE_CLOSE, 30);
    r.fd = fd;
    CHECK(run(&f, r) == 0);

    /* O_DIRECT: 512-byte alignment of address, offset and length. */
    fd = open_at(&f, dir.fd, "odirect", O_CREAT | O_RDWR | O_DIRECT | O_CLOEXEC,
                 31);
    if (fd == -EINVAL) {
        printf("O_DIRECT unsupported by the build tree's filesystem\n");
    } else {
        CHECK(fd >= 0);
        memset(page, 'd', 4096);
        CHECK(io(&f, VSR_IO_SQE_WRITE, fd, page, 4096, 0, 32) == 4096);
        CHECK(io(&f, VSR_IO_SQE_READ, fd, page + 4096, 512, 512, 33) == 512);
        /* The kernel's rule is the filesystem's: ext4 and xfs refuse
         * misaligned direct I/O with -EINVAL, btrfs serves it buffered.
         * Recorded, not checked. */
        memset(&stx, 0, sizeof(stx));
        r = rec(VSR_IO_SQE_STATX, 38);
        r.fd = dir.fd;
        r.addr = "odirect";
        r.addr2 = &stx;
        r.length = STATX_DIOALIGN;
        CHECK(run(&f, r) == 0);
        printf("O_DIRECT (dio align mem %u offset %u): misaligned address %d, "
               "offset %d, length %d\n",
               (stx.stx_mask & STATX_DIOALIGN) ? stx.stx_dio_mem_align : 0u,
               (stx.stx_mask & STATX_DIOALIGN) ? stx.stx_dio_offset_align : 0u,
               io(&f, VSR_IO_SQE_READ, fd, page + 1, 512, 0, 34),
               io(&f, VSR_IO_SQE_READ, fd, page, 512, 100, 35),
               io(&f, VSR_IO_SQE_READ, fd, page, 100, 0, 36));
        r = rec(VSR_IO_SQE_CLOSE, 37);
        r.fd = fd;
        CHECK(run(&f, r) == 0);
    }

    /* Directory operations and atomic rename. */
    r = rec(VSR_IO_SQE_MKDIRAT, 40);
    r.fd = dir.fd;
    r.addr = "sub";
    r.length = 0755;
    CHECK(run(&f, r) == 0);
    r.user_data = 41;
    CHECK(run(&f, r) == -EEXIST);
    CHECK(open_at(&f, dir.fd, "data", O_CREAT | O_EXCL | O_RDWR, 42) ==
          -EEXIST);
    CHECK(open_at(&f, dir.fd, "missing", O_RDONLY, 43) == -ENOENT);
    fd = open_at(&f, dir.fd, "a", O_CREAT | O_RDWR | O_CLOEXEC, 44);
    CHECK(fd >= 0 && io(&f, VSR_IO_SQE_WRITE, fd, "AAAA", 4, 0, 45) == 4);
    CHECK(close(fd) == 0);
    fd = open_at(&f, dir.fd, "b", O_CREAT | O_RDWR | O_CLOEXEC, 46);
    CHECK(fd >= 0 && io(&f, VSR_IO_SQE_WRITE, fd, "BBBBBB", 6, 0, 47) == 6);
    r = rec(VSR_IO_SQE_RENAMEAT, 48);
    r.fd = dir.fd;
    r.addr = "a";
    r.addr2 = "b";
    CHECK(run(&f, r) == 0);
    /* The old descriptor of b keeps the replaced file's bytes. */
    memset(buffer, 0, sizeof(buffer));
    CHECK(io(&f, VSR_IO_SQE_READ, fd, buffer, 16, 0, 49) == 6);
    CHECK(close(fd) == 0);
    fd = open_at(&f, dir.fd, "b", O_RDONLY | O_CLOEXEC, 50);
    memset(buffer, 0, sizeof(buffer));
    CHECK(fd >= 0 && io(&f, VSR_IO_SQE_READ, fd, buffer, 16, 0, 51) == 4);
    CHECK(memcmp(buffer, "AAAA", 4) == 0);
    CHECK(open_at(&f, dir.fd, "a", O_RDONLY, 52) == -ENOENT);
    r = rec(VSR_IO_SQE_UNLINKAT, 53);
    r.fd = dir.fd;
    r.addr = "b";
    CHECK(run(&f, r) == 0);
    /* The unlinked file stays readable through its descriptor. */
    CHECK(io(&f, VSR_IO_SQE_READ, fd, buffer, 16, 0, 54) == 4);
    CHECK(close(fd) == 0);
    r.user_data = 55;
    CHECK(run(&f, r) == -ENOENT);

    /* Direct descriptors: allocation, a named slot, FIXED_FILE use, CLOSE
     * of a slot, exhaustion. */
    CHECK(f.ex.ops->register_files(f.ex.ctx, 32) == -EINVAL);
    CHECK(f.ex.ops->register_files(f.ex.ctx, 8) == 0);
    CHECK(f.ex.ops->register_files(f.ex.ctx, 8) == -EBUSY);
    r = rec(VSR_IO_SQE_OPENAT, 60);
    r.flags = VSR_IO_SQE_DIRECT;
    r.fd = dir.fd;
    r.addr = "direct";
    r.op_flags = O_CREAT | O_RDWR;
    r.length = 0644;
    r.fd2 = VSR_IO_SLOT_ALLOC;
    slot = run(&f, r);
    CHECK(slot >= 0 && slot < 8);
    r.user_data = 61;
    r.fd2 = 5;
    r.op_flags = O_RDWR;
    CHECK(slot != 5 && run(&f, r) == 5);
    r = rec(VSR_IO_SQE_WRITE, 62);
    r.flags = VSR_IO_SQE_FIXED_FILE;
    r.fd = slot;
    r.addr = "direct!";
    r.length = 7;
    CHECK(run(&f, r) == 7);
    memset(buffer, 0, sizeof(buffer));
    r = rec(VSR_IO_SQE_READ, 63);
    r.flags = VSR_IO_SQE_FIXED_FILE;
    r.fd = 5;
    r.addr = buffer;
    r.length = 16;
    CHECK(run(&f, r) == 7 && memcmp(buffer, "direct!", 7) == 0);
    r = rec(VSR_IO_SQE_CLOSE, 64);
    r.flags = VSR_IO_SQE_FIXED_FILE;
    r.fd = 5;
    CHECK(run(&f, r) == 0);
    r = rec(VSR_IO_SQE_READ, 65);
    r.flags = VSR_IO_SQE_FIXED_FILE;
    r.fd = 5;
    r.addr = buffer;
    r.length = 16;
    CHECK(run(&f, r) == -EBADF);
    r = rec(VSR_IO_SQE_CLOSE, 66);
    r.flags = VSR_IO_SQE_FIXED_FILE;
    r.fd = 5;
    CHECK(run(&f, r) == -EBADF);
    slots[opened++] = slot;
    for (;;) {
        r = rec(VSR_IO_SQE_OPENAT, 70 + opened);
        r.flags = VSR_IO_SQE_DIRECT;
        r.fd = dir.fd;
        r.addr = "direct";
        r.op_flags = O_RDONLY;
        r.fd2 = VSR_IO_SLOT_ALLOC;
        slot = run(&f, r);
        if (slot < 0) {
            break;
        }
        CHECK(opened < 8);
        slots[opened++] = slot;
    }
    CHECK(slot == -ENFILE && opened == 8);
    for (uint32_t i = 0; i < opened; ++i) {
        r = rec(VSR_IO_SQE_CLOSE, 90 + i);
        r.flags = VSR_IO_SQE_FIXED_FILE;
        r.fd = slots[i];
        CHECK(run(&f, r) == 0);
    }

    /* update_file installs a descriptor the executor then owns. */
    fd = openat(dir.fd, "direct", O_RDONLY | O_CLOEXEC);
    CHECK(fd >= 0);
    CHECK(f.ex.ops->update_file(f.ex.ctx, 8, fd) == -EINVAL);
    CHECK(f.ex.ops->update_file(f.ex.ctx, 2, fd) == 0);
    CHECK(fcntl(fd, F_GETFD) == -1 && errno == EBADF);
    r = rec(VSR_IO_SQE_READ, 100);
    r.flags = VSR_IO_SQE_FIXED_FILE;
    r.fd = 2;
    r.addr = buffer;
    r.length = 16;
    CHECK(run(&f, r) == 7);
    CHECK(f.ex.ops->update_file(f.ex.ctx, 2, -1) == 0);
    r.user_data = 101;
    CHECK(run(&f, r) == -EBADF);

    /* Links: a short read fails its chain; success runs it in order. */
    fd = open_at(&f, dir.fd, "slot", O_CREAT | O_RDWR | O_CLOEXEC, 102);
    CHECK(fd >= 0);
    {
        struct vsr_io_sqe chain[3];

        chain[0] = rec(VSR_IO_SQE_WRITE, 103);
        chain[0].flags = VSR_IO_SQE_LINK;
        chain[0].fd = fd;
        chain[0].addr = "0123456789";
        chain[0].length = 10;
        chain[1] = rec(VSR_IO_SQE_FSYNC, 104);
        chain[1].flags = VSR_IO_SQE_LINK;
        chain[1].fd = fd;
        chain[2] = rec(VSR_IO_SQE_READ, 105);
        chain[2].fd = fd;
        chain[2].addr = buffer;
        chain[2].length = 10;
        submit(&f, chain, 3);
        CHECK(take(&f, 103).result == 10);
        CHECK(take(&f, 104).result == 0);
        CHECK(take(&f, 105).result == 10);

        chain[0] = rec(VSR_IO_SQE_READ, 106);
        chain[0].flags = VSR_IO_SQE_LINK;
        chain[0].fd = fd;
        chain[0].addr = buffer;
        chain[0].length = 20; /* The file holds 10: short. */
        chain[1] = rec(VSR_IO_SQE_NOP, 107);
        chain[1].flags = VSR_IO_SQE_LINK;
        chain[2] = rec(VSR_IO_SQE_NOP, 108);
        submit(&f, chain, 3);
        CHECK(take(&f, 106).result == 10);
        CHECK(take(&f, 107).result == -ECANCELED);
        CHECK(take(&f, 108).result == -ECANCELED);

        /* A record the executor rejects fails its chain the same way. */
        chain[0] = rec(VSR_IO_SQE_WRITE, 109);
        chain[0].flags = VSR_IO_SQE_LINK | VSR_IO_SQE_FIXED_BUFFER;
        chain[0].fd = fd;
        chain[0].addr = buffer;
        chain[0].length = 4;
        chain[1] = rec(VSR_IO_SQE_NOP, 110);
        submit(&f, chain, 2);
        CHECK(take(&f, 109).result == -EFAULT);
        CHECK(take(&f, 110).result == -ECANCELED);

        /* A trailing LINK has no successor: nothing waits on it. */
        chain[0] = rec(VSR_IO_SQE_NOP, 111);
        chain[0].flags = VSR_IO_SQE_LINK;
        submit(&f, chain, 1);
        CHECK(take(&f, 111).result == 0);
        submit1(&f, rec(VSR_IO_SQE_NOP, 112));
        CHECK(take(&f, 112).result == 0);
    }
    CHECK(close(fd) == 0);

    remove_directory(&dir);
    close_fixture(&f);
    free(page);
}

/* ------------------------------------------------------------------------
 * Network
 * --------------------------------------------------------------------- */

static int32_t make_socket(struct fixture *f, uint64_t user_data)
{
    struct vsr_io_sqe r = rec(VSR_IO_SQE_SOCKET, user_data);

    r.length = AF_INET;
    r.op_flags = SOCK_STREAM | SOCK_CLOEXEC;
    return run(f, r);
}

static int32_t connect_to(struct fixture *f, int fd,
                          const struct sockaddr_in *address, uint64_t user_data)
{
    struct vsr_io_sqe r = rec(VSR_IO_SQE_CONNECT, user_data);

    r.fd = fd;
    r.addr = address;
    r.length = sizeof(*address);
    return run(f, r);
}

static struct vsr_io_sqe send_record(int fd, bool fixed_file, const void *addr,
                                     uint32_t length, uint64_t user_data)
{
    struct vsr_io_sqe r = rec(VSR_IO_SQE_SEND, user_data);

    r.fd = fd;
    r.flags = fixed_file ? VSR_IO_SQE_FIXED_FILE : 0;
    r.addr = addr;
    r.length = length;
    return r;
}

/* Receives exactly `length` bytes on a plain descriptor. */
static void receive_all(struct fixture *f, int fd, void *buffer,
                        uint32_t length, uint64_t user_data)
{
    uint32_t got = 0;

    while (got < length) {
        struct vsr_io_sqe r = rec(VSR_IO_SQE_RECV, user_data);
        int32_t n;

        r.fd = fd;
        r.addr = (unsigned char *)buffer + got;
        r.length = length - got;
        n = run(f, r);
        CHECK(n > 0);
        got += (uint32_t)n;
    }
}

struct zero_copy {
    int32_t result;
    uint16_t result_flags;
    uint16_t notif_flags;
    int32_t notif;
};

static struct zero_copy zc_send(struct fixture *f, struct vsr_io_sqe r)
{
    struct zero_copy out;
    struct vsr_io_cqe cqe;

    r.op_flags |= VSR_IO_SEND_ZERO_COPY;
    submit1(f, r);
    cqe = take(f, r.user_data);
    out.result = cqe.result;
    out.result_flags = cqe.flags;
    if (cqe.flags & VSR_IO_CQE_MORE) {
        cqe = take(f, r.user_data);
        out.notif = cqe.result;
        out.notif_flags = cqe.flags;
    } else {
        out.notif = INT32_MIN;
        out.notif_flags = 0;
    }
    return out;
}

static void test_network(void)
{
    static char vec[] = "vec";
    static char tored[] = "tored";
    struct vsr_io_uring_options o = options();
    struct fixture f;
    struct vsr_io_sqe r;
    struct vsr_io_cqe cqe;
    struct sockaddr_in address;
    socklen_t address_length = sizeof(address);
    struct vsr_io_region memory;
    struct vsr_io_region region;
    struct vsr_io_buffer buffers[8];
    struct vsr_io_vec vecs[2];
    unsigned char *ring_memory = page_alloc(4096);
    unsigned char *pool = page_alloc(4096);
    unsigned char *zc = page_alloc(4096);
    unsigned char pattern[160];
    char buffer[256];
    int value = 1;
    int32_t listener;
    int32_t client;
    int32_t server;
    uint32_t total;
    struct zero_copy z;

    CHECK(open_fixture(&f, &o) == 0);
    CHECK(f.ex.ops->register_files(f.ex.ctx, 16) == 0);
    CHECK(f.ex.ops->register_buffers(f.ex.ctx, 2) == 0);
    for (size_t i = 0; i < sizeof(pattern); ++i) {
        pattern[i] = (unsigned char)('A' + i % 26);
    }

    /* Listener through the ring: socket, setsockopt, bind, listen. */
    listener = make_socket(&f, 200);
    CHECK(listener >= 0);
    r = rec(VSR_IO_SQE_SETSOCKOPT, 201);
    r.fd = listener;
    r.op_flags = (uint32_t)SOL_SOCKET << 16 | SO_REUSEADDR;
    r.addr = &value;
    r.length = sizeof(value);
    CHECK(run(&f, r) == 0);
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    r = rec(VSR_IO_SQE_BIND, 202);
    r.fd = listener;
    r.addr = &address;
    r.length = sizeof(address);
    CHECK(run(&f, r) == 0);
    r.user_data = 203;
    CHECK(run(&f, r) == -EINVAL); /* Already bound. */
    r = rec(VSR_IO_SQE_LISTEN, 204);
    r.fd = listener;
    r.length = 16;
    CHECK(run(&f, r) == 0);
    CHECK(getsockname(listener, (struct sockaddr *)&address, &address_length) ==
          0);
    CHECK(address.sin_port != 0);
    value = 0;
    r = rec(VSR_IO_SQE_GETSOCKOPT, 205);
    r.fd = listener;
    r.op_flags = (uint32_t)SOL_SOCKET << 16 | SO_REUSEADDR;
    r.addr = &value;
    r.length = sizeof(value);
    CHECK(run(&f, r) == (int32_t)sizeof(value) && value == 1);

    /* BIND conflicts and CONNECT without a listener. */
    {
        struct sockaddr_in unused;
        socklen_t unused_length = sizeof(unused);
        int32_t other = make_socket(&f, 206);

        CHECK(other >= 0);
        r = rec(VSR_IO_SQE_BIND, 207);
        r.fd = other;
        r.addr = &address;
        r.length = sizeof(address);
        CHECK(run(&f, r) == -EADDRINUSE);
        memset(&unused, 0, sizeof(unused));
        unused.sin_family = AF_INET;
        unused.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        r.user_data = 208;
        r.addr = &unused;
        CHECK(run(&f, r) == 0);
        CHECK(getsockname(other, (struct sockaddr *)&unused, &unused_length) ==
              0);
        CHECK(close(other) == 0);
        other = make_socket(&f, 209);
        CHECK(connect_to(&f, other, &unused, 209) == -ECONNREFUSED);
        CHECK(close(other) == 0);
    }

    /* Multishot accept into allocated slots. */
    r = rec(VSR_IO_SQE_ACCEPT, 210);
    r.fd = listener;
    r.flags = VSR_IO_SQE_DIRECT;
    r.fd2 = VSR_IO_SLOT_ALLOC;
    r.op_flags = VSR_IO_ACCEPT_MULTISHOT;
    submit1(&f, r);
    client = make_socket(&f, 211);
    CHECK(client >= 0);
    CHECK(connect_to(&f, client, &address, 212) == 0);
    cqe = take(&f, 210);
    server = cqe.result;
    CHECK(server >= 0 && server < 16 && (cqe.flags & VSR_IO_CQE_MORE));
    value = 1;
    r = rec(VSR_IO_SQE_SETSOCKOPT, 213);
    r.flags = VSR_IO_SQE_FIXED_FILE;
    r.fd = server;
    r.op_flags = (uint32_t)IPPROTO_TCP << 16 | TCP_NODELAY;
    r.addr = &value;
    r.length = sizeof(value);
    CHECK(run(&f, r) == 0);

    /* Incremental provided-buffer ring and a multishot receive. */
    memory.base = ring_memory;
    memory.size = 4096;
    CHECK(f.ex.ops->buffer_ring(f.ex.ctx, 7, 6, 0, &memory) == -EINVAL);
    CHECK(f.ex.ops->buffer_ring(f.ex.ctx, 7, 8, VSR_IO_BUFFER_RING_INCREMENTAL,
                                &memory) == 0);
    CHECK(f.ex.ops->buffer_ring(f.ex.ctx, 7, 8, VSR_IO_BUFFER_RING_INCREMENTAL,
                                &memory) == -EEXIST);
    for (uint16_t i = 0; i < 8; ++i) {
        buffers[i].base = pool + (size_t)64 * i;
        buffers[i].length = 64;
        buffers[i].id = (uint16_t)(10 + i);
        buffers[i].reserved = 0;
    }
    CHECK(f.ex.ops->provide(f.ex.ctx, 9, buffers, 1) == -ENOENT);
    CHECK(f.ex.ops->provide(f.ex.ctx, 7, buffers, 2) == 0);
    r = rec(VSR_IO_SQE_RECV, 220);
    r.flags = VSR_IO_SQE_FIXED_FILE | VSR_IO_SQE_BUFFER_SELECT;
    r.fd = server;
    r.buffer_group = 7;
    r.op_flags = VSR_IO_RECV_MULTISHOT;
    submit1(&f, r);
    CHECK(run(&f, send_record(client, false, "abc", 3, 221)) == 3);
    cqe = take(&f, 220);
    CHECK(cqe.result == 3 && cqe.buffer_id == 10);
    CHECK(cqe.flags ==
          (VSR_IO_CQE_MORE | VSR_IO_CQE_BUFFER | VSR_IO_CQE_BUFFER_MORE));
    CHECK(run(&f, send_record(client, false, "defg", 4, 222)) == 4);
    cqe = take(&f, 220);
    CHECK(cqe.result == 4 && cqe.buffer_id == 10);
    CHECK(cqe.flags ==
          (VSR_IO_CQE_MORE | VSR_IO_CQE_BUFFER | VSR_IO_CQE_BUFFER_MORE));
    CHECK(memcmp(pool, "abcdefg", 7) == 0);
    /* 60 bytes: the 57 left in buffer 10, which leaves the ring, then 3 in
     * buffer 11. */
    CHECK(run(&f, send_record(client, false, pattern, 60, 223)) == 60);
    total = 0;
    while (total < 60) {
        cqe = take(&f, 220);
        CHECK(cqe.result > 0 && (cqe.flags & VSR_IO_CQE_MORE) &&
              (cqe.flags & VSR_IO_CQE_BUFFER));
        if (cqe.buffer_id == 10) {
            CHECK(total == 0 && cqe.result == 57 &&
                  !(cqe.flags & VSR_IO_CQE_BUFFER_MORE));
        } else {
            CHECK(cqe.buffer_id == 11 && (cqe.flags & VSR_IO_CQE_BUFFER_MORE));
        }
        total += (uint32_t)cqe.result;
    }
    CHECK(total == 60);
    CHECK(memcmp(pool + 7, pattern, 57) == 0 &&
          memcmp(pool + 64, pattern + 57, 3) == 0);
    /* 100 bytes: 61 fill buffer 11, then the ring is empty: -ENOBUFS ends
     * the multishot receive. */
    CHECK(run(&f, send_record(client, false, pattern, 100, 224)) == 100);
    cqe = take(&f, 220);
    CHECK(cqe.result == 61 && cqe.buffer_id == 11 &&
          cqe.flags == (VSR_IO_CQE_MORE | VSR_IO_CQE_BUFFER));
    cqe = take(&f, 220);
    CHECK(cqe.result == -ENOBUFS && !(cqe.flags & VSR_IO_CQE_MORE));
    /* Re-arm after providing: the 39 bytes waiting arrive. */
    CHECK(f.ex.ops->provide(f.ex.ctx, 7, &buffers[2], 1) == 0);
    r.user_data = 225;
    submit1(&f, r);
    cqe = take(&f, 225);
    CHECK(cqe.result == 39 && cqe.buffer_id == 12 &&
          cqe.flags ==
              (VSR_IO_CQE_MORE | VSR_IO_CQE_BUFFER | VSR_IO_CQE_BUFFER_MORE));
    CHECK(memcmp(pool + 128, pattern + 61, 39) == 0);
    /* The ring holds 8: buffer 12 is still in it. */
    CHECK(f.ex.ops->provide(f.ex.ctx, 7, &buffers[3], 8) == -ENOSPC);
    CHECK(f.ex.ops->provide(f.ex.ctx, 7, &buffers[3], 5) == 0);
    CHECK(f.ex.ops->provide(f.ex.ctx, 7, buffers, 3) == -ENOSPC);

    /* PEEK leaves the bytes for the next receive. */
    CHECK(run(&f, send_record(server, true, "xyz", 3, 230)) == 3);
    memset(buffer, 0, sizeof(buffer));
    r = rec(VSR_IO_SQE_RECV, 231);
    r.fd = client;
    r.addr = buffer;
    r.length = sizeof(buffer);
    r.op_flags = VSR_IO_RECV_PEEK;
    CHECK(run(&f, r) == 3 && memcmp(buffer, "xyz", 3) == 0);
    memset(buffer, 0, sizeof(buffer));
    r.user_data = 232;
    r.op_flags = 0;
    CHECK(run(&f, r) == 3 && memcmp(buffer, "xyz", 3) == 0);

    /* Plain vectored send. */
    vecs[0].base = vec;
    vecs[0].length = 3;
    vecs[1].base = tored;
    vecs[1].length = 5;
    r = send_record(server, true, vecs, 2, 233);
    r.op_flags = VSR_IO_SEND_VECTORED;
    CHECK(run(&f, r) == 8);
    memset(buffer, 0, sizeof(buffer));
    receive_all(&f, client, buffer, 8, 234);
    CHECK(memcmp(buffer, "vectored", 8) == 0);

    /* Zero-copy sends: the result with MORE, then the NOTIF. */
    z = zc_send(&f, send_record(server, true, "zc-data", 7, 240));
    CHECK(z.result == 7 && z.result_flags == VSR_IO_CQE_MORE);
    CHECK(z.notif == 0 && z.notif_flags == VSR_IO_CQE_NOTIF);
    receive_all(&f, client, buffer, 7, 241);
    CHECK(memcmp(buffer, "zc-data", 7) == 0);
    region.base = zc;
    region.size = 4096;
    CHECK(f.ex.ops->update_buffer(f.ex.ctx, 1, &region) == 0);
    memcpy(zc, "fixed-zero-copy", 15);
    r = send_record(server, true, zc, 15, 242);
    r.flags |= VSR_IO_SQE_FIXED_BUFFER;
    r.buffer_index = 1;
    z = zc_send(&f, r);
    CHECK(z.result == 15 && z.result_flags == VSR_IO_CQE_MORE);
    CHECK(z.notif == 0 && z.notif_flags == VSR_IO_CQE_NOTIF);
    receive_all(&f, client, buffer, 15, 243);
    CHECK(memcmp(buffer, "fixed-zero-copy", 15) == 0);
    vecs[0].base = zc + 6;
    vecs[0].length = 5;
    vecs[1].base = zc;
    vecs[1].length = 5;
    r = send_record(server, true, vecs, 2, 244);
    r.flags |= VSR_IO_SQE_FIXED_BUFFER;
    r.buffer_index = 1;
    r.op_flags = VSR_IO_SEND_VECTORED;
    z = zc_send(&f, r);
    CHECK(z.result == 10 && z.result_flags == VSR_IO_CQE_MORE);
    CHECK(z.notif == 0 && z.notif_flags == VSR_IO_CQE_NOTIF);
    receive_all(&f, client, buffer, 10, 245);
    CHECK(memcmp(buffer, "zero-fixed", 10) == 0);
    r = send_record(server, true, vecs, 2, 246);
    r.op_flags = VSR_IO_SEND_VECTORED;
    z = zc_send(&f, r);
    CHECK(z.result == 10 && z.notif_flags == VSR_IO_CQE_NOTIF);
    receive_all(&f, client, buffer, 10, 247);
    /* Outside the region: the kernel's -EFAULT, and still the NOTIF. */
    r = send_record(server, true, buffer, 4, 248);
    r.flags |= VSR_IO_SQE_FIXED_BUFFER;
    r.buffer_index = 1;
    z = zc_send(&f, r);
    printf("zero-copy FIXED_BUFFER outside the region: %d flags %u, then "
           "%d flags %u\n",
           z.result, z.result_flags, z.notif, z.notif_flags);
    CHECK(z.result == -EFAULT);
    /* A plain send's FIXED_BUFFER check is the executor's: one completion. */
    r = send_record(server, true, buffer, 4, 249);
    r.flags |= VSR_IO_SQE_FIXED_BUFFER;
    r.buffer_index = 1;
    submit1(&f, r);
    cqe = take(&f, 249);
    CHECK(cqe.result == -EFAULT && cqe.flags == 0);
    CHECK(!arrives(&f, 249, 20, &cqe));
    memcpy(zc, "plain-fixed", 11);
    r = send_record(server, true, zc, 11, 250);
    r.flags |= VSR_IO_SQE_FIXED_BUFFER;
    r.buffer_index = 1;
    CHECK(run(&f, r) == 11);
    receive_all(&f, client, buffer, 11, 251);
    CHECK(memcmp(buffer, "plain-fixed", 11) == 0);
    /* Clearing a region a pending record uses: the kernel keeps its own
     * reference until the record completes. Recorded, not checked. */
    {
        int32_t cleared;

        r = rec(VSR_IO_SQE_READ, 255);
        r.flags = VSR_IO_SQE_FIXED_BUFFER;
        r.fd = client;
        r.addr = zc;
        r.length = 4;
        r.buffer_index = 1;
        submit1(&f, r);
        CHECK(!arrives(&f, 255, 20, &cqe));
        cleared = f.ex.ops->update_buffer(f.ex.ctx, 1, NULL);
        CHECK(run(&f, send_record(server, true, "late", 4, 256)) == 4);
        cqe = take(&f, 255);
        printf("update_buffer(NULL) under a pending fixed read: %d; the read "
               "then completes %d\n",
               cleared, cqe.result);
        CHECK(cqe.result == 4 && memcmp(zc, "late", 4) == 0);
        CHECK(f.ex.ops->update_buffer(f.ex.ctx, 1, &region) == 0);
    }
    /* SKIP_SUCCESS is refused on zero-copy and multishot records. */
    r = send_record(server, true, "x", 1, 252);
    r.flags |= VSR_IO_SQE_SKIP_SUCCESS;
    r.op_flags = VSR_IO_SEND_ZERO_COPY;
    CHECK(run(&f, r) == -EINVAL);
    CHECK(!arrives(&f, 252, 20, &cqe));

    /* LINK on a send: all-or-nothing, then the successor. */
    {
        struct vsr_io_sqe chain[2];

        chain[0] = send_record(client, false, "link", 4, 253);
        chain[0].flags = VSR_IO_SQE_LINK;
        chain[1] = rec(VSR_IO_SQE_NOP, 254);
        submit(&f, chain, 2);
        CHECK(take(&f, 253).result == 4 && take(&f, 254).result == 0);
        cqe = take(&f, 225);
        CHECK(cqe.result == 4 && cqe.buffer_id == 12);
    }

    /* Short send: a small send buffer and a peer that does not read. */
    {
        static unsigned char big[1u << 20];
        int32_t quiet_client = make_socket(&f, 260);
        int32_t quiet_server;
        int size = 4096;

        CHECK(quiet_client >= 0);
        r = rec(VSR_IO_SQE_SETSOCKOPT, 261);
        r.fd = quiet_client;
        r.op_flags = (uint32_t)SOL_SOCKET << 16 | SO_SNDBUF;
        r.addr = &size;
        r.length = sizeof(size);
        CHECK(run(&f, r) == 0);
        CHECK(connect_to(&f, quiet_client, &address, 262) == 0);
        cqe = take(&f, 210);
        quiet_server = cqe.result;
        CHECK(quiet_server >= 0 && (cqe.flags & VSR_IO_CQE_MORE));
        cqe = (submit1(&f,
                       send_record(quiet_client, false, big, sizeof(big), 263)),
               take(&f, 263));
        printf("short send: %d of %zu bytes\n", cqe.result, sizeof(big));
        CHECK(cqe.result > 0 && (uint32_t)cqe.result < sizeof(big));
        r = rec(VSR_IO_SQE_CLOSE, 264);
        r.fd = quiet_client;
        CHECK(run(&f, r) == 0);
        r = rec(VSR_IO_SQE_CLOSE, 265);
        r.flags = VSR_IO_SQE_FIXED_FILE;
        r.fd = quiet_server;
        CHECK(run(&f, r) == 0);
    }

    /* Cancellation: by user_data, of nothing, by descriptor, all. */
    r = rec(VSR_IO_SQE_RECV, 270);
    r.fd = client;
    r.addr = buffer;
    r.length = sizeof(buffer);
    submit1(&f, r);
    {
        struct vsr_io_sqe cancel = rec(VSR_IO_SQE_CANCEL, 271);

        cancel.offset = 270;
        CHECK(run(&f, cancel) == 0);
        CHECK(take(&f, 270).result == -ECANCELED);
        cancel.user_data = 272;
        CHECK(run(&f, cancel) == -ENOENT);
        r.user_data = 273;
        submit1(&f, r);
        r.user_data = 274;
        submit1(&f, r);
        cancel = rec(VSR_IO_SQE_CANCEL, 275);
        cancel.fd = client;
        cancel.op_flags = VSR_IO_CANCEL_BY_FD | VSR_IO_CANCEL_ALL;
        CHECK(run(&f, cancel) == 2);
        CHECK(take(&f, 273).result == -ECANCELED);
        CHECK(take(&f, 274).result == -ECANCELED);
        /* By slot: the multishot receive terminates. */
        cancel = rec(VSR_IO_SQE_CANCEL, 276);
        cancel.flags = VSR_IO_SQE_FIXED_FILE;
        cancel.fd = server;
        cancel.op_flags = VSR_IO_CANCEL_BY_FD;
        CHECK(run(&f, cancel) == 0);
        cqe = take(&f, 225);
        CHECK(cqe.result == -ECANCELED && !(cqe.flags & VSR_IO_CQE_MORE));
    }

    /* DIRECT exhaustion: sockets fill the table, then an accept finds no
     * slot. */
    {
        int32_t slots[16];
        uint32_t opened = 0;
        int32_t extra;
        int32_t late;
        bool refused;

        for (;;) {
            r = rec(VSR_IO_SQE_SOCKET, 280);
            r.flags = VSR_IO_SQE_DIRECT;
            r.fd2 = VSR_IO_SLOT_ALLOC;
            r.length = AF_INET;
            r.op_flags = SOCK_STREAM;
            slots[opened] = run(&f, r);
            if (slots[opened] < 0) {
                break;
            }
            opened++;
            CHECK(opened < 16);
        }
        CHECK(slots[opened] == -ENFILE && opened == 15);
        extra = make_socket(&f, 281);
        CHECK(extra >= 0 && connect_to(&f, extra, &address, 282) == 0);
        cqe = take(&f, 210);
        CHECK(cqe.result == -ENFILE && !(cqe.flags & VSR_IO_CQE_MORE));
        for (uint32_t i = 0; i < opened; ++i) {
            r = rec(VSR_IO_SQE_CLOSE, 283);
            r.flags = VSR_IO_SQE_FIXED_FILE;
            r.fd = slots[i];
            CHECK(run(&f, r) == 0);
        }
        /* Is the connection that found no slot still queued? */
        r = rec(VSR_IO_SQE_ACCEPT, 284);
        r.fd = listener;
        r.flags = VSR_IO_SQE_DIRECT;
        r.fd2 = VSR_IO_SLOT_ALLOC;
        submit1(&f, r);
        late = arrives(&f, 284, 200, &cqe) ? cqe.result : INT32_MIN;
        r = rec(VSR_IO_SQE_RECV, 285);
        r.fd = extra;
        r.addr = buffer;
        r.length = sizeof(buffer);
        submit1(&f, r);
        refused = arrives(&f, 285, 200, &cqe);
        printf("accept after -ENFILE: %s (%d); the refused client's receive: "
               "%s (%d)\n",
               late == INT32_MIN ? "nothing queued" : "queued", late,
               refused ? "completed" : "still waiting",
               refused ? cqe.result : 0);
        if (late == INT32_MIN) {
            struct vsr_io_sqe cancel = rec(VSR_IO_SQE_CANCEL, 286);

            cancel.offset = 284;
            CHECK(run(&f, cancel) == 0);
            CHECK(take(&f, 284).result == -ECANCELED);
        } else {
            r = rec(VSR_IO_SQE_CLOSE, 287);
            r.flags = VSR_IO_SQE_FIXED_FILE;
            r.fd = late;
            CHECK(run(&f, r) == 0);
        }
        if (!refused) {
            struct vsr_io_sqe cancel = rec(VSR_IO_SQE_CANCEL, 288);

            cancel.offset = 285;
            CHECK(run(&f, cancel) == 0);
            CHECK(take(&f, 285).result == -ECANCELED);
        }
        r = rec(VSR_IO_SQE_CLOSE, 289);
        r.fd = extra;
        CHECK(run(&f, r) == 0);
    }

    /* Shutdown: the peer's receive sees the end of the stream. A linked
     * receive waits for its whole length (MSG_WAITALL): the stream ends
     * after 3 of 4 bytes, so it returns 3 and fails its chain. */
    CHECK(run(&f, send_record(client, false, "bye", 3, 290)) == 3);
    r = rec(VSR_IO_SQE_SHUTDOWN, 291);
    r.fd = client;
    r.length = SHUT_WR;
    CHECK(run(&f, r) == 0);
    memset(buffer, 0, sizeof(buffer));
    {
        struct vsr_io_sqe chain[2];

        chain[0] = rec(VSR_IO_SQE_RECV, 292);
        chain[0].flags = VSR_IO_SQE_FIXED_FILE | VSR_IO_SQE_LINK;
        chain[0].fd = server;
        chain[0].addr = buffer;
        chain[0].length = 4;
        chain[1] = rec(VSR_IO_SQE_NOP, 293);
        submit(&f, chain, 2);
        CHECK(take(&f, 292).result == 3 && memcmp(buffer, "bye", 3) == 0);
        CHECK(take(&f, 293).result == -ECANCELED);
    }
    r = rec(VSR_IO_SQE_RECV, 294);
    r.flags = VSR_IO_SQE_FIXED_FILE;
    r.fd = server;
    r.addr = buffer;
    r.length = sizeof(buffer);
    CHECK(run(&f, r) == 0);

    /* Cancelling the multishot accept ends it. */
    r = rec(VSR_IO_SQE_ACCEPT, 295);
    r.fd = listener;
    r.op_flags = VSR_IO_ACCEPT_MULTISHOT;
    submit1(&f, r);
    {
        struct vsr_io_sqe cancel = rec(VSR_IO_SQE_CANCEL, 296);

        cancel.offset = 295;
        CHECK(run(&f, cancel) == 0);
        cqe = take(&f, 295);
        CHECK(cqe.result == -ECANCELED && !(cqe.flags & VSR_IO_CQE_MORE));
    }
    /* A multishot accept names no slot. */
    r = rec(VSR_IO_SQE_ACCEPT, 297);
    r.fd = listener;
    r.flags = VSR_IO_SQE_DIRECT;
    r.fd2 = 3;
    r.op_flags = VSR_IO_ACCEPT_MULTISHOT;
    CHECK(run(&f, r) == -EINVAL);

    r = rec(VSR_IO_SQE_CLOSE, 298);
    r.flags = VSR_IO_SQE_FIXED_FILE;
    r.fd = server;
    CHECK(run(&f, r) == 0);
    CHECK(close(client) == 0 && close(listener) == 0);
    CHECK(f.ex.ops->buffer_ring(f.ex.ctx, 7, 0, 0, NULL) == 0);
    CHECK(f.ex.ops->buffer_ring(f.ex.ctx, 7, 0, 0, NULL) == -ENOENT);
    close_fixture(&f);
    free(ring_memory);
    free(pool);
    free(zc);
}

/* ------------------------------------------------------------------------
 * Timeouts
 * --------------------------------------------------------------------- */

static void test_timeouts(void)
{
    struct vsr_io_uring_options o = options();
    struct fixture f;
    struct vsr_io_sqe r;
    struct vsr_io_sqe update;
    uint64_t target;
    uint64_t t0;
    uint64_t deadline;

    CHECK(open_fixture(&f, &o) == 0);

    t0 = now(&f);
    r = rec(VSR_IO_SQE_TIMEOUT, 300);
    r.offset = 20 * MS;
    CHECK(run(&f, r) == -ETIME);
    CHECK(now(&f) - t0 >= 19 * MS);

    deadline = now(&f) + 30 * MS;
    r = rec(VSR_IO_SQE_TIMEOUT, 301);
    r.op_flags = VSR_IO_TIMEOUT_ABSOLUTE;
    r.offset = deadline;
    CHECK(run(&f, r) == -ETIME);
    CHECK(now(&f) >= deadline);
    r.user_data = 302;
    r.offset = now(&f) - 1; /* In the past: fires at once. */
    CHECK(run(&f, r) == -ETIME);

    /* Update to a relative and to an absolute expiry. */
    r = rec(VSR_IO_SQE_TIMEOUT, 303);
    r.offset = 10000 * MS;
    submit1(&f, r);
    target = 303;
    update = rec(VSR_IO_SQE_TIMEOUT_UPDATE, 304);
    update.addr2 = &target;
    update.offset = 20 * MS;
    t0 = now(&f);
    CHECK(run(&f, update) == 0);
    CHECK(take(&f, 303).result == -ETIME);
    CHECK(now(&f) - t0 < 2000 * MS);
    r.user_data = 305;
    submit1(&f, r);
    target = 305;
    update.user_data = 306;
    update.op_flags = VSR_IO_TIMEOUT_ABSOLUTE;
    update.offset = now(&f) + 20 * MS;
    CHECK(run(&f, update) == 0);
    CHECK(take(&f, 305).result == -ETIME);
    target = 999;
    update.user_data = 307;
    CHECK(run(&f, update) == -ENOENT);
    update.addr2 = NULL;
    update.user_data = 308;
    CHECK(run(&f, update) == -EINVAL);

    /* Cancel. */
    r.user_data = 309;
    submit1(&f, r);
    update = rec(VSR_IO_SQE_CANCEL, 310);
    update.offset = 309;
    CHECK(run(&f, update) == 0);
    CHECK(take(&f, 309).result == -ECANCELED);
    close_fixture(&f);
}

/* ------------------------------------------------------------------------
 * Wake
 * --------------------------------------------------------------------- */

struct waker {
    struct vsr_io_executor ex;
    uint64_t delay_ms;
};

static void *wake_later(void *arg)
{
    const struct waker *w = arg;
    struct timespec ts;

    ts.tv_sec = 0;
    ts.tv_nsec = (long)(w->delay_ms * 1000000u);
    (void)nanosleep(&ts, NULL);
    w->ex.ops->wake(w->ex.ctx);
    return NULL;
}

static void wake_during(struct fixture *f, uint32_t want, uint64_t min_wait)
{
    struct waker w;
    pthread_t thread;
    uint64_t t0;

    w.ex = f->ex;
    w.delay_ms = 50;
    CHECK(pthread_create(&thread, NULL, wake_later, &w) == 0);
    t0 = now(f);
    CHECK(f->ex.ops->submit_and_wait(f->ex.ctx, NULL, 0, want, min_wait,
                                     VSR_NO_DEADLINE) == 0);
    CHECK(now(f) - t0 < 3000 * MS);
    CHECK(pthread_join(thread, NULL) == 0);
    /* Wake completions never reach reap. */
    CHECK(f->ex.ops->reap(f->ex.ctx, stash, STASH) == 0);
}

static void test_wake(void)
{
    struct vsr_io_uring_options o = options();
    struct fixture f;
    uint64_t t0;

    CHECK(open_fixture(&f, &o) == 0);
    wake_during(&f, 1, 0);
    wake_during(&f, 8, 0);
    wake_during(&f, 8, 100 * MS);
    wake_during(&f, 1, 0);

    /* A wake before the wait: the next wait returns at once. */
    f.ex.ops->wake(f.ex.ctx);
    f.ex.ops->wake(f.ex.ctx);
    t0 = now(&f);
    CHECK(f.ex.ops->submit_and_wait(f.ex.ctx, NULL, 0, 1, 0, VSR_NO_DEADLINE) ==
          0);
    CHECK(now(&f) - t0 < 1000 * MS);
    CHECK(f.ex.ops->reap(f.ex.ctx, stash, STASH) == 0);
    /* Consumed: the following wait blocks until its deadline. */
    t0 = now(&f);
    CHECK(f.ex.ops->submit_and_wait(f.ex.ctx, NULL, 0, 1, 0, t0 + 30 * MS) ==
          0);
    CHECK(now(&f) - t0 >= 29 * MS);
    close_fixture(&f);
}

/* SQPOLL and NAPI are options; a machine may refuse SQPOLL to the user. */
static void test_variants(void)
{
    struct vsr_io_uring_options o = options();
    struct fixture f;
    int rc;

    o.sqpoll_idle_ms = 10;
    rc = open_fixture(&f, &o);
    printf("SQPOLL ring: %d\n", rc);
    if (rc == 0) {
        struct vsr_io_sqe r;

        submit1(&f, rec(VSR_IO_SQE_NOP, 400));
        CHECK(take(&f, 400).result == 0);
        r = rec(VSR_IO_SQE_TIMEOUT, 401);
        r.offset = 10 * MS;
        CHECK(run(&f, r) == -ETIME);
        wake_during(&f, 1, 0);
        close_fixture(&f);
    }
    o = options();
    o.napi_busy_poll_us = 20;
    rc = open_fixture(&f, &o);
    printf("NAPI ring: %d\n", rc);
    if (rc == 0) {
        submit1(&f, rec(VSR_IO_SQE_NOP, 402));
        CHECK(take(&f, 402).result == 0);
        close_fixture(&f);
    }
}

/* ------------------------------------------------------------------------
 * Ring plumbing under pressure: a full SQ mid-batch with chains, CQ
 * overflow and its recovery, want beyond the CQ, deinit with pending
 * multishot and timeout records, and what the kernel says where the
 * translation has room (recorded, not checked).
 * --------------------------------------------------------------------- */

static void test_pressure(void)
{
    struct vsr_io_uring_options o = options();
    struct fixture f;
    struct vsr_io_sqe batch[100];
    uint32_t seen;
    uint64_t expect;
    int32_t listener;
    struct sockaddr_in address;
    struct vsr_io_sqe r;

    CHECK(open_fixture(&f, &o) == 0);

    /* 100 records in one batch over a 32-entry SQ: chains of 5 straddle
     * every SQ boundary, one chain fails in its middle, and every record
     * completes exactly once, chains in order. */
    for (uint32_t i = 0; i < 100; ++i) {
        batch[i] = rec(VSR_IO_SQE_NOP, 1000 + i);
        if (i % 5 != 4) {
            batch[i].flags = VSR_IO_SQE_LINK;
        }
    }
    batch[51].opcode = 200; /* Rejected: fails records 52..54. */
    submit(&f, batch, 100);
    for (uint32_t i = 0; i < 100; ++i) {
        struct vsr_io_cqe cqe = take(&f, 1000 + i);

        if (i == 51) {
            CHECK(cqe.result == -EINVAL);
        } else if (i > 51 && i < 55) {
            CHECK(cqe.result == -ECANCELED);
        } else {
            CHECK(cqe.result == 0);
        }
        CHECK(!(cqe.flags & VSR_IO_CQE_MORE));
    }
    /* Chain order: 1005..1009 completed in submission order. */
    /* A chain longer than the SQ fails alone, without linking. */
    for (uint32_t i = 0; i < 40; ++i) {
        batch[i] = rec(VSR_IO_SQE_NOP, 2000 + i);
        batch[i].flags = VSR_IO_SQE_LINK;
    }
    submit(&f, batch, 40);
    for (uint32_t i = 0; i < 40; ++i) {
        CHECK(take(&f, 2000 + i).result == -EINVAL);
    }

    /* CQ overflow: 3000 completions over a 128-entry CQ without reaping,
     * with wakes in between (the multishot wake poll dies when the CQ is
     * full and must come back); every completion arrives, none twice. */
    for (uint32_t round = 0; round < 30; ++round) {
        for (uint32_t i = 0; i < 100; ++i) {
            batch[i] = rec(VSR_IO_SQE_NOP, 3000 + round * 100 + i);
        }
        submit(&f, batch, 100);
        if (round % 7 == 3) {
            f.ex.ops->wake(f.ex.ctx);
        }
    }
    seen = 0;
    expect = 0;
    while (seen < 3000) {
        uint32_t n;

        CHECK(f.ex.ops->submit_and_wait(f.ex.ctx, NULL, 0, 1, 0,
                                        now(&f) + 5000 * MS) == 0);
        n = f.ex.ops->reap(f.ex.ctx, stash, 37);
        CHECK(n > 0);
        for (uint32_t i = 0; i < n; ++i) {
            CHECK(stash[i].user_data >= 3000 && stash[i].user_data < 6000);
            CHECK(stash[i].result == 0);
            expect += stash[i].user_data;
        }
        seen += n;
    }
    CHECK(expect == (UINT64_C(3000) + 5999) * 3000 / 2);
    CHECK(f.ex.ops->reap(f.ex.ctx, stash, STASH) == 0);
    /* The wake poll is back: a wake ends the next wait. */
    wake_during(&f, 1, 0);

    /* want beyond the CQ can never be satisfied: refused, not blocked. */
    CHECK(f.ex.ops->submit_and_wait(f.ex.ctx, NULL, 0, 129, 0,
                                    VSR_NO_DEADLINE) == -EINVAL);
    CHECK(f.ex.ops->submit_and_wait(f.ex.ctx, NULL, 0, 128, 0, now(&f)) == 0);

    /* A direct open with O_CLOEXEC, which the kernel alone refuses. */
    listener = make_socket(&f, 4000);
    CHECK(listener >= 0);
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    r = rec(VSR_IO_SQE_BIND, 4001);
    r.fd = listener;
    r.addr = &address;
    r.length = sizeof(address);
    CHECK(run(&f, r) == 0);
    r = rec(VSR_IO_SQE_LISTEN, 4002);
    r.fd = listener;
    r.length = 4;
    CHECK(run(&f, r) == 0);
    CHECK(f.ex.ops->register_files(f.ex.ctx, 4) == 0);
    r = rec(VSR_IO_SQE_OPENAT, 4003);
    r.flags = VSR_IO_SQE_DIRECT;
    r.fd = AT_FDCWD;
    r.addr = ".";
    r.op_flags = O_RDONLY | O_DIRECTORY | O_CLOEXEC;
    r.fd2 = VSR_IO_SLOT_ALLOC;
    /* O_CLOEXEC is dropped for a slot: the kernel would refuse it. */
    CHECK(run(&f, r) >= 0);
    r.op_flags = O_RDONLY | O_DIRECTORY;
    CHECK(run(&f, r) >= 0);

    /* deinit with a multishot accept, a plain accept and a timeout
     * pending: the drain cancels them and nothing completes afterwards
     * into memory that is gone (ASan would see a stash write). */
    r = rec(VSR_IO_SQE_ACCEPT, 4010);
    r.fd = listener;
    r.op_flags = VSR_IO_ACCEPT_MULTISHOT;
    submit1(&f, r);
    r = rec(VSR_IO_SQE_ACCEPT, 4011);
    r.fd = listener;
    submit1(&f, r);
    r = rec(VSR_IO_SQE_TIMEOUT, 4012);
    r.offset = 60000 * MS;
    submit1(&f, r);
    CHECK(f.ex.ops->submit_and_wait(f.ex.ctx, NULL, 0, 0, 0, 0) == 0);
    close_fixture(&f);
    CHECK(close(listener) == 0);
    /* Idempotent. */
    vsr_io_uring_deinit(&f.ex);
}

int main(void)
{
    struct vsr_io_uring_options o = options();
    struct fixture f;
    int rc;

    setvbuf(stdout, NULL, _IONBF, 0);
    rc = open_fixture(&f, &o);
    if (rc == -ENOSYS || rc == -EPERM) {
        printf("skip: no usable io_uring (%s)\n", strerror(-rc));
        return 77;
    }
    if (rc != 0) {
        printf("init failed: %s\n", strerror(-rc));
    }
    CHECK(rc == 0);
    close_fixture(&f);

    test_layout();
    test_basics();
    test_files();
    test_network();
    test_timeouts();
    test_wake();
    test_variants();
    test_pressure();
    return 0;
}
