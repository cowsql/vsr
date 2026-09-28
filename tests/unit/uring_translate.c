/* Record -> SQE and CQE -> record translation of the io_uring executor,
 * against SQEs in test memory (no ring): every opcode, every flag
 * combination, and the FIXED_BUFFER region checks.
 *
 * A record the executor rejects is reported by vsr_io_uring_translate as
 * a negative errno; the executor then submits a NOP that completes with
 * that errno in the record's place (tests/integration/uring_smoke checks
 * that on a real ring). Out-of-region FIXED_BUFFER records are rejected
 * here with -EFAULT, the errno the kernel would produce, so the failure
 * is deterministic and identical to the simulation's; zero-copy sends are
 * the exception and keep the kernel's check, because only a kernel failure
 * is followed by the NOTIF completion the contract promises. */
#define _GNU_SOURCE
#include "config.h"

#include "io/uring.h"
#include "lib/check.h"

#include <errno.h>
#include <fcntl.h>
#include <liburing.h>
#include <netinet/in.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>

#define REGION_BYTES 4096u

static unsigned char region_memory[REGION_BYTES];
static unsigned char outside[64];
static struct vsr_io_uring_region regions[3];
static struct vsr_io_uring_direct directs[4];
static struct vsr_io_uring_timespec timespecs[1];
static struct vsr_io_uring uring;

static void reset(void)
{
    memset(&uring, 0, sizeof(uring));
    memset(regions, 0, sizeof(regions));
    regions[0].base = region_memory;
    regions[0].size = REGION_BYTES;
    /* regions[1] registered but empty; index 2 is past the table. */
    uring.buffers = regions;
    uring.buffers_registered = 2;
    for (size_t i = 0; i < sizeof(directs) / sizeof(directs[0]); ++i) {
        directs[i].user_data = 0;
        directs[i].slot = -1;
    }
    uring.directs = directs;
    uring.directs_capacity = sizeof(directs) / sizeof(directs[0]);
    uring.timespecs = timespecs;
    uring.timespecs_count = 1;
    uring.wake_fd = -1;
}

static struct vsr_io_sqe rec(uint8_t opcode)
{
    struct vsr_io_sqe r;

    memset(&r, 0, sizeof(r));
    r.opcode = opcode;
    r.user_data = UINT64_C(0x0102030405060708);
    return r;
}

static int tr(struct vsr_io_sqe r, struct io_uring_sqe *sqe)
{
    /* Garbage first: translation must define every field it relies on. */
    memset(sqe, 0xa5, sizeof(*sqe));
    return vsr_io_uring_translate(&uring, &r, sqe);
}

static bool zeroed(const struct io_uring_sqe *sqe)
{
    const unsigned char *bytes = (const unsigned char *)sqe;

    for (size_t i = 0; i < sizeof(*sqe); ++i) {
        if (bytes[i] != 0) {
            return false;
        }
    }
    return true;
}

static int rejected(struct vsr_io_sqe r)
{
    struct io_uring_sqe sqe;
    int rc = tr(r, &sqe);

    CHECK(rc < 0 && zeroed(&sqe));
    return rc;
}

static uint64_t ptr(const void *p)
{
    return (uint64_t)(uintptr_t)p;
}

/* ------------------------------------------------------------------------
 * Every opcode, field by field
 * --------------------------------------------------------------------- */

static void test_nop(void)
{
    struct io_uring_sqe sqe;

    CHECK(tr(rec(VSR_IO_SQE_NOP), &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_NOP && sqe.flags == 0 &&
          sqe.user_data == UINT64_C(0x0102030405060708));
}

static void test_read_write(void)
{
    struct io_uring_sqe sqe;
    struct vsr_io_sqe r;
    struct vsr_io_vec vecs[2];

    r = rec(VSR_IO_SQE_READ);
    r.fd = 7;
    r.addr = outside;
    r.length = 16;
    r.offset = 4096;
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_READ && sqe.fd == 7 &&
          sqe.addr == ptr(outside) && sqe.len == 16 && sqe.off == 4096 &&
          sqe.buf_index == 0);
    r.opcode = VSR_IO_SQE_WRITE;
    CHECK(tr(r, &sqe) == 0 && sqe.opcode == IORING_OP_WRITE &&
          sqe.addr == ptr(outside) && sqe.len == 16 && sqe.off == 4096);

    /* Fixed: inside region 0, up to its last byte. */
    r = rec(VSR_IO_SQE_READ);
    r.flags = VSR_IO_SQE_FIXED_BUFFER;
    r.fd = 7;
    r.addr = region_memory + 100;
    r.length = REGION_BYTES - 100;
    r.offset = 8;
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_READ_FIXED && sqe.buf_index == 0 &&
          sqe.addr == ptr(region_memory + 100) &&
          sqe.len == REGION_BYTES - 100 && sqe.off == 8);
    r.opcode = VSR_IO_SQE_WRITE;
    CHECK(tr(r, &sqe) == 0 && sqe.opcode == IORING_OP_WRITE_FIXED);
    r.length = 0;
    r.addr = region_memory + REGION_BYTES; /* Empty, at the end. */
    CHECK(tr(r, &sqe) == 0);

    /* Outside the region, an empty index, past the table: -EFAULT. */
    r.addr = region_memory + 100;
    r.length = REGION_BYTES - 99;
    CHECK(rejected(r) == -EFAULT);
    r.addr = region_memory + REGION_BYTES + 1;
    r.length = 0;
    CHECK(rejected(r) == -EFAULT);
    r.addr = outside;
    r.length = 1;
    CHECK(rejected(r) == -EFAULT);
    r.addr = region_memory;
    r.buffer_index = 1;
    CHECK(rejected(r) == -EFAULT);
    r.buffer_index = 2;
    CHECK(rejected(r) == -EFAULT);
    r.buffer_index = 0;
    uring.buffers_registered = 0;
    CHECK(rejected(r) == -EFAULT);
    reset();

    /* Vectors. */
    vecs[0].base = region_memory;
    vecs[0].length = 8;
    vecs[1].base = region_memory + 1024;
    vecs[1].length = 8;
    r = rec(VSR_IO_SQE_READV);
    r.fd = 3;
    r.addr = vecs;
    r.length = 2;
    r.offset = 512;
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_READV && sqe.addr == ptr(vecs) &&
          sqe.len == 2 && sqe.off == 512);
    r.opcode = VSR_IO_SQE_WRITEV;
    CHECK(tr(r, &sqe) == 0 && sqe.opcode == IORING_OP_WRITEV);
    r.flags = VSR_IO_SQE_FIXED_BUFFER;
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_WRITEV_FIXED && sqe.buf_index == 0 &&
          sqe.addr == ptr(vecs) && sqe.len == 2);
    r.opcode = VSR_IO_SQE_READV;
    CHECK(tr(r, &sqe) == 0 && sqe.opcode == IORING_OP_READV_FIXED);
    vecs[1].base = outside;
    CHECK(rejected(r) == -EFAULT);
    r.opcode = VSR_IO_SQE_WRITEV;
    CHECK(rejected(r) == -EFAULT);
    r.length = 1025;
    CHECK(rejected(r) == -EINVAL);
    r.flags = 0;
    CHECK(rejected(r) == -EINVAL);
    r.flags = VSR_IO_SQE_FIXED_BUFFER;
    r.length = 1;
    r.addr = NULL;
    CHECK(rejected(r) == -EFAULT);
}

static void test_files(void)
{
    struct io_uring_sqe sqe;
    struct vsr_io_sqe r;
    struct statx stx;

    r = rec(VSR_IO_SQE_FSYNC);
    r.fd = 4;
    CHECK(tr(r, &sqe) == 0 && sqe.opcode == IORING_OP_FSYNC && sqe.fd == 4 &&
          sqe.fsync_flags == 0);
    r.op_flags = VSR_IO_FSYNC_DATASYNC;
    CHECK(tr(r, &sqe) == 0 && sqe.fsync_flags == IORING_FSYNC_DATASYNC);
    r.op_flags = 2;
    CHECK(rejected(r) == -EINVAL);

    r = rec(VSR_IO_SQE_FALLOCATE);
    r.fd = 4;
    r.offset = 4096;
    r.length = 8192;
    CHECK(tr(r, &sqe) == 0 && sqe.opcode == IORING_OP_FALLOCATE &&
          sqe.off == 4096 && sqe.addr == 8192 && sqe.len == 0);

    r = rec(VSR_IO_SQE_OPENAT);
    r.fd = AT_FDCWD;
    r.addr = "path";
    r.op_flags = O_RDWR | O_CREAT;
    r.length = 0600;
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_OPENAT && sqe.fd == AT_FDCWD &&
          sqe.addr == ptr(r.addr) && sqe.open_flags == (O_RDWR | O_CREAT) &&
          sqe.len == 0600 && sqe.file_index == 0);
    r.flags = VSR_IO_SQE_DIRECT;
    r.fd2 = VSR_IO_SLOT_ALLOC;
    CHECK(tr(r, &sqe) == 0 && sqe.file_index == IORING_FILE_INDEX_ALLOC);
    CHECK(uring.directs_count == 0);
    /* A named slot: the kernel completes with 0, so the executor keeps
     * (user_data, slot) to report the slot. */
    r.fd2 = 3;
    CHECK(tr(r, &sqe) == 0 && sqe.file_index == 4);
    CHECK(uring.directs_count == 1 && directs[0].slot == 3 &&
          directs[0].user_data == r.user_data);
    r.flags |= VSR_IO_SQE_SKIP_SUCCESS; /* No completion to report. */
    CHECK(tr(r, &sqe) == 0 && uring.directs_count == 1);
    r.flags = VSR_IO_SQE_DIRECT;
    r.fd2 = -7;
    CHECK(rejected(r) == -EINVAL);
    r.fd2 = 1;
    for (uint32_t i = 1; i < uring.directs_capacity; ++i) {
        CHECK(tr(r, &sqe) == 0);
    }
    CHECK(rejected(r) == -EAGAIN); /* Table full. */
    reset();

    r = rec(VSR_IO_SQE_CLOSE);
    r.fd = 9;
    CHECK(tr(r, &sqe) == 0 && sqe.opcode == IORING_OP_CLOSE && sqe.fd == 9 &&
          sqe.file_index == 0 && sqe.flags == 0);
    r.flags = VSR_IO_SQE_FIXED_FILE;
    CHECK(tr(r, &sqe) == 0 && sqe.opcode == IORING_OP_CLOSE &&
          sqe.file_index == 10 && sqe.fd == 0 && sqe.flags == 0);
    r.fd = -1;
    CHECK(rejected(r) == -EBADF);

    r = rec(VSR_IO_SQE_RENAMEAT);
    r.fd = 5;
    r.addr = "old";
    r.addr2 = "new";
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_RENAMEAT && sqe.fd == 5 && sqe.len == 5 &&
          sqe.addr == ptr(r.addr) && sqe.addr2 == ptr(r.addr2) &&
          sqe.rename_flags == 0);

    r = rec(VSR_IO_SQE_UNLINKAT);
    r.fd = 5;
    r.addr = "name";
    r.op_flags = AT_REMOVEDIR;
    CHECK(tr(r, &sqe) == 0 && sqe.opcode == IORING_OP_UNLINKAT && sqe.fd == 5 &&
          sqe.addr == ptr(r.addr) && sqe.unlink_flags == AT_REMOVEDIR);

    r = rec(VSR_IO_SQE_MKDIRAT);
    r.fd = 5;
    r.addr = "dir";
    r.length = 0755;
    CHECK(tr(r, &sqe) == 0 && sqe.opcode == IORING_OP_MKDIRAT &&
          sqe.len == 0755 && sqe.addr == ptr(r.addr));

    r = rec(VSR_IO_SQE_STATX);
    r.fd = 5;
    r.addr = "file";
    r.addr2 = &stx;
    r.op_flags = AT_SYMLINK_NOFOLLOW;
    r.length = STATX_SIZE;
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_STATX && sqe.addr == ptr(r.addr) &&
          sqe.off == ptr(&stx) && sqe.len == STATX_SIZE &&
          sqe.statx_flags == AT_SYMLINK_NOFOLLOW);
}

static void test_sockets(void)
{
    struct io_uring_sqe sqe;
    struct vsr_io_sqe r;
    struct sockaddr_storage address;
    int value = 1;

    r = rec(VSR_IO_SQE_SOCKET);
    r.length = AF_INET6;
    r.op_flags = SOCK_STREAM;
    r.offset = IPPROTO_TCP;
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_SOCKET && sqe.fd == AF_INET6 &&
          sqe.off == SOCK_STREAM && sqe.len == IPPROTO_TCP &&
          sqe.file_index == 0);
    r.flags = VSR_IO_SQE_DIRECT;
    r.fd2 = VSR_IO_SLOT_ALLOC;
    CHECK(tr(r, &sqe) == 0 && sqe.file_index == IORING_FILE_INDEX_ALLOC);
    r.fd2 = 0;
    CHECK(tr(r, &sqe) == 0 && sqe.file_index == 1);
    reset();

    r = rec(VSR_IO_SQE_CONNECT);
    r.fd = 6;
    r.addr = &address;
    r.length = 16;
    CHECK(tr(r, &sqe) == 0 && sqe.opcode == IORING_OP_CONNECT &&
          sqe.addr == ptr(&address) && sqe.off == 16);
    r.opcode = VSR_IO_SQE_BIND;
    CHECK(tr(r, &sqe) == 0 && sqe.opcode == IORING_OP_BIND &&
          sqe.addr == ptr(&address) && sqe.off == 16);
    r = rec(VSR_IO_SQE_LISTEN);
    r.fd = 6;
    r.length = 64;
    CHECK(tr(r, &sqe) == 0 && sqe.opcode == IORING_OP_LISTEN && sqe.len == 64);

    r = rec(VSR_IO_SQE_ACCEPT);
    r.fd = 6;
    CHECK(tr(r, &sqe) == 0 && sqe.opcode == IORING_OP_ACCEPT &&
          sqe.accept_flags == SOCK_CLOEXEC && sqe.ioprio == 0 &&
          sqe.file_index == 0);
    r.op_flags = VSR_IO_ACCEPT_MULTISHOT;
    CHECK(tr(r, &sqe) == 0 && (sqe.ioprio & IORING_ACCEPT_MULTISHOT));
    r.flags = VSR_IO_SQE_DIRECT;
    r.fd2 = VSR_IO_SLOT_ALLOC;
    CHECK(tr(r, &sqe) == 0 && (sqe.ioprio & IORING_ACCEPT_MULTISHOT) &&
          sqe.file_index == IORING_FILE_INDEX_ALLOC && sqe.accept_flags == 0);
    r.fd2 = 2; /* The kernel's multishot accept only allocates. */
    CHECK(rejected(r) == -EINVAL);
    r.fd2 = VSR_IO_SLOT_ALLOC;
    r.flags |= VSR_IO_SQE_SKIP_SUCCESS;
    CHECK(rejected(r) == -EINVAL);
    r.flags = VSR_IO_SQE_DIRECT;
    r.op_flags = 0;
    r.fd2 = 2;
    CHECK(tr(r, &sqe) == 0 && sqe.ioprio == 0 && sqe.file_index == 3);
    r.op_flags = 4;
    CHECK(rejected(r) == -EINVAL);
    reset();

    r = rec(VSR_IO_SQE_SHUTDOWN);
    r.fd = 6;
    r.length = SHUT_WR;
    CHECK(tr(r, &sqe) == 0 && sqe.opcode == IORING_OP_SHUTDOWN &&
          sqe.len == SHUT_WR);

    r = rec(VSR_IO_SQE_SETSOCKOPT);
    r.fd = 6;
    r.op_flags = (uint32_t)IPPROTO_TCP << 16 | 1u;
    r.addr = &value;
    r.length = sizeof(value);
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_URING_CMD &&
          sqe.cmd_op == SOCKET_URING_OP_SETSOCKOPT && sqe.fd == 6 &&
          sqe.level == IPPROTO_TCP && sqe.optname == 1 &&
          sqe.optval == ptr(&value) && sqe.optlen == sizeof(value));
    r.opcode = VSR_IO_SQE_GETSOCKOPT;
    CHECK(tr(r, &sqe) == 0 && sqe.cmd_op == SOCKET_URING_OP_GETSOCKOPT);
}

static void test_recv(void)
{
    struct io_uring_sqe sqe;
    struct vsr_io_sqe r;

    r = rec(VSR_IO_SQE_RECV);
    r.fd = 8;
    r.addr = outside;
    r.length = sizeof(outside);
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_RECV && sqe.addr == ptr(outside) &&
          sqe.len == sizeof(outside) && sqe.msg_flags == 0 && sqe.ioprio == 0 &&
          sqe.flags == 0);
    r.op_flags = VSR_IO_RECV_PEEK;
    CHECK(tr(r, &sqe) == 0 && sqe.msg_flags == MSG_PEEK);
    /* LINK: a short receive must fail the chain. */
    r.flags = VSR_IO_SQE_LINK;
    CHECK(tr(r, &sqe) == 0 && sqe.msg_flags == (MSG_PEEK | MSG_WAITALL) &&
          sqe.flags == IOSQE_IO_LINK);
    r.op_flags = 8;
    CHECK(rejected(r) == -EINVAL);

    /* FIXED_BUFFER: checked, then a plain receive (the kernel's RECV has
     * no fixed form). */
    r = rec(VSR_IO_SQE_RECV);
    r.flags = VSR_IO_SQE_FIXED_BUFFER;
    r.fd = 8;
    r.addr = region_memory;
    r.length = 64;
    CHECK(tr(r, &sqe) == 0 && sqe.opcode == IORING_OP_RECV && sqe.ioprio == 0 &&
          sqe.addr == ptr(region_memory));
    r.addr = outside;
    CHECK(rejected(r) == -EFAULT);

    /* Provided buffers, single and multishot. */
    r = rec(VSR_IO_SQE_RECV);
    r.flags = VSR_IO_SQE_BUFFER_SELECT;
    r.fd = 8;
    r.buffer_group = 7;
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_RECV && sqe.addr == 0 &&
          sqe.flags == IOSQE_BUFFER_SELECT && sqe.buf_group == 7 &&
          sqe.ioprio == 0);
    r.op_flags = VSR_IO_RECV_MULTISHOT | VSR_IO_RECV_PEEK;
    CHECK(tr(r, &sqe) == 0);
    CHECK((sqe.ioprio & IORING_RECV_MULTISHOT) && sqe.buf_group == 7 &&
          sqe.flags == IOSQE_BUFFER_SELECT && sqe.msg_flags == MSG_PEEK);
    r.flags |= VSR_IO_SQE_FIXED_FILE | VSR_IO_SQE_LINK;
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.flags ==
          (IOSQE_BUFFER_SELECT | IOSQE_FIXED_FILE | IOSQE_IO_LINK));
    CHECK(sqe.msg_flags == MSG_PEEK); /* No WAITALL on multishot. */
    r.flags = VSR_IO_SQE_BUFFER_SELECT | VSR_IO_SQE_SKIP_SUCCESS;
    CHECK(rejected(r) == -EINVAL);
    r.flags = 0; /* Multishot needs provided buffers. */
    CHECK(rejected(r) == -EINVAL);
    r.flags = VSR_IO_SQE_BUFFER_SELECT | VSR_IO_SQE_FIXED_BUFFER;
    CHECK(rejected(r) == -EINVAL);
}

static void test_send(void)
{
    struct io_uring_sqe sqe;
    struct vsr_io_sqe r;
    struct vsr_io_vec vecs[2];

    r = rec(VSR_IO_SQE_SEND);
    r.fd = 8;
    r.addr = outside;
    r.length = 10;
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_SEND && sqe.addr == ptr(outside) &&
          sqe.len == 10 && sqe.msg_flags == MSG_NOSIGNAL && sqe.ioprio == 0);
    r.flags = VSR_IO_SQE_LINK;
    CHECK(tr(r, &sqe) == 0 && sqe.msg_flags == (MSG_NOSIGNAL | MSG_WAITALL));
    r.flags = 0;
    r.op_flags = 4;
    CHECK(rejected(r) == -EINVAL);

    vecs[0].base = region_memory;
    vecs[0].length = 4;
    vecs[1].base = region_memory + 10;
    vecs[1].length = 4;
    r = rec(VSR_IO_SQE_SEND);
    r.fd = 8;
    r.addr = vecs;
    r.length = 2;
    r.op_flags = VSR_IO_SEND_VECTORED;
    CHECK(tr(r, &sqe) == 0 && sqe.opcode == IORING_OP_SEND &&
          sqe.ioprio == IORING_SEND_VECTORIZED && sqe.addr == ptr(vecs) &&
          sqe.len == 2);
    /* Plain send, FIXED_BUFFER: checked, then sent as plain memory. */
    r.flags = VSR_IO_SQE_FIXED_BUFFER;
    CHECK(tr(r, &sqe) == 0 && sqe.opcode == IORING_OP_SEND &&
          sqe.ioprio == IORING_SEND_VECTORIZED && sqe.buf_index == 0);
    vecs[1].base = outside;
    CHECK(rejected(r) == -EFAULT);
    r.op_flags = 0;
    r.addr = outside;
    r.length = 4;
    CHECK(rejected(r) == -EFAULT);
    r.op_flags = VSR_IO_SEND_VECTORED;
    r.length = 1025;
    CHECK(rejected(r) == -EINVAL);

    /* Zero copy: the region check is the kernel's. */
    vecs[1].base = region_memory + 10;
    r = rec(VSR_IO_SQE_SEND);
    r.fd = 8;
    r.addr = region_memory;
    r.length = 16;
    r.op_flags = VSR_IO_SEND_ZERO_COPY;
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_SEND_ZC && sqe.ioprio == 0 &&
          sqe.msg_flags == MSG_NOSIGNAL && sqe.addr == ptr(region_memory) &&
          sqe.len == 16);
    r.flags = VSR_IO_SQE_FIXED_BUFFER | VSR_IO_SQE_FIXED_FILE;
    r.buffer_index = 1;
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_SEND_ZC &&
          sqe.ioprio == IORING_RECVSEND_FIXED_BUF && sqe.buf_index == 1 &&
          sqe.flags == IOSQE_FIXED_FILE);
    r.addr = outside; /* Left to the kernel: -EFAULT, then NOTIF. */
    CHECK(tr(r, &sqe) == 0 && sqe.addr == ptr(outside));
    r.addr = vecs;
    r.length = 2;
    r.buffer_index = 0;
    r.op_flags = VSR_IO_SEND_ZERO_COPY | VSR_IO_SEND_VECTORED;
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_SEND_ZC &&
          sqe.ioprio == (IORING_RECVSEND_FIXED_BUF | IORING_SEND_VECTORIZED) &&
          sqe.addr == ptr(vecs) && sqe.len == 2 && sqe.buf_index == 0);
    r.flags |= VSR_IO_SQE_SKIP_SUCCESS;
    CHECK(rejected(r) == -EINVAL);
}

static void test_timeouts(void)
{
    struct io_uring_sqe sqe;
    struct vsr_io_sqe r;
    uint64_t target = 77;

    r = rec(VSR_IO_SQE_TIMEOUT);
    r.offset = UINT64_C(2500000000);
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_TIMEOUT && sqe.addr == ptr(&timespecs[0]) &&
          sqe.len == 1 && sqe.off == 0 && sqe.timeout_flags == 0);
    CHECK(timespecs[0].tv_sec == 2 && timespecs[0].tv_nsec == 500000000);
    r.op_flags = VSR_IO_TIMEOUT_ABSOLUTE;
    r.offset = UINT64_C(7000000001);
    CHECK(tr(r, &sqe) == 0 && sqe.timeout_flags == IORING_TIMEOUT_ABS);
    CHECK(timespecs[0].tv_sec == 7 && timespecs[0].tv_nsec == 1);
    r.op_flags = 2;
    CHECK(rejected(r) == -EINVAL);

    r = rec(VSR_IO_SQE_TIMEOUT_UPDATE);
    r.addr2 = &target;
    r.offset = 5;
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_TIMEOUT_REMOVE && sqe.addr == 77 &&
          sqe.addr2 == ptr(&timespecs[0]) &&
          sqe.timeout_flags == IORING_TIMEOUT_UPDATE);
    CHECK(timespecs[0].tv_sec == 0 && timespecs[0].tv_nsec == 5);
    r.op_flags = VSR_IO_TIMEOUT_ABSOLUTE;
    CHECK(tr(r, &sqe) == 0 &&
          sqe.timeout_flags == (IORING_TIMEOUT_UPDATE | IORING_TIMEOUT_ABS));
    r.addr2 = NULL;
    CHECK(rejected(r) == -EINVAL);
    /* Without timespec storage nothing can be armed. */
    uring.timespecs = NULL;
    r = rec(VSR_IO_SQE_TIMEOUT);
    CHECK(rejected(r) == -EINVAL);
    reset();
}

static void test_cancel(void)
{
    struct io_uring_sqe sqe;
    struct vsr_io_sqe r;

    r = rec(VSR_IO_SQE_CANCEL);
    r.offset = 1234;
    CHECK(tr(r, &sqe) == 0);
    CHECK(sqe.opcode == IORING_OP_ASYNC_CANCEL && sqe.addr == 1234 &&
          sqe.cancel_flags == 0);
    r.op_flags = VSR_IO_CANCEL_ALL;
    CHECK(tr(r, &sqe) == 0 && sqe.cancel_flags == IORING_ASYNC_CANCEL_ALL);
    r.op_flags = VSR_IO_CANCEL_BY_FD;
    r.fd = 12;
    CHECK(tr(r, &sqe) == 0 && sqe.fd == 12 &&
          sqe.cancel_flags == IORING_ASYNC_CANCEL_FD);
    r.op_flags = VSR_IO_CANCEL_BY_FD | VSR_IO_CANCEL_ALL;
    r.flags = VSR_IO_SQE_FIXED_FILE;
    CHECK(tr(r, &sqe) == 0 && sqe.flags == 0 &&
          sqe.cancel_flags ==
              (IORING_ASYNC_CANCEL_FD | IORING_ASYNC_CANCEL_FD_FIXED |
               IORING_ASYNC_CANCEL_ALL));
    r.op_flags = 4;
    CHECK(rejected(r) == -EINVAL);
}

/* ------------------------------------------------------------------------
 * Every opcode x every flag combination
 * --------------------------------------------------------------------- */

static struct vsr_io_vec sweep_vecs[1];
static uint64_t sweep_target = 1;

/* A record of `opcode` whose fields are valid under every flag, so that
 * the flags alone decide acceptance. */
static struct vsr_io_sqe sweep_record(uint8_t opcode)
{
    struct vsr_io_sqe r = rec(opcode);

    sweep_vecs[0].base = region_memory;
    sweep_vecs[0].length = 8;
    r.fd = 3;
    r.fd2 = VSR_IO_SLOT_ALLOC;
    switch ((enum vsr_io_sqe_opcode)opcode) {
    case VSR_IO_SQE_READV:
    case VSR_IO_SQE_WRITEV:
        r.addr = sweep_vecs;
        r.length = 1;
        break;
    case VSR_IO_SQE_TIMEOUT_UPDATE:
        r.addr2 = &sweep_target;
        break;
    case VSR_IO_SQE_SOCKET:
        r.length = AF_INET;
        r.op_flags = SOCK_STREAM;
        break;
    case VSR_IO_SQE_NOP:
    case VSR_IO_SQE_READ:
    case VSR_IO_SQE_WRITE:
    case VSR_IO_SQE_FSYNC:
    case VSR_IO_SQE_FALLOCATE:
    case VSR_IO_SQE_OPENAT:
    case VSR_IO_SQE_CLOSE:
    case VSR_IO_SQE_RENAMEAT:
    case VSR_IO_SQE_UNLINKAT:
    case VSR_IO_SQE_MKDIRAT:
    case VSR_IO_SQE_STATX:
    case VSR_IO_SQE_CONNECT:
    case VSR_IO_SQE_BIND:
    case VSR_IO_SQE_LISTEN:
    case VSR_IO_SQE_ACCEPT:
    case VSR_IO_SQE_RECV:
    case VSR_IO_SQE_SEND:
    case VSR_IO_SQE_SHUTDOWN:
    case VSR_IO_SQE_SETSOCKOPT:
    case VSR_IO_SQE_GETSOCKOPT:
    case VSR_IO_SQE_TIMEOUT:
    case VSR_IO_SQE_CANCEL:
    default:
        r.addr = region_memory;
        r.length = 8;
        break;
    }
    return r;
}

static bool one_of(uint8_t opcode, const uint8_t *set, size_t count)
{
    for (size_t i = 0; i < count; ++i) {
        if (set[i] == opcode) {
            return true;
        }
    }
    return false;
}

static void test_sweep(void)
{
    static const uint8_t fixed_buffer[] = {VSR_IO_SQE_READ,  VSR_IO_SQE_WRITE,
                                           VSR_IO_SQE_READV, VSR_IO_SQE_WRITEV,
                                           VSR_IO_SQE_RECV,  VSR_IO_SQE_SEND};
    static const uint8_t direct[] = {VSR_IO_SQE_OPENAT, VSR_IO_SQE_SOCKET,
                                     VSR_IO_SQE_ACCEPT};
    uint32_t accepted = 0;

    for (uint8_t opcode = 0; opcode <= VSR_IO_SQE_CANCEL + 1; ++opcode) {
        for (uint32_t flags = 0; flags < 128; ++flags) {
            struct vsr_io_sqe r = sweep_record(opcode);
            struct io_uring_sqe sqe;
            bool fixed = (flags & VSR_IO_SQE_FIXED_BUFFER) != 0;
            bool select = (flags & VSR_IO_SQE_BUFFER_SELECT) != 0;
            bool invalid = opcode > VSR_IO_SQE_CANCEL || flags >= 64 ||
                           (select && opcode != VSR_IO_SQE_RECV) ||
                           (fixed && !one_of(opcode, fixed_buffer,
                                             sizeof(fixed_buffer))) ||
                           (fixed && select) ||
                           ((flags & VSR_IO_SQE_DIRECT) &&
                            !one_of(opcode, direct, sizeof(direct)));
            uint8_t expected = 0;
            int rc;

            reset();
            r.flags = (uint8_t)flags;
            rc = tr(r, &sqe);
            if (invalid) {
                CHECK(rc == -EINVAL && zeroed(&sqe));
                continue;
            }
            CHECK(rc == 0);
            accepted++;
            CHECK(sqe.user_data == r.user_data);
            if (flags & VSR_IO_SQE_LINK) {
                expected |= IOSQE_IO_LINK;
            }
            if (flags & VSR_IO_SQE_SKIP_SUCCESS) {
                expected |= IOSQE_CQE_SKIP_SUCCESS;
            }
            if (select) {
                expected |= IOSQE_BUFFER_SELECT;
            }
            if ((flags & VSR_IO_SQE_FIXED_FILE) && opcode != VSR_IO_SQE_CLOSE &&
                opcode != VSR_IO_SQE_CANCEL) {
                expected |= IOSQE_FIXED_FILE;
            }
            CHECK(sqe.flags == expected);
            if (flags & VSR_IO_SQE_DIRECT) {
                CHECK(sqe.file_index == IORING_FILE_INDEX_ALLOC);
            }
        }
    }
    CHECK(accepted > 26 * 8);
}

/* ------------------------------------------------------------------------
 * CQE -> record
 * --------------------------------------------------------------------- */

static void test_reap_one(void)
{
    struct io_uring_cqe cqe;
    struct vsr_io_cqe record;

    memset(&cqe, 0, sizeof(cqe));
    cqe.user_data = UINT64_C(0xff00000000000001);
    cqe.res = -ENOBUFS;
    memset(&record, 0xa5, sizeof(record));
    vsr_io_uring_reap_one(&cqe, &record);
    CHECK(record.user_data == cqe.user_data && record.result == -ENOBUFS &&
          record.flags == 0 && record.buffer_id == 0);

    cqe.res = 1500;
    cqe.flags = IORING_CQE_F_MORE | IORING_CQE_F_BUFFER |
                IORING_CQE_F_BUF_MORE | IORING_CQE_F_SOCK_NONEMPTY |
                (UINT32_C(0xbeef) << IORING_CQE_BUFFER_SHIFT);
    vsr_io_uring_reap_one(&cqe, &record);
    CHECK(record.result == 1500 && record.buffer_id == 0xbeef);
    CHECK(record.flags ==
          (VSR_IO_CQE_MORE | VSR_IO_CQE_BUFFER | VSR_IO_CQE_BUFFER_MORE));

    /* Upper bits without F_BUFFER are not a buffer id. */
    cqe.flags = IORING_CQE_F_MORE | (UINT32_C(7) << IORING_CQE_BUFFER_SHIFT);
    vsr_io_uring_reap_one(&cqe, &record);
    CHECK(record.flags == VSR_IO_CQE_MORE && record.buffer_id == 0);

    cqe.res = 0;
    cqe.flags = IORING_CQE_F_NOTIF;
    vsr_io_uring_reap_one(&cqe, &record);
    CHECK(record.result == 0 && record.flags == VSR_IO_CQE_NOTIF);
}

int main(void)
{
    reset();
    test_nop();
    test_read_write();
    test_files();
    test_sockets();
    test_recv();
    test_send();
    test_timeouts();
    test_cancel();
    test_sweep();
    test_reap_one();
    return 0;
}
