/* vsr_io_uring_init's refusals of a kernel it cannot run on (decision 53
 * of docs/io-design.md: -ENOSYS for a kernel without io_uring or older
 * than the baseline), tested on a current kernel. Each scenario runs in a
 * forked child whose main thread carries a seccomp filter over
 * io_uring_setup and IORING_REGISTER_PROBE, the two calls that tell a
 * kernel's version:
 *
 * - A kernel without io_uring (CONFIG_IO_URING=n, or before 5.1): the
 *   filter fails io_uring_setup with ENOSYS (SECCOMP_RET_ERRNO), and with
 *   EPERM for io_uring disabled by sysctl; both pass through unchanged.
 * - An older kernel: the filter hands the calls to a supervisor thread of
 *   the same process (SECCOMP_RET_USER_NOTIF; the thread predates the
 *   filter, so its own calls reach the kernel), which answers them as that
 *   kernel would: it refuses setup flags the kernel does not know with
 *   EINVAL, creates a real ring and cuts its feature bits down to the
 *   kernel's, or writes the kernel's opcode table in place of the probe.
 *
 * The kernels emulated: Linux 6.1 (setup flags up to DEFER_TASKRUN, so
 * NO_SQARRAY is refused; features up to LINKED_FILE), 6.11 (every setup
 * flag; features up to RECVSEND_BUNDLE, no MIN_TIMEOUT), 6.14 (every
 * feature; opcodes up to LISTEN, no READV_FIXED or WRITEV_FIXED), and a
 * kernel built without networking (the full table with the socket
 * opcodes unsupported). Each must be refused with -ENOSYS, closing every
 * descriptor it opened; a real refusal of the options (an SQPOLL CPU that
 * does not exist) must stay -EINVAL. Exits 77 when the kernel has no
 * usable io_uring or seccomp user notification. */
#define _GNU_SOURCE
#include "config.h"

#include "io/uring.h" /* The UAPI: setup flags, features, the probe. */
#include "lib/check.h"
#include "vsr-io.h"

#include <errno.h>
#include <fcntl.h>
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

#define SKIP 77

/* Setup flags and feature bits by kernel version, from the UAPI header's
 * history: DEFER_TASKRUN (bit 13) is the newest flag 6.1 knows,
 * LINKED_FILE (bit 12) its newest feature, RECVSEND_BUNDLE (bit 14) the
 * newest feature of 6.11; LISTEN is the last opcode of 6.14. */
#define LINUX_6_1_SETUP_FLAGS ((IORING_SETUP_DEFER_TASKRUN << 1) - 1)
#define LINUX_6_1_FEATURES ((IORING_FEAT_LINKED_FILE << 1) - 1)
#define LINUX_6_11_FEATURES ((IORING_FEAT_RECVSEND_BUNDLE << 1) - 1)
#define LINUX_6_14_OP_LAST (IORING_OP_LISTEN + 1)

/* What the filter does with io_uring_setup and with a probe. */
enum action { PASS, FAIL_ENOSYS, FAIL_EPERM, NOTIFY };

/* The kernel the supervisor plays. */
struct kernel {
    uint32_t setup_flags; /* Setup flags it knows. */
    uint32_t features;    /* Feature bits it reports. */
    uint32_t op_last;     /* 0: the probe is not intercepted. */
    bool without_net;     /* Socket opcodes reported unsupported. */
};

static struct kernel kernel;
static int listener_pipe[2];
static atomic_uint notified; /* Calls the supervisor answered. */

static struct vsr_io_uring_options options(void)
{
    struct vsr_io_uring_options o;

    memset(&o, 0, sizeof(o));
    o.sq_entries = 8;
    o.cq_entries = 16;
    o.file_slots = 4;
    o.buffer_regions = 1;
    o.sqpoll_cpu = UINT32_MAX;
    return o;
}

/* Initializes an executor with `o`, returning init's result; the
 * executor is deinitialized again when init succeeded. */
static int try_init(const struct vsr_io_uring_options *o)
{
    struct vsr_io_need need;
    struct vsr_io_executor ex;
    void *memory;
    int rc;

    CHECK(vsr_io_uring_layout(o, &need) == VSR_OK);
    memory = aligned_alloc(need.alignment, (need.size + need.alignment - 1) &
                                               ~(need.alignment - 1));
    CHECK(memory != NULL);
    rc = vsr_io_uring_init(memory, need.size, o, &ex);
    if (rc == 0) {
        vsr_io_uring_deinit(&ex);
    }
    free(memory);
    return rc;
}

static int open_descriptors(void)
{
    int count = 0;

    for (int fd = 0; fd < 1024; ++fd) {
        count += fcntl(fd, F_GETFD) >= 0 ? 1 : 0;
    }
    return count;
}

static bool is_socket_opcode(uint32_t op)
{
    return op == IORING_OP_SOCKET || op == IORING_OP_CONNECT ||
           op == IORING_OP_BIND || op == IORING_OP_LISTEN ||
           op == IORING_OP_ACCEPT || op == IORING_OP_RECV ||
           op == IORING_OP_SEND || op == IORING_OP_SEND_ZC ||
           op == IORING_OP_SENDMSG_ZC || op == IORING_OP_SHUTDOWN;
}

/* io_uring_setup as the emulated kernel answers it. The arguments are
 * the main thread's, in this address space. */
static void answer_setup(const struct seccomp_notif *req,
                         struct seccomp_notif_resp *resp)
{
    /* NOLINTNEXTLINE(performance-no-int-to-ptr): a syscall's pointer */
    struct io_uring_params *params = (void *)(uintptr_t)req->data.args[1];
    struct io_uring_params copy = *params;
    long fd;

    if (copy.flags & ~kernel.setup_flags) {
        resp->error = -EINVAL;
        return;
    }
    fd = syscall(SYS_io_uring_setup, (unsigned)req->data.args[0], &copy);
    if (fd < 0) {
        resp->error = -errno;
        return;
    }
    copy.features &= kernel.features;
    *params = copy;
    resp->val = fd;
}

/* IORING_REGISTER_PROBE as the emulated kernel answers it: its opcode
 * count, every opcode supported unless it needs networking. */
static void answer_probe(const struct seccomp_notif *req,
                         struct seccomp_notif_resp *resp)
{
    /* NOLINTNEXTLINE(performance-no-int-to-ptr): a syscall's pointer */
    unsigned char *memory = (void *)(uintptr_t)req->data.args[2];
    uint32_t count = (uint32_t)req->data.args[3];
    struct io_uring_probe probe;
    struct io_uring_probe_op op;

    if (count > kernel.op_last) {
        count = kernel.op_last;
    }
    memset(&probe, 0, sizeof(probe));
    probe.last_op = (uint8_t)(kernel.op_last - 1);
    probe.ops_len = (uint8_t)count;
    memcpy(memory, &probe, sizeof(probe));
    for (uint32_t i = 0; i < count; ++i) {
        memset(&op, 0, sizeof(op));
        op.op = (uint8_t)i;
        if (!(kernel.without_net && is_socket_opcode(i))) {
            op.flags = IO_URING_OP_SUPPORTED;
        }
        memcpy(memory + offsetof(struct io_uring_probe, ops) + i * sizeof(op),
               &op, sizeof(op));
    }
    resp->val = 0;
}

static void *supervise(void *arg)
{
    int listener;

    (void)arg;
    CHECK(read(listener_pipe[0], &listener, sizeof(listener)) ==
          (ssize_t)sizeof(listener));
    if (listener < 0) {
        return NULL; /* The filter answers alone. */
    }
    for (;;) {
        struct seccomp_notif req;
        struct seccomp_notif_resp resp;

        memset(&req, 0, sizeof(req));
        if (ioctl(listener, SECCOMP_IOCTL_NOTIF_RECV, &req) != 0) {
            CHECK(errno == EINTR || errno == ENOENT);
            continue;
        }
        memset(&resp, 0, sizeof(resp));
        resp.id = req.id;
        if (req.data.nr == SYS_io_uring_setup) {
            answer_setup(&req, &resp);
        } else {
            answer_probe(&req, &resp);
        }
        atomic_fetch_add(&notified, 1u);
        CHECK(ioctl(listener, SECCOMP_IOCTL_NOTIF_SEND, &resp) == 0);
    }
    return NULL;
}

static uint32_t action_of(enum action action)
{
    switch (action) {
    case FAIL_ENOSYS:
        return SECCOMP_RET_ERRNO | (uint32_t)ENOSYS;
    case FAIL_EPERM:
        return SECCOMP_RET_ERRNO | (uint32_t)EPERM;
    case NOTIFY:
        return SECCOMP_RET_USER_NOTIF;
    case PASS:
        break;
    }
    return SECCOMP_RET_ALLOW;
}

/* Filters this thread's io_uring_setup and its IORING_REGISTER_PROBE
 * (through the registered ring or not); returns the listener descriptor
 * with NOTIFY, else -1. Exits SKIP where seccomp is unavailable. */
static int install_filter(enum action setup, enum action probe)
{
    /* The opcode's low 32 bits; the register opcode is 32-bit. */
    const uint32_t opcode_at =
        (uint32_t)offsetof(struct seccomp_data, args[1]) +
        (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__ ? 4u : 0u);
    struct sock_filter code[] = {
        BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, nr)),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, SYS_io_uring_setup, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, action_of(setup)),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, SYS_io_uring_register, 1, 0),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
        BPF_STMT(BPF_LD | BPF_W | BPF_ABS, opcode_at),
        BPF_STMT(BPF_ALU | BPF_AND | BPF_K,
                 ~(uint32_t)IORING_REGISTER_USE_REGISTERED_RING),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, IORING_REGISTER_PROBE, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, action_of(probe)),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
    };
    struct sock_fprog program = {
        .len = (unsigned short)(sizeof(code) / sizeof(code[0])),
        .filter = code,
    };
    bool notify = setup == NOTIFY || probe == NOTIFY;
    long rc;

    CHECK(prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) == 0);
    rc = syscall(SYS_seccomp, SECCOMP_SET_MODE_FILTER,
                 notify ? SECCOMP_FILTER_FLAG_NEW_LISTENER : 0, &program);
    if (rc < 0) {
        printf("  seccomp: %s\n", strerror(errno));
        _exit(SKIP);
    }
    return notify ? (int)rc : -1;
}

/* Runs init under the filter and the emulated kernel; returns its result
 * after checking that the emulation was consulted and that the refusal
 * left no descriptor open. */
static int refused_init(enum action setup, enum action probe)
{
    struct vsr_io_uring_options o = options();
    pthread_t supervisor;
    int before;
    int rc;

    CHECK(pipe(listener_pipe) == 0);
    CHECK(pthread_create(&supervisor, NULL, supervise, NULL) == 0);
    CHECK(pthread_detach(supervisor) == 0);
    {
        int listener = install_filter(setup, probe);

        CHECK(write(listener_pipe[1], &listener, sizeof(listener)) ==
              (ssize_t)sizeof(listener));
    }
    before = open_descriptors();
    rc = try_init(&o);
    CHECK(open_descriptors() == before);
    if (setup == NOTIFY || probe == NOTIFY) {
        CHECK(atomic_load(&notified) > 0);
    }
    return rc;
}

/* ------------------------------------------------------------------------
 * Scenarios
 * --------------------------------------------------------------------- */

static void scenario_no_io_uring(void)
{
    CHECK(refused_init(FAIL_ENOSYS, PASS) == -ENOSYS);
}

static void scenario_io_uring_disabled(void)
{
    CHECK(refused_init(FAIL_EPERM, PASS) == -EPERM);
}

/* Refuses NO_SQARRAY at setup with EINVAL, the answer of every kernel
 * before 6.6; the executor must still tell an old kernel from bad
 * options. */
static void scenario_linux_6_1(void)
{
    kernel.setup_flags = LINUX_6_1_SETUP_FLAGS;
    kernel.features = LINUX_6_1_FEATURES;
    CHECK(refused_init(NOTIFY, PASS) == -ENOSYS);
}

/* Every setup flag, no MIN_TIMEOUT: the feature check refuses it. */
static void scenario_linux_6_11(void)
{
    kernel.setup_flags = UINT32_MAX;
    kernel.features = LINUX_6_11_FEATURES;
    CHECK(refused_init(NOTIFY, PASS) == -ENOSYS);
}

/* Every feature, no READV_FIXED: the probe refuses it. */
static void scenario_linux_6_14(void)
{
    kernel.op_last = LINUX_6_14_OP_LAST;
    CHECK(refused_init(PASS, NOTIFY) == -ENOSYS);
}

/* The current opcode table with the socket opcodes unsupported. */
static void scenario_without_net(void)
{
    kernel.op_last = IORING_OP_LAST;
    kernel.without_net = true;
    CHECK(refused_init(PASS, NOTIFY) == -ENOSYS);
}

/* The same probe answer with networking: the emulation itself is sound,
 * init succeeds and closes what it opened. */
static void scenario_current_probe(void)
{
    kernel.op_last = IORING_OP_LAST;
    CHECK(refused_init(PASS, NOTIFY) == 0);
}

/* The real kernel refusing the options: an SQPOLL thread on a CPU that
 * does not exist is -EINVAL, not an old kernel. */
static void scenario_bad_sqpoll_cpu(void)
{
    struct vsr_io_uring_options o = options();
    int before = open_descriptors();

    o.sqpoll_idle_ms = 10;
    o.sqpoll_cpu = 1u << 20;
    CHECK(try_init(&o) == -EINVAL);
    CHECK(open_descriptors() == before);
}

/* ------------------------------------------------------------------------
 * Driver
 * --------------------------------------------------------------------- */

struct scenario {
    const char *name;
    void (*run)(void);
};

static const struct scenario scenarios[] = {
    {"no_io_uring", scenario_no_io_uring},
    {"io_uring_disabled", scenario_io_uring_disabled},
    {"linux_6_1", scenario_linux_6_1},
    {"linux_6_11", scenario_linux_6_11},
    {"linux_6_14", scenario_linux_6_14},
    {"without_net", scenario_without_net},
    {"current_probe", scenario_current_probe},
    {"bad_sqpoll_cpu", scenario_bad_sqpoll_cpu},
};

int main(void)
{
    struct vsr_io_uring_options o = options();
    bool failed = false;
    int rc;

    setvbuf(stdout, NULL, _IONBF, 0);
    rc = try_init(&o);
    if (rc == -ENOSYS || rc == -EPERM) {
        printf("skip: no usable io_uring (%s)\n", strerror(-rc));
        return SKIP;
    }
    CHECK(rc == 0);
    for (size_t i = 0; i < sizeof(scenarios) / sizeof(scenarios[0]); ++i) {
        int status;
        pid_t pid;

        fflush(stdout);
        pid = fork();
        CHECK(pid >= 0);
        if (pid == 0) {
            scenarios[i].run();
            /* _exit: the supervisor thread is still blocked. */
            _exit(0);
        }
        CHECK(waitpid(pid, &status, 0) == pid);
        if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
            printf("%s: PASS\n", scenarios[i].name);
        } else if (WIFEXITED(status) && WEXITSTATUS(status) == SKIP) {
            printf("%s: SKIP\n", scenarios[i].name);
        } else {
            printf("%s: FAIL\n", scenarios[i].name);
            failed = true;
        }
    }
    return failed ? 1 : 0;
}
