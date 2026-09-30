# Handover: the io_uring executor on its target kernel (2026-09-30)

The REAL-RING items left open by sessions 2 and 3 (README "Open items":
uring_smoke and the conformance uring column on kernel 7.2, the `-ENOSYS`
path of `vsr_io_uring_init`, SQPOLL/NAPI beyond creation, clang 21 / gcc 15
`make check`, the UAPI header against linux-libc-dev 7.2.6), closed on
branch `wt/uring` from main `ed82e1a`.

Machine: Linux 7.2.6+deb14-amd64, 8 CPUs, build tree on btrfs; clang
21.1.8, gcc 16.2.0, gcc-15 15.3.0, linux-libc-dev 7.2.6-1, cppcheck 2.21,
clang-tidy 21; RLIMIT_MEMLOCK 8 MiB; `kernel.io_uring_disabled` 0.

## Commits

- `eeaf67f` Vendor Linux 7.2.6's io_uring UAPI headers
- `33e8f5e` Run the ring tests over SQPOLL and NAPI rings
- `8d8a677` Refuse kernels before 6.6 with -ENOSYS, and test every refusal
- `f845d46` Note where Linux 7.2.6 and the simulation answer differently
- `6f30d79` Clear cppcheck 2.21's findings in the executor and its tests
- `9e9f93e` Mark the refusal test's syscall pointers for clang-tidy
- this file

Placeholder decision: U1 (docs/io-design.md section 10; the contract
change row in docs/io-implementation.md section 10 cites it).

## 1. The vendored UAPI header

Found: `src/io/uapi/io_uring.h` was liburing 2.15's variant of the kernel
header, not the kernel's (`__kernel_rwf_t rw_flags`, no `write_stream`, no
`IORING_NOP_*` file flags or `IORING_URING_CMD_MULTISHOT`, the old
`io_uring_napi` layout, zcrx definitions inline).

Done: it is now Linux 7.2.6's `headers_install` output, copied from
`/usr/include/linux/io_uring.h` (linux-libc-dev 7.2.6-1), with
`linux/io_uring/zcrx.h` vendored beside it as `src/io/uapi/io_uring/zcrx.h`;
the one local change is that include, spelled `"io_uring/zcrx.h"` so the
system's io_uring headers are never read (`clang -H` confirms). Provenance
(version, package, sha256 of both originals) is in a note at the top of
each file and in `src/io/uapi/README.md`, decision 52's row, design
section 2 and the implementation doc's file list. The kernel header now
needs `__DECLARE_FLEX_ARRAY`, which `<linux/stddef.h>` has only from 5.16:
`src/io/uring.h` defines the kernel's own version when it is missing
(uring.c compiled with -Wpedantic -Werror under clang 21, gcc 16 and gcc
15 against a pre-5.16 `linux/stddef.h` shadow).

How it was checked: a scratch program compiled once per header printed
the value of every `IORING_*`/`IOSQE_*`/`IOU_*`/`IO_URING_*` identifier
used by uring.c, uring.h, uring_translate.c (103 constants) and the size,
offset and size of every field of the 18 structures they touch; the two
outputs were identical except `io_uring_napi`. A second pass compared all
262 identifiers the two headers share: only `IORING_URING_CMD_MASK` (1 to
3) and `__ZCRX_CTRL_LAST` changed, both unused. `io_uring_napi` keeps its
16 bytes; the old `pad[3]`/`resv` became `opcode` (offset 5) and
`op_param` (8), and the zeroed struct meant, and means,
`IO_URING_NAPI_REGISTER_OP` with `IO_URING_NAPI_TRACKING_DYNAMIC`. init
now sets both by name; uring.c `_Static_assert`s the offsets and sizes of
every structure it hands the kernel; uring_smoke reads the registration
back from the kernel (re-registering returns the replaced settings: 20 us,
prefer 0, dynamic tracking), a check verified to fail on a wrong value.

## 2. SQPOLL and NAPI beyond creation

- `executor_conformance` has two more columns, `uring-sqpoll` (SQ thread
  idling after 10 ms, so both the awake thread and the NEED_WAKEUP path
  are taken) and `uring-napi` (20 us busy poll). Only a missing default
  ring makes the suite exit 77; a refused variant is reported.
- `uring_smoke` runs its whole suite over four rings: default, SQPOLL,
  NAPI, SQPOLL+NAPI (files, fixed buffers and files, sockets with connect,
  multishot accept, buffer-select and multishot receives, plain and
  zero-copy sends including the vectored fixed-buffer SENDMSG_ZC, links,
  cancel, timeouts, wakes from another thread, a full SQ, CQ overflow,
  deinit with a multishot accept, an accept and a timeout pending), plus a
  per-variant test (SQPOLL: submissions to an idle SQ thread; NAPI: the
  read-back above).
- Results: every column passes (31 passed, 5 environment skips each); the
  smoke passes over all four rings; stable over 6 conformance and 8 smoke
  repetitions and with two copies of each running at once. SQPOLL is not
  refused to the user here (an `iou-sqp-<pid>` thread serves the ring).
- No executor bug found. Probed without a finding: whether deinit under
  SQPOLL lets a receive complete into caller memory afterwards (the drain
  cancels what the SQ thread consumed; an SQE not yet consumed is left to
  the kernel's teardown). A scratch program submitted receives and called
  deinit at once, with the SQ thread asleep, and pinned to the submitting
  CPU or not: 0 of 200 single receives and 0 of 4800 in batches of 24
  completed into their buffers after deinit.
- Limit: loopback sockets carry no NAPI id, so the NAPI runs cover the
  registration and every path with busy polling on, not the polling of a
  device queue. A veth pair in an unprivileged user+net namespace works
  here (`unshare -rn`), but veth only uses NAPI with GRO requested
  through ethtool (not installed) or XDP.

## 3. The -ENOSYS path of vsr_io_uring_init

Bug found and fixed (breaking test first): a kernel before 6.6 refuses the
`NO_SQARRAY` setup flag with `-EINVAL`, so on e.g. Debian 12's 6.1 init
returned `-EINVAL`, not the documented `-ENOSYS`, and the ring tests
failed instead of skipping. init now classifies a setup `-EINVAL` by the
feature bits of a plain one-entry ring: every setup flag the executor uses
predates `IORING_FEAT_MIN_TIMEOUT` (6.12), so a missing required feature
means an old kernel (`-ENOSYS`) and a full set means options a current
kernel refuses (`-EINVAL`, e.g. an SQPOLL CPU that does not exist).
Decision U1; vsr-io.h now names init's refusal errno values.

How it is tested without an older kernel: `tests/integration/uring_refusals`.
Each scenario runs in a forked child whose main thread carries a seccomp
filter over `io_uring_setup` and `IORING_REGISTER_PROBE` (with or without
`IORING_REGISTER_USE_REGISTERED_RING`):

- no io_uring: `SECCOMP_RET_ERRNO(ENOSYS)` on setup, init is `-ENOSYS`;
  io_uring disabled: `EPERM`, init is `-EPERM`.
- older kernels: `SECCOMP_RET_USER_NOTIF`, answered by a supervisor thread
  created before the filter (so its own calls reach the kernel) exactly as
  that kernel would. Linux 6.1: setup flags above `DEFER_TASKRUN` refused
  with `EINVAL`, features up to `LINKED_FILE` on a real ring the thread
  creates (this scenario failed with `-EINVAL` before the fix); 6.11:
  every flag, no `MIN_TIMEOUT`; 6.14: the opcode table up to `LISTEN`, no
  `READV_FIXED`; a kernel without networking: the full table, socket
  opcodes unsupported. Each is `-ENOSYS`; the supervisor must have been
  consulted and no descriptor may stay open. Controls: the current table
  under the same emulation initializes, and a real SQPOLL CPU of 2^20
  stays `-EINVAL`.
- The test exits 77 without a ring or without seccomp user notification.

## 4. gcc-15

`CC=gcc-15 ../../configure --enable-werror --disable-sanitize` and
`make -j2 check`: 67/67 PASS (the 66 of main plus `uring_refusals`),
no warning under `-Werror`; main's head had also passed 66/66 before
these changes. The uring tests in it ran over the real ring: all four
smoke rings, conformance 31 passed and 5 skipped per ring column, every
refusal scenario.

## 5. Kernel behaviour the simulation does not model

From the "recorded, not checked" lines of the conformance and smoke logs
(identical over the default, SQPOLL and NAPI rings). None is a divergence
the engine depends on today, so the simulation is unchanged and
docs/io-implementation.md section 11 lists them with what code that
starts depending on them must handle:

- CANCEL of a queued disk write: the kernel cancels it (CANCEL 0, the
  1 MiB buffered write on btrfs `-ECANCELED`); the simulation never
  cancels disk work (`-EALREADY`, the write completes). The engine cancels
  only socket records, by user_data.
- CANCEL `BY_FD` without `ALL`: the kernel cancelled the later of two
  receives, the simulation the earlier; unspecified by the contract and
  unused by the engine.
- `update_buffer` of a region in use: 0 on the ring (the kernel keeps the
  old registration until the record completes), `-EBUSY` in the
  simulation (decision 65).
- btrfs and tmpfs serve misaligned `O_DIRECT` reads (buffered); the
  simulation refuses them like ext4/XFS. The `odirect_*` rows skip here.
- Short-send lengths (6144 of 8 MiB with a 4096-byte `SO_SNDBUF` on the
  ring, 262144 in the simulation); other recorded facts (zero-copy
  `FIXED_BUFFER` outside the region: `-EFAULT` with MORE then the NOTIF;
  accept after `-ENFILE`: nothing queued, the refused client sees EOF)
  match the contract and the simulation.

## 6. Checks at the end

- clang 21 ASan+UBSan `--enable-werror` `make -j2 check`: 67/67 PASS.
- gcc 16 `--enable-werror --disable-sanitize` `make -j2 check`: 67/67 PASS.
- gcc 15: 67/67 PASS (above).
- `make format-check` (clang-format 21) and shellcheck: clean.
- `make compile-commands && make lint` (561 s): clang-tidy 21 fails only
  on `src/io/store.c` (7, clang-analyzer-security.ArrayBound, the store
  agent's) and `tests/unit/pool.c` (1, fixed on main); `make cppcheck`
  (2.21) reports only `tests/unit/codec.c` and `tests/unit/engine_tables.c`
  (being fixed on main). Nothing in the files this branch touches: the
  cppcheck 2.21 findings in `src/io/uring.c` and
  `tests/integration/executor_conformance.c` are fixed (`6f30d79`), and
  clang-tidy's two `performance-no-int-to-ptr` hits in the new test are
  marked (`9e9f93e`).

## Open items

- The ring's `odirect_alignment`/`odirect_address` rows need a build tree
  on ext4 or XFS; `fallocate_enospc` needs a bounded disk and
  `connect_unreachable` partition control on the ring. A mount namespace
  (`unshare -rm`, allowed here) with a size-limited tmpfs would bound the
  disk for `fallocate_enospc` if the factory ran inside one.
- NAPI busy polling of a real device queue (needs a NIC peer, or a veth
  pair with GRO enabled through ethtool, in a user+net namespace).
- Hardening candidate, not reproduced: under SQPOLL, deinit's drain could
  first wait for the SQ thread to consume every published SQE, so the
  SYNC_CANCEL sees them; today an unconsumed SQE is cancelled by the
  kernel's asynchronous teardown instead.
- The README's "Still pending from session 2" bullet is closed by this
  file.
