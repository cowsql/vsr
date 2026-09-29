# Handover: VSR io_uring I/O layer, session 3 (2026-09-29)

Use this as the opening brief of the next session, after the working
agreement and reading list of the original brief (docs/io-design.md
decisions 1-99, docs/io-implementation.md, the three public headers,
docs/development.md, tests/README.md). The per-module handoffs in this
directory hold the detail; this file is the index and the plan.

## Where the code is

- `main` (= `claude/vsr-io-uring-session-2-5sj9es`): everything verified.
  Session 2's work plus this session's store phases 1-3, the complete
  link module and its review, the stream module with the backpressure
  fix, the fault-wrapper review, a lint-clean tree and the chores.
- Three unverified WIP branches from agents paused at wrap-up (each ends
  in a commit titled "WIP: ..."; none was built in that state):
  - `wip/store-phase-4`: store phase 4 in progress: three store fixes
    being made in src/io/store.c after a read of durable_now,
    base_apply, base_set, apply_restore, pin_floor/written_floor, and a
    draft tests/fuzzy/recovery.c (libFuzzer with an independent
    checker). Brief: "Next steps" 1 below.
  - `wip/stream-review`: the independent review of src/io/stream.c,
    uncommitted fixes/tests in stream.c, stream.h, tests/unit/stream.c
    and docs, on top of the merged backpressure fix. Placeholder
    decision labels `B2`, `B3`... may appear (renumber from 100).
  - `wip/snapshot`: src/io/snapshot.c implemented (commit "Implement the
    snapshot module") and tests/unit/snapshot.c with its first test
    passing; the other tests are unwritten. Placeholder labels `P1`...
  Each branch forked from an older integration commit: merge `main` into
  it first (conflicts, if any, are in the decision tables of
  docs/io-design.md section 10 and docs/io-implementation.md section 10;
  keep both sides in numeric order).

## State of main (verified in the container: kernel 6.18, clang 18, gcc 13)

- `make check`: 66/66 under clang (ASan+UBSan, --enable-werror) and gcc
  (--enable-werror --disable-sanitize). The final head (backpressure fix,
  decision 99) passed 66/66 under both compilers and format-check at
  wrap-up.
- `make lint` exits 0 (format-check, shellcheck, clang-tidy 18,
  cppcheck 2.13) as of f545fa4; the later commits touched only
  link.c/stream.* and docs and were lint-clean per their authors.
- Fuzzing (one worker, -max_len=8192): frame 48.2M execs, 1028 edges;
  new entry target (vsr_io_codec_get_entry_at) 94.7M execs, 340 edges;
  no crashes.
- Unit tests now include store, link, stream (plus session 2's).

## Environment (local laptop)

    sudo apt-get install autoconf automake make clang clang-tools \
      clang-format clang-tidy libclang-rt-18-dev cppcheck shellcheck bear
    ./bootstrap && mkdir -p build/asan && cd build/asan && \
      ../../configure --enable-werror && make -j4 check
    mkdir -p build/gcc && cd build/gcc && \
      CC=gcc ../../configure --enable-werror --disable-sanitize && make -j4 check
    # lint: make compile-commands && make lint     (~8 min now)
    # fuzz: ../../configure --enable-fuzzing && make fuzz

The container lacked the clang sanitizer runtime (libclang-rt-18-dev),
cppcheck, shellcheck and bear; configure fails without the first.

## Done this session

- Fault wrapper review (docs/handover/faulty-executor.md): 4 real bugs
  fixed (buffer-sharing completion order, O_DIRECT short-count
  alignment, early waits at rate 0, refused-batch accounting);
  conformance rows faulty-sim/faulty-uring and scenario faulty_rates.
- Lint pass: whole tree clean; one real bug (get_clients_record
  dereferenced a NULL cursor before checking it); tidy now visits each
  file once and reports every failing file.
- Store phases 1-3 (store-phase-1/2/3.md): creation, tail ring,
  extents, write pipeline, SYNC/flush/idle superblock; indexes, hot and
  cold LOADs, RECLAIM, freeing, admission, base files, capture;
  recovery (floor F, torn tails, stale headers, re-read, replay through
  index_apply, FDATASYNC flush order).
- Link (link-phase-2.md, link-phase-3.md, link-review.md): nodes,
  authorizations, dial/backoff, accept, TRUSTED and EXTERNAL
  handshakes, carrier election, reassembly across slabs, delivery with
  the decision-67 check, send path (coalescing, short sends, zero-copy
  NOTIF, queues, retirement), stream links, TCP_NODELAY. Independent
  review: 5 bugs fixed (own node id accepted as a peer, retiring
  messages hung on a closing link, dropped retiring messages still
  read by zero-copy sends, teardown not cancelling CONNECT/EXTERNAL
  preamble, a leaked handshakes_due).
- Stream (stream.md): requester and source, BUFFERS/FILE writes,
  window, timeouts, snapshot hooks (weak stubs until snapshot.c);
  backpressure fix: a stream link pauses its receive while its frame
  waits for a window unit (decision 99).
- Chores: entry fuzzer, faster lint.

## Decisions logged this session (docs/io-design.md section 10)

69 ring rule (cache >= write_behind + pinned + 2 records + 2 headers +
block); 70 32-bit executor lengths, short write = write error; 71
RECOVER of a missing log creates it; 72 raw link sockets until takeover,
listener table; 73 authorization and dial rules; 74 link deadline
dispatch, module consumes its slots, io->now; 75 MESSAGE refusals drop
the frame; 76 reassembly and held runs; 77 index_apply from record
bytes; 78 freeing floor amendments and FREEING; 79 every OK LOAD carries
a lease, RETRY for stale CLIENT/REQUEST loads; 80 base files; 81
retained_begin; 82 send ring; 83 queue rules; 84 stream links; 85
TCP_NODELAY; 86 identity refusals (own node); 87 teardown cancels
CONNECT/preamble; 88 floor F sweep; 89 FDATASYNC recovery flush order;
90 anchor's clients file at recovery via the snapshot module; 91 load
region holds the recovered manifest; 92 decision 57's directory listing
deferred (no executor opcode); 93 stream handles and op ids; 94 no
RELEASE for stream leases; 95 source write queue and units; 96 close
order; 97 early ends; 98 snapshot hooks; 99 paused stream receive.

## Next steps, in order

1. Store phase 4 from `wip/store-phase-4` (base: store-phase-3.md):
   finish the fixes it started, the recovery fuzzer
   (tests/fuzzy/recovery.c, independent checker written from docs
   sections 5 and 6.4 without reading store.c's scanner, >= 30 min
   campaign), a seeded random walk against a reference model, the
   FREEING-at-completion hazard in docs/io-implementation.md section 11
   (false CORRUPT after a lost superblock in FDATASYNC mode; failing
   test first), whole-store mutation testing.
2. Finish the stream review from `wip/stream-review` (authors' focus:
   linger/close order and loss status, unit-ring bookkeeping, exactly-
   once completion on every path).
3. Finish the snapshot module from `wip/snapshot` (tests, mutation,
   handoff for the engine). It replaces stream.c's weak hooks and must
   set store->base_slot and load the anchor's clients file at recovery
   (decision 90); bound clients-file record lengths before
   get_clients_record.
4. Independent Fable reviews of store.c (recovery first) and snapshot.c.
5. Engine part 2 (engine.c): the call orders are in the "engine part 2"
   sections of link-phase-3.md, stream.md and store-phase-1.md/-3.md;
   op routing of implementation section 7 (LOADs before STOREs,
   decision 51).
6. tests/integration/{engine,streams,snapshots}, uring_faults over the
   wrapper (faulty-executor.md lists what it injects and its gaps),
   tests/fuzzy/iocluster(+extended), networked examples, benchmarks,
   docs, final report.

## Open items

- Accept flood past the 16-entry orphan table leaks descriptors (link
  review, OPEN: needs a listener pause).
- Decision 92: NEW/JOIN refusal of stray clients-* files and RECOVER's
  unlink need a directory listing the executor lacks.
- Fault wrapper: no short counts on vectored/zero-copy SENDs (the
  engine's send path is vectored), READV/WRITEV only -EIO.
- Still pending from session 2, for a machine with kernel 7.2:
  uring_smoke and the uring column of executor_conformance, the -ENOSYS
  path of vsr_io_uring_init on an older kernel, SQPOLL/NAPI beyond
  creation, clang 21 / gcc 15 `make check`, and a diff of
  src/io/uapi/io_uring.h against linux-libc-dev 7.2.6.

## How the delegation worked (keep doing this)

- One worktree per agent under build/wt/<name> on branch wt/<name>,
  created from the integration branch; agents never merge or push; the
  coordinator merges, renumbers placeholder decision labels (S1, L1,
  T1, R1, B1, P1...) to the next free numbers, resolves the add/add
  conflicts in the two decision tables, and pushes.
- Big modules split into phases, one fresh agent per phase, each
  writing a handoff file for the next and a final report of at most 15
  lines; this kept every context small. Reviews by a different agent
  than the author, breaking test first, mutation-tested.
- Checks run in a separate detached worktree (build/wt/verify) so merges
  never disturb a running build: bootstrap, then clang ASan `make check`,
  then gcc `make check`, then `make compile-commands && make lint`.
- Paused agents resume with their context through SendMessage; commit
  WIP before a container can be reclaimed.
