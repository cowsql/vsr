# Fault-injecting wrapper: what callers may assume (from the review, merged)

Fixed in review: completions sharing one provided buffer (INCREMENTAL rings) kept in order, chained completions too (ed3e4ae); short file counts are whole units of the request's alignment (941d49b); rate-zero waits no longer return early, want counts due held completions, min_wait honoured (d6de603); refused batches not counted, NULL records refused by the inner (8e15ae1). Conformance rows `faulty-sim`/`faulty-uring` (rate 0, nothing injected) and scenario `faulty_rates` (nonzero, sim and real ring, every fault kind required).

- -EIO on READ/WRITE/READV/WRITEV/FSYNC: the operation ran; a write's bytes may be in the file (recovery may find the record intact); nothing a failed FSYNC covered is durable. No write fails without landing; torn/absent data comes only from a crash.
- Short file counts: multiple of the request's alignment (block_bytes for the store's writes); bytes past the count were written anyway. The store treats a short write as a write error (decision 70), which is compatible.
- Streams: assumes stream sockets; never combine SKIP_SUCCESS with SEND/RECV (a short count would go unseen).
- Injected CANCEL leaves queued bytes queued; the caller's own CANCEL may return -ENOENT while the receive reports -ECANCELED.
- Waits may return early while a delayed completion is held (as after a wake): keep reaping. `want` above the CQ size may be accepted while completions are held.
- A chained record outliving the 4096-entry map may be delayed. Reserved user_data UINT64_MAX - 1 (FAULTY_EXECUTOR_USER_DATA) is refused with -EINVAL.
- Coverage gaps for uring_faults: no short counts on vectored or zero-copy SENDs (the engine's send path is vectored, so "short sends" will not happen there; trimming the vector count is a small follow-up), none on READV/WRITEV or multishot BUFFER_SELECT receives (READV/WRITEV get only -EIO); batches over 256 records get no submission-side faults.
- Pre-existing lint on base: 9 clang-tidy errors in executor_conformance.c, two cppcheck notes (trace_entry::reserved unused, a variable scope) — the lint agent handles them.
