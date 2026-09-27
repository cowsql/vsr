# Benchmarks

`make benchmark` builds and runs `core.c`, outside the correctness `TESTS` list.
Use a separate optimized build without sanitizers or coverage. The optional
benchmark requires a POSIX monotonic clock and GNU/LLVM linker `--wrap` support;
these are not requirements of the C11 library itself.

The CSV reports configurations, per-replica arena bytes, step call count, summed
core nanoseconds per request across all replicas, median and 99th percentile
step durations, and total host wall time. It compares one and three replicas,
both durability policies, batch sizes 1 and 8, single/scattered payloads, and
32-byte/4096-byte commands. Each sample executes 64 fresh client requests.
Record compiler flags and hardware alongside the CSV when comparing runs.

Linker wrapping times only `vsr_step`; graph cloning, storage copies, history
oracles, and message scheduling remain outside the measured core interval.
Timing includes clock-read overhead and sanitizer overhead if enabled. The
host's quadratic reference model is deliberately not a throughput benchmark
for a production adapter. There are no timing pass/fail thresholds.

For cache measurements, run the optimized executable under your platform's
hardware-counter profiler. The core's object files can also be inspected for
undefined symbols to verify that no allocation, clock, networking, or storage
functions are called by the library. Zero-copy input/output lifetimes are
checked separately by the unit and integration suites.
