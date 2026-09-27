# Benchmarks

Reserved for repeatable core and host-adapter measurements. Keep benchmarks
outside `TESTS`: noisy timing thresholds do not belong in correctness checks.
Report configuration, seed, compiler flags, hardware, workload, and sample
distribution. Measure allocations/copies and cache behavior alongside latency
and throughput. Use a separate optimized build without sanitizers or coverage.
