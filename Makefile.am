ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = -I$(top_srcdir)/include -I$(top_srcdir)/src -I$(top_srcdir)/tests \
  $(URING_CFLAGS)
AM_CFLAGS = $(VSR_CFLAGS) $(VSR_INSTRUMENT_CFLAGS)
AM_LDFLAGS = $(VSR_LDFLAGS) $(VSR_INSTRUMENT_LDFLAGS)

# One static library holds the core, the I/O layer, the simulation and the
# client bookkeeping, so every program linking it also links liburing.
lib_LIBRARIES = src/libvsr.a
LIBVSR = src/libvsr.a $(URING_LIBS)
include_HEADERS = include/vsr.h include/vsr-io.h include/vsr-sim.h \
  include/vsr-client.h
pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = vsr.pc
# The replication core (src/*.c): no dependency beyond mem* functions.
src_libvsr_a_SOURCES = \
  src/checked.c src/checked.h src/objects.c src/objects.h \
  src/validate.c src/validate.h src/core.c src/owned.c src/internal.h \
  src/protocol.c src/protocol.h src/extension.c src/extension.h \
  src/transition.c src/transition.h src/checkpoint.c src/checkpoint.h \
  src/epochs.c src/epochs.h src/reads.c src/reads.h
# The I/O layer, in the module order of docs/io-implementation.md: sans-IO
# planners, the engine, and the io_uring executor under src/io; the simulated
# world under src/sim; the client bookkeeping under src/client.
src_libvsr_a_SOURCES += src/io/crc32c.c src/io/crc32c.h src/io/cursor.c \
  src/io/pool.c src/io/slots.c src/io/deadline.c src/client/client.c
# Private headers of the I/O layer: the interface specification every
# module implements (docs/io-implementation.md); compiled by the header
# check until their sources land.
src_libvsr_a_SOURCES += src/io/cursor.h src/io/wire.h src/io/codec.h \
  src/io/pool.h src/io/slots.h src/io/deadline.h src/io/link.h \
  src/io/stream.h src/io/store.h src/io/snapshot.h src/io/engine.h \
  src/io/uring.h src/sim/sim.h
noinst_HEADERS = tests/lib/check.h
noinst_PROGRAMS = examples/replication examples/failover examples/checkpoint \
  examples/epochs examples/reads examples/witness
EXAMPLE_HOST = tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h \
  examples/common.c examples/common.h examples/kv.c examples/kv.h
examples_replication_SOURCES = examples/replication.c $(EXAMPLE_HOST)
examples_replication_LDADD = $(LIBVSR)
examples_failover_SOURCES = examples/failover.c $(EXAMPLE_HOST)
examples_failover_LDADD = $(LIBVSR)
examples_checkpoint_SOURCES = examples/checkpoint.c $(EXAMPLE_HOST)
examples_checkpoint_LDADD = $(LIBVSR)
examples_epochs_SOURCES = examples/epochs.c $(EXAMPLE_HOST)
examples_epochs_LDADD = $(LIBVSR)
examples_reads_SOURCES = examples/reads.c $(EXAMPLE_HOST)
examples_reads_LDADD = $(LIBVSR)
examples_witness_SOURCES = examples/witness.c $(EXAMPLE_HOST)
examples_witness_LDADD = $(LIBVSR)

UNIT_TESTS = tests/unit/checked tests/unit/objects tests/unit/random tests/unit/memory \
  tests/unit/validate tests/unit/runtime tests/unit/reads tests/unit/memory_watch
UNIT_TESTS += tests/unit/descriptors tests/unit/layout
UNIT_TESTS += tests/unit/crc32c tests/unit/cursor tests/unit/pool \
  tests/unit/slots tests/unit/deadline tests/unit/client
INTEGRATION_TESTS = tests/integration/replication tests/integration/normal_contract \
  tests/integration/reads tests/integration/checkpoints tests/integration/epochs
INTEGRATION_TESTS += tests/integration/lifecycle tests/integration/transitions \
  tests/integration/audit tests/integration/transfer_audit \
  tests/integration/api_contract
INTEGRATION_TESTS += tests/integration/failures tests/integration/view_change_edges \
  tests/integration/offers tests/integration/capture tests/integration/clients
REGRESSION_TESTS = tests/regression/trim_resend \
  tests/regression/recovery_coverage tests/regression/transfer_anchor \
  tests/regression/witness_prefix tests/regression/boundary_from_view_change \
  tests/regression/review_epoch_recovery_vote tests/regression/learner_restart \
  tests/regression/learner_follows tests/regression/epoch_announcement_recovery \
  tests/regression/api_init_out tests/regression/pressure_reload \
  tests/regression/anchor_behind_applied tests/regression/reply_restore \
  tests/regression/next_epoch_before_steady \
  tests/regression/recovery_into_handoff tests/regression/view_change_minimum \
  tests/regression/lagging_reload
FUZZY_TESTS = tests/fuzzy/cluster tests/fuzzy/cluster_extended
# Reproductions of open safety bugs found in review go here; they turn into
# XPASS once the core is fixed and then move to the ordinary regressions.
XFAIL_TESTS =
check_PROGRAMS = $(UNIT_TESTS) $(INTEGRATION_TESTS) $(REGRESSION_TESTS) $(FUZZY_TESTS)
tests_unit_checked_SOURCES = tests/unit/checked.c
tests_unit_checked_LDADD = $(LIBVSR)
tests_unit_objects_SOURCES = tests/unit/objects.c
tests_unit_objects_LDADD = $(LIBVSR)
tests_unit_random_SOURCES = tests/unit/random.c tests/lib/random.c tests/lib/random.h
tests_unit_memory_SOURCES = tests/unit/memory.c tests/lib/memory.c tests/lib/memory.h
tests_unit_memory_watch_SOURCES = tests/unit/memory_watch.c tests/lib/memory.c tests/lib/memory.h
tests_unit_validate_SOURCES = tests/unit/validate.c
tests_unit_validate_LDADD = $(LIBVSR)
tests_unit_runtime_SOURCES = tests/unit/runtime.c
tests_unit_runtime_LDADD = $(LIBVSR)
tests_unit_reads_SOURCES = tests/unit/reads.c
tests_unit_reads_LDADD = $(LIBVSR)
tests_unit_descriptors_SOURCES = tests/unit/descriptors.c
tests_unit_descriptors_LDADD = $(LIBVSR)
tests_unit_layout_SOURCES = tests/unit/layout.c
tests_unit_layout_LDADD = $(LIBVSR)
tests_unit_crc32c_SOURCES = tests/unit/crc32c.c tests/lib/random.c tests/lib/random.h
tests_unit_crc32c_LDADD = $(LIBVSR)
tests_unit_cursor_SOURCES = tests/unit/cursor.c
tests_unit_cursor_LDADD = $(LIBVSR)
tests_unit_pool_SOURCES = tests/unit/pool.c tests/lib/random.c tests/lib/random.h
tests_unit_pool_LDADD = $(LIBVSR)
tests_unit_slots_SOURCES = tests/unit/slots.c
tests_unit_slots_LDADD = $(LIBVSR)
tests_unit_deadline_SOURCES = tests/unit/deadline.c tests/lib/random.c \
  tests/lib/random.h
tests_unit_deadline_LDADD = $(LIBVSR)
tests_unit_client_SOURCES = tests/unit/client.c tests/lib/random.c tests/lib/random.h
tests_unit_client_LDADD = $(LIBVSR)
tests_integration_replication_SOURCES = tests/integration/replication.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_integration_replication_LDADD = $(LIBVSR)
tests_integration_normal_contract_SOURCES = tests/integration/normal_contract.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_integration_normal_contract_LDADD = $(LIBVSR)
tests_integration_reads_SOURCES = tests/integration/reads.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_integration_reads_LDADD = $(LIBVSR)
tests_integration_checkpoints_SOURCES = tests/integration/checkpoints.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_integration_checkpoints_LDADD = $(LIBVSR)
tests_integration_epochs_SOURCES = tests/integration/epochs.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_integration_epochs_LDADD = $(LIBVSR)
tests_integration_lifecycle_SOURCES = tests/integration/lifecycle.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_integration_lifecycle_LDADD = $(LIBVSR)
tests_integration_transitions_SOURCES = tests/integration/transitions.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_integration_transitions_LDADD = $(LIBVSR)
tests_integration_audit_SOURCES = tests/integration/audit.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_integration_audit_LDADD = $(LIBVSR)
tests_integration_transfer_audit_SOURCES = tests/integration/transfer_audit.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_integration_transfer_audit_LDADD = $(LIBVSR)
tests_integration_api_contract_SOURCES = tests/integration/api_contract.c
tests_integration_api_contract_LDADD = $(LIBVSR)
tests_integration_failures_SOURCES = tests/integration/failures.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_integration_failures_LDADD = $(LIBVSR)
tests_integration_view_change_edges_SOURCES = tests/integration/view_change_edges.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_integration_view_change_edges_LDADD = $(LIBVSR)
tests_integration_offers_SOURCES = tests/integration/offers.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_integration_offers_LDADD = $(LIBVSR)
tests_integration_capture_SOURCES = tests/integration/capture.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_integration_capture_LDADD = $(LIBVSR)
tests_integration_clients_SOURCES = tests/integration/clients.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_integration_clients_LDADD = $(LIBVSR)
tests_regression_trim_resend_SOURCES = tests/regression/trim_resend.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_regression_trim_resend_LDADD = $(LIBVSR)
tests_regression_recovery_coverage_SOURCES = tests/regression/recovery_coverage.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_regression_recovery_coverage_LDADD = $(LIBVSR)
tests_regression_transfer_anchor_SOURCES = tests/regression/transfer_anchor.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_regression_transfer_anchor_LDADD = $(LIBVSR)
tests_regression_witness_prefix_SOURCES = tests/regression/witness_prefix.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_regression_witness_prefix_LDADD = $(LIBVSR)
tests_regression_pressure_reload_SOURCES = tests/regression/pressure_reload.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_regression_pressure_reload_LDADD = $(LIBVSR)
tests_regression_lagging_reload_SOURCES = tests/regression/lagging_reload.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_regression_lagging_reload_LDADD = $(LIBVSR)
tests_regression_anchor_behind_applied_SOURCES = \
  tests/regression/anchor_behind_applied.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_regression_anchor_behind_applied_LDADD = $(LIBVSR)
tests_regression_reply_restore_SOURCES = tests/regression/reply_restore.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_regression_reply_restore_LDADD = $(LIBVSR)
tests_regression_view_change_minimum_SOURCES = \
  tests/regression/view_change_minimum.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_regression_view_change_minimum_LDADD = $(LIBVSR)
tests_regression_review_epoch_recovery_vote_SOURCES = \
  tests/regression/review_epoch_recovery_vote.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_regression_review_epoch_recovery_vote_LDADD = $(LIBVSR)
tests_regression_epoch_announcement_recovery_SOURCES = \
  tests/regression/epoch_announcement_recovery.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_regression_epoch_announcement_recovery_LDADD = $(LIBVSR)
tests_regression_next_epoch_before_steady_SOURCES = \
  tests/regression/next_epoch_before_steady.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_regression_next_epoch_before_steady_LDADD = $(LIBVSR)
tests_regression_boundary_from_view_change_SOURCES = \
  tests/regression/boundary_from_view_change.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_regression_boundary_from_view_change_LDADD = $(LIBVSR)
tests_regression_learner_restart_SOURCES = tests/regression/learner_restart.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_regression_learner_restart_LDADD = $(LIBVSR)
tests_regression_learner_follows_SOURCES = tests/regression/learner_follows.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_regression_learner_follows_LDADD = $(LIBVSR)
tests_regression_recovery_into_handoff_SOURCES = \
  tests/regression/recovery_into_handoff.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_regression_recovery_into_handoff_LDADD = $(LIBVSR)
tests_regression_api_init_out_SOURCES = tests/regression/api_init_out.c
tests_regression_api_init_out_LDADD = $(LIBVSR)
CLUSTER_SCENARIO = tests/fuzzy/scenario.c tests/fuzzy/scenario.h \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_fuzzy_cluster_SOURCES = tests/fuzzy/cluster.c $(CLUSTER_SCENARIO) \
  tests/lib/random.c tests/lib/random.h
tests_fuzzy_cluster_LDADD = $(LIBVSR)
# The same scheduler with a wider default flag union: 16 seeds of
# 1+4+8+16+64. Flags 2 and 32 run in the campaigns listed in
# docs/implementation.md rather than on every check, to bound its time.
tests_fuzzy_cluster_extended_SOURCES = $(tests_fuzzy_cluster_SOURCES)
tests_fuzzy_cluster_extended_CPPFLAGS = $(AM_CPPFLAGS) \
  -DCLUSTER_DEFAULT_COUNT=16 -DCLUSTER_DEFAULT_PROFILE=93
tests_fuzzy_cluster_extended_LDADD = $(LIBVSR)
EXTRA_PROGRAMS = tests/benchmark/core
tests_benchmark_core_SOURCES = tests/benchmark/core.c \
  tests/lib/memory.c tests/lib/memory.h \
  tests/lib/memory_cluster.c tests/lib/memory_cluster.h
tests_benchmark_core_LDADD = $(LIBVSR)
tests_benchmark_core_LDFLAGS = $(AM_LDFLAGS) -Wl,--wrap=vsr_step
TESTS = $(check_PROGRAMS) examples/replication examples/failover examples/checkpoint \
  examples/epochs examples/reads examples/witness

# Keep the public signatures and descriptor layouts checked independently.
check_LIBRARIES = tests/integration/libheader.a
tests_integration_libheader_a_SOURCES = tests/integration/header.c

if FUZZING
noinst_PROGRAMS += tests/fuzzy/checked tests/fuzzy/validation tests/fuzzy/cluster_fuzz
tests_fuzzy_checked_SOURCES = tests/fuzzy/checked.c
tests_fuzzy_checked_LDADD = $(LIBVSR)
tests_fuzzy_checked_LDFLAGS = $(AM_LDFLAGS) -fsanitize=fuzzer
tests_fuzzy_validation_SOURCES = tests/fuzzy/validation.c
tests_fuzzy_validation_LDADD = $(LIBVSR)
tests_fuzzy_validation_LDFLAGS = $(AM_LDFLAGS) -fsanitize=fuzzer
tests_fuzzy_cluster_fuzz_SOURCES = tests/fuzzy/cluster_fuzz.c $(CLUSTER_SCENARIO)
tests_fuzzy_cluster_fuzz_LDADD = $(LIBVSR)
tests_fuzzy_cluster_fuzz_LDFLAGS = $(AM_LDFLAGS) -fsanitize=fuzzer
endif

EXTRA_DIST = \
  bootstrap vsr.pc.in .clang-format .clang-tidy .editorconfig .gitignore \
  .github/workflows/ci.yml \
  README.md DESIGN.md docs/protocol.md docs/vsr-api.md docs/development.md \
  docs/implementation.md docs/changes.md docs/io-design.md \
  docs/io-implementation.md examples/README.md \
  tests/README.md tests/regression/README.md tests/benchmark/README.md \
  tests/fuzzy/KNOWN_FAILURES.md

# Ask Clang for matching tools, including Debian's versioned installations.
# Override with e.g. make format-check CLANG_FORMAT=clang-format-18.
CLANG_FORMAT = $(shell $(CC) -print-prog-name=clang-format)
CLANG_TIDY = $(shell $(CC) -print-prog-name=clang-tidy)
SCAN_BUILD = $(shell $(CC) -print-prog-name=scan-build)
LLVM_COV = $(shell $(CC) -print-prog-name=llvm-cov)
LLVM_PROFDATA = $(shell $(CC) -print-prog-name=llvm-profdata)
FORMAT_FILES = find $(srcdir)/include $(srcdir)/src $(srcdir)/tests $(srcdir)/examples -type f \( -name '*.c' -o -name '*.h' \) -print0
# Overridable from the environment as well as the command line.
FUZZ_RUNS ?= 10000
FUZZ_ARGS ?=

.PHONY: check-build check-unit check-integration check-regression check-fuzzy \
  format format-check lint \
  compile-commands tidy cppcheck analyze shellcheck check-valgrind coverage fuzz

.PHONY: benchmark
benchmark: tests/benchmark/core
	./tests/benchmark/core

check-build: all
	$(MAKE) $(check_PROGRAMS) $(check_LIBRARIES)

check-unit: check-build
	$(MAKE) check TESTS='$(UNIT_TESTS)'

check-integration: check-build
	$(MAKE) check TESTS='$(INTEGRATION_TESTS)'

check-regression: check-build
	$(MAKE) check TESTS='$(REGRESSION_TESTS)'

check-fuzzy: check-build
	$(MAKE) check TESTS='$(FUZZY_TESTS)'

format:
	$(FORMAT_FILES) | xargs -0 $(CLANG_FORMAT) -i

format-check:
	$(FORMAT_FILES) | xargs -0 $(CLANG_FORMAT) --dry-run --Werror

shellcheck:
	shellcheck $(srcdir)/bootstrap

# Clean first: an incremental/no-op Bear build produces an incomplete database.
compile-commands:
	$(MAKE) clean
	bear --output compile_commands.json -- $(MAKE) check-build

compile_commands.json:
	@echo 'Run make compile-commands first.' >&2; exit 1

tidy: compile_commands.json
	@set -e; for file in $(filter %.c,$(SOURCES)); do \
	  $(CLANG_TIDY) --quiet -p . $(srcdir)/$$file; \
	done

# The const* suggestions are style-only and keep signatures symmetric with
# callbacks and mutating siblings. Tests mutate objects that stores, graphs
# and memberships hold by pointer, which the flow analysis reads as unused
# or redundant stores. The other checks stay fatal.
cppcheck: compile_commands.json
	cppcheck --project=compile_commands.json --enable=warning,style,performance,portability \
	  --error-exitcode=1 --inline-suppr --suppress=missingIncludeSystem \
	  --suppress=constParameterPointer --suppress=constVariablePointer \
	  --suppress=constParameterCallback --suppress=constVariable \
	  --suppress='unreadVariable:*/tests/*' --suppress='redundantAssignment:*/tests/*'

lint: format-check shellcheck tidy cppcheck

analyze:
	$(MAKE) clean
	$(SCAN_BUILD) --status-bugs --use-cc='$(CC)' -o analysis $(MAKE) check-build

check-valgrind:
	@test '$(VSR_SANITIZE)' = no || { echo 'Use an unsanitized build for Valgrind.' >&2; exit 1; }
	$(MAKE) check LOG_COMPILER='valgrind --error-exitcode=1 --leak-check=full --show-leak-kinds=all --errors-for-leak-kinds=all --track-origins=yes'

if COVERAGE
COVERAGE_ARGS = $(addprefix -object=,$(check_PROGRAMS)) -instr-profile=coverage/vsr.profdata -ignore-filename-regex='(/tests/|/usr/)'
coverage:
	rm -rf coverage
	$(MKDIR_P) coverage
	LLVM_PROFILE_FILE='$(abs_builddir)/coverage/%m-%p.profraw' $(MAKE) check
	$(LLVM_PROFDATA) merge -sparse coverage/*.profraw -o coverage/vsr.profdata
	$(LLVM_COV) report $(COVERAGE_ARGS)
	$(LLVM_COV) show $(COVERAGE_ARGS) -format=html -output-dir=coverage/html -show-branches=count
	$(LLVM_COV) export $(COVERAGE_ARGS) -format=lcov > coverage/coverage.lcov
else
coverage:
	@echo 'Configure a separate build with --enable-coverage first.' >&2; exit 1
endif

if FUZZING
fuzz: tests/fuzzy/checked tests/fuzzy/validation tests/fuzzy/cluster_fuzz
	$(MKDIR_P) tests/fuzzy/corpus/checked tests/fuzzy/corpus/validation \
	  tests/fuzzy/corpus/cluster tests/fuzzy/artifacts
	./tests/fuzzy/checked -runs=$(FUZZ_RUNS) -max_len=64 \
	  -artifact_prefix=tests/fuzzy/artifacts/ $(FUZZ_ARGS) tests/fuzzy/corpus/checked
	./tests/fuzzy/validation -runs=$(FUZZ_RUNS) -max_len=256 \
	  -artifact_prefix=tests/fuzzy/artifacts/ $(FUZZ_ARGS) tests/fuzzy/corpus/validation
	./tests/fuzzy/cluster_fuzz -runs=$(FUZZ_RUNS) -max_len=512 \
	  -artifact_prefix=tests/fuzzy/artifacts/ $(FUZZ_ARGS) tests/fuzzy/corpus/cluster
else
fuzz:
	@echo 'Configure a separate build with --enable-fuzzing first.' >&2; exit 1
endif

clean-local:
	-rm -rf coverage analysis

distclean-local:
	-rm -f compile_commands.json

# Fuzz corpora/crashes are deliberately retained by clean and distclean.
