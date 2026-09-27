ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = -I$(top_srcdir)/include -I$(top_srcdir)/src -I$(top_srcdir)/tests
AM_CFLAGS = $(VSR_CFLAGS) $(VSR_INSTRUMENT_CFLAGS)
AM_LDFLAGS = $(VSR_LDFLAGS) $(VSR_INSTRUMENT_LDFLAGS)

# The protocol is not implemented yet: build an internal archive without
# installing a library that appears to implement the public API.
noinst_LIBRARIES = src/libvsr.a
src_libvsr_a_SOURCES = src/checked.c src/checked.h
noinst_HEADERS = include/vsr.h tests/lib/check.h

check_PROGRAMS = tests/unit/checked
tests_unit_checked_SOURCES = tests/unit/checked.c
tests_unit_checked_LDADD = src/libvsr.a
TESTS = $(check_PROGRAMS)

# Compile-only contracts may reference API functions with no implementation yet.
check_LIBRARIES = tests/integration/libheader.a
tests_integration_libheader_a_SOURCES = tests/integration/header.c

if FUZZING
noinst_PROGRAMS = tests/fuzz/checked
tests_fuzz_checked_SOURCES = tests/fuzz/checked.c
tests_fuzz_checked_LDADD = src/libvsr.a
tests_fuzz_checked_LDFLAGS = $(AM_LDFLAGS) -fsanitize=fuzzer
endif

EXTRA_DIST = \
  bootstrap .clang-format .clang-tidy .editorconfig .gitignore \
  .github/workflows/ci.yml \
  README.md DESIGN.md docs/protocol.md docs/vsr-api.md docs/development.md \
  tests/README.md tests/regression/README.md tests/benchmark/README.md

# Ask Clang for matching tools, including Debian's versioned installations.
# Override with e.g. make format-check CLANG_FORMAT=clang-format-18.
CLANG_FORMAT = $(shell $(CC) -print-prog-name=clang-format)
CLANG_TIDY = $(shell $(CC) -print-prog-name=clang-tidy)
SCAN_BUILD = $(shell $(CC) -print-prog-name=scan-build)
LLVM_COV = $(shell $(CC) -print-prog-name=llvm-cov)
LLVM_PROFDATA = $(shell $(CC) -print-prog-name=llvm-profdata)
FORMAT_FILES = find $(srcdir)/include $(srcdir)/src $(srcdir)/tests -type f \( -name '*.c' -o -name '*.h' \) -print0
FUZZ_RUNS = 10000
FUZZ_ARGS =

.PHONY: check-build check-unit check-integration format format-check lint \
  compile-commands tidy cppcheck analyze shellcheck check-valgrind coverage fuzz

check-build: all $(check_PROGRAMS) $(check_LIBRARIES)

check-unit: check-build
	$(MAKE) check TESTS='tests/unit/checked'

check-integration: $(check_LIBRARIES)

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

cppcheck: compile_commands.json
	cppcheck --project=compile_commands.json --enable=warning,style,performance,portability \
	  --error-exitcode=1 --inline-suppr --suppress=missingIncludeSystem

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
fuzz: tests/fuzz/checked
	$(MKDIR_P) tests/fuzz/corpus/checked tests/fuzz/artifacts
	./tests/fuzz/checked -runs=$(FUZZ_RUNS) -max_len=64 \
	  -artifact_prefix=tests/fuzz/artifacts/ $(FUZZ_ARGS) tests/fuzz/corpus/checked
else
fuzz:
	@echo 'Configure a separate build with --enable-fuzzing first.' >&2; exit 1
endif

clean-local:
	-rm -rf coverage analysis

distclean-local:
	-rm -f compile_commands.json

# Fuzz corpora/crashes are deliberately retained by clean and distclean.
