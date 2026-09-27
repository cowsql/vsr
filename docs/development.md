# Development

Public headers go in `include/`, private C code in `src/`, runnable examples in
`examples/`, and all tests in `tests/`. The library builds into `src/libvsr.a`,
which `make install` installs with `include/vsr.h` and `vsr.pc`. It depends on
nothing beyond `memcpy`, `memmove`, `memset`, `memcmp`/`bcmp` and the stack
protector; check with `nm -u src/libvsr.a` in a `--disable-sanitize` build.

## Build

From Git, use Autoconf >= 2.69, Automake >= 1.16, GNU Make, and a C11 compiler.
Clang is the default; override it with `CC=gcc`. No Libtool or external test
framework is needed. Install tools directly on Debian/Ubuntu:

```sh
sudo apt-get install autoconf automake make clang clang-tools clang-format \
  clang-tidy llvm libclang-rt-dev gcc bear cppcheck valgrind shellcheck
```

From the repository root:

```sh
./bootstrap
mkdir -p build/asan
cd build/asan
../../configure --enable-werror
make -j"$(nproc)" check
```

Defaults are `-O1 -g3`, strict warnings, frame pointers and supported stack/linker
hardening. Compiler probes enable extra checks for bounds, conversions,
uninitialized values, format strings, lifetimes and suspicious control flow.
GCC also enables `-fanalyzer`; Clang analysis runs via `make analyze`.
Optimized builds enable libc fortification, including level 3 where supported.
Include generated `config.h` before system headers in implementation files.
The arithmetic helpers require callers to check results and pass non-null outputs.
User `CC`, `CFLAGS`, `CPPFLAGS` and `LDFLAGS` are respected. ASan and UBSan are
enabled by default for the library and every executable test layer. Violations
fail the test. Warnings-as-errors are opt-in; CI enables them.
Use `make V=1` for commands.
Use separate build directories for different flags; rebuild after changing them.

| Configure option | Purpose |
| --- | --- |
| `--enable-werror` | Fail on compiler warnings |
| `--enable-sanitize` | ASan + UBSan, with fatal diagnostics (default) |
| `--disable-sanitize` | Explicitly opt out for Valgrind or release builds |
| `--enable-sanitize=address,undefined,integer` | Also check unsigned overflow and integer conversions with Clang |
| `--enable-sanitize=memory` or `thread` | Separate MSan or TSan build |
| `--enable-fuzzing` | libFuzzer, including library instrumentation and ASan/UBSan |
| `--enable-coverage` | Clang source coverage |
| `--disable-hardening` | Disable stack/linker hardening |

MSan needs instrumented dependencies; TSan is useful for future threaded host
adapters. These runtimes require a compatible host address-space layout.
See the upstream [sanitizer documentation](https://clang.llvm.org/docs/UsersManual.html#controlling-code-generation).
For an optimized GCC build, configure a fresh directory with
`CC=gcc CFLAGS='-O2 -g -DNDEBUG' --enable-werror`.

## Development checks

Run these in a Clang build directory. For `check-valgrind`, configure a separate
directory with `--disable-sanitize` because Valgrind and ASan cannot be combined:

```sh
make format-check
make format                # apply the checked-in style
make compile-commands      # clean rebuild through Bear
make lint                  # format, ShellCheck, clang-tidy, Cppcheck
make analyze               # clean rebuild with Clang Static Analyzer
make check-valgrind        # full leak and origin checks; no sanitizers
make distcheck             # build/test/clean a source distribution
make benchmark             # tests/benchmark/core; use --disable-sanitize
```

`make check-unit`, `make check-integration`, `make check-regression` and
`make check-fuzzy` run one test layer. The examples run as part of `make check`.

Run `compile-commands` and `analyze` sequentially: both clean the build.
Regenerate `compile_commands.json` after changing flags or adding files.
Point clangd at it with `--compile-commands-dir=build/YOUR_BUILD`.
LLVM tools are located through Clang so Debian's versioned packages work;
commands can be overridden, e.g. `make tidy CLANG_TIDY=clang-tidy-21`.
Older Valgrind versions may need `CFLAGS='-O1 -g -gdwarf-4'`.

In a separate build configured with `--enable-fuzzing`, run `make fuzz`;
`FUZZ_RUNS` and `FUZZ_ARGS` can be set in the environment or on the command
line. See [test layers and replay](../tests/README.md). In a build configured
with `--enable-coverage`, run `make coverage` for fresh profiles, a terminal
report, `coverage/html/index.html` and `coverage/coverage.lcov`. LLVM tools must
match the compiler's major. Coverage measures `src/` across every test program
run by `make check`; test sources and system headers are excluded.

CI runs Clang, GCC, sanitizers, lint, static analysis, Valgrind, fuzzing, coverage
and `distcheck` on native runners. Generated Autotools files are ignored in Git
but included in source distributions, which start directly with `./configure`.
