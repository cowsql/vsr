AC_PREREQ([2.69])
AC_INIT([vsr], [0.1.0])
AC_CONFIG_SRCDIR([include/vsr.h])
AC_CONFIG_AUX_DIR([build-aux])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_HEADERS([config.h])
AM_INIT_AUTOMAKE([1.16 foreign subdir-objects -Wall -Werror -Wno-portability dist-xz no-dist-gzip])
AM_SILENT_RULES([yes])

# Respect explicit compiler/flag choices, but make Clang the default.
AS_IF([test -z "$CC"], [CC=clang])
AS_IF([test "${CFLAGS+set}" != set], [CFLAGS="-O1 -g3"])
AC_PROG_CC
AM_PROG_AR
AC_PROG_RANLIB

AC_ARG_ENABLE([werror],
  [AS_HELP_STRING([--enable-werror], [treat all compiler warnings as errors])],
  [], [enable_werror=no])
AC_ARG_ENABLE([hardening],
  [AS_HELP_STRING([--disable-hardening], [disable supported stack/linker hardening])],
  [], [enable_hardening=yes])
AC_ARG_ENABLE([sanitize],
  [AS_HELP_STRING([--enable-sanitize@<:@=KIND@:>@],
    [yes (=address,undefined), address, undefined, integer, address,undefined,integer, thread, or memory; default=yes; --disable-sanitize for Valgrind/release builds])],
  [], [enable_sanitize=yes])
AC_ARG_ENABLE([coverage],
  [AS_HELP_STRING([--enable-coverage], [enable Clang source-based coverage])],
  [], [enable_coverage=no])
AC_ARG_ENABLE([fuzzing],
  [AS_HELP_STRING([--enable-fuzzing], [build libFuzzer targets (requires Clang and ASan/UBSan)])],
  [], [enable_fuzzing=no])

for vsr_option in "$enable_werror" "$enable_hardening" "$enable_coverage" "$enable_fuzzing"; do
  AS_CASE([$vsr_option], [yes|no], [], [AC_MSG_ERROR([boolean options accept only yes or no])])
done
AS_CASE([$enable_sanitize],
  [yes], [enable_sanitize=address,undefined],
  [no|address|undefined|address,undefined|integer|address,undefined,integer|thread|memory], [],
  [AC_MSG_ERROR([unsupported sanitizer selection: $enable_sanitize])])
AS_IF([test "x$enable_fuzzing" = xyes], [
  AS_IF([test "x$enable_sanitize" = xno], [enable_sanitize=address,undefined])
  AS_CASE([$enable_sanitize], [address,undefined|address,undefined,integer], [],
    [AC_MSG_ERROR([fuzzing requires --enable-sanitize=address,undefined (optionally with ,integer)])])
])

VSR_CFLAGS="-std=c11"
VSR_LDFLAGS=""

# Probe diagnostics rather than silently passing unsupported flags to GCC.
m4_foreach_w([vsr_flag], [
  -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion -Wshadow
  -Wformat=2 -Wundef -Wcast-align -Wcast-qual -Wwrite-strings
  -Wstrict-prototypes -Wmissing-prototypes -Wold-style-definition
  -Wvla -Wswitch-enum -Wdouble-promotion -Wnull-dereference
  -Wuninitialized -Wconditional-uninitialized -Wmaybe-uninitialized
  -Warray-bounds=2 -Warray-parameter=2 -Wstrict-flex-arrays
  -Wformat-overflow=2 -Wformat-truncation=2 -Wformat-signedness
  -Wstringop-overflow=4 -Wstringop-overread -Wstringop-truncation
  -Wduplicated-cond -Wduplicated-branches -Wlogical-op -Wrestrict
  -Walloca -Walloc-zero -Walloc-size -Wpointer-arith -Wbad-function-cast
  -Wcast-function-type -Wcast-function-type-strict -Wstrict-aliasing=3
  -Wmissing-declarations -Wmissing-variable-declarations -Wnested-externs
  -Wshorten-64-to-32 -Wimplicit-int-conversion -Wenum-conversion
  -Wdate-time -Wunreachable-code -Wuse-after-free=3 -Wtrampolines
  -Wimplicit-fallthrough -Werror=implicit-function-declaration
  -Werror=incompatible-pointer-types -Werror=return-type
  -fno-common -fno-omit-frame-pointer -fstrict-flex-arrays=3
  -fanalyzer
], [VSR_CHECK_CFLAG(vsr_flag)])

AS_IF([test "x$enable_werror" = xyes], [
  VSR_CFLAGS="$VSR_CFLAGS -Werror"
])
AS_IF([test "x$enable_hardening" = xyes], [
  AC_DEFINE([VSR_HARDENING], [1], [Enable libc fortification in optimized builds.])
  VSR_CHECK_CFLAG([-fstack-protector-strong])
  VSR_CHECK_CFLAG([-fstack-clash-protection])
  VSR_CHECK_CFLAG([-fcf-protection=full])
  VSR_CHECK_CFLAG([-mbranch-protection=standard])
  VSR_CHECK_LDFLAG([-Wl,-z,relro])
  VSR_CHECK_LDFLAG([-Wl,-z,now])
])

# Include config.h before system headers. Keep -O0 and user fortify overrides
# usable; level 3 needs dynamic object-size support, otherwise use level 2.
AH_BOTTOM([
#if defined(VSR_HARDENING) && defined(__OPTIMIZE__) && !defined(_FORTIFY_SOURCE)
# if defined(__has_builtin)
#  if __has_builtin(__builtin_dynamic_object_size)
#   define _FORTIFY_SOURCE 3
#  endif
# endif
# ifndef _FORTIFY_SOURCE
#  define _FORTIFY_SOURCE 2
# endif
#endif
])

VSR_INSTRUMENT_CFLAGS=""
VSR_INSTRUMENT_LDFLAGS=""
AS_IF([test "x$enable_sanitize" != xno], [
  VSR_INSTRUMENT_CFLAGS="-fsanitize=$enable_sanitize -fno-sanitize-recover=all -fno-omit-frame-pointer -fno-optimize-sibling-calls"
  VSR_INSTRUMENT_LDFLAGS="-fsanitize=$enable_sanitize"
  AS_CASE([$enable_sanitize], [*address*], [
    VSR_CHECK_CFLAG([-fsanitize-address-use-after-scope])
    VSR_CHECK_CFLAG([-fsanitize-address-use-after-return=always])
  ])
  AS_CASE([$enable_sanitize],
    [memory], [
      VSR_INSTRUMENT_CFLAGS="$VSR_INSTRUMENT_CFLAGS -fsanitize-memory-track-origins=2 -fPIE"
      VSR_INSTRUMENT_LDFLAGS="$VSR_INSTRUMENT_LDFLAGS -pie"
    ],
    [thread], [
      VSR_INSTRUMENT_CFLAGS="$VSR_INSTRUMENT_CFLAGS -fPIE"
      VSR_INSTRUMENT_LDFLAGS="$VSR_INSTRUMENT_LDFLAGS -pie"
    ])
])
AS_IF([test "x$enable_coverage" = xyes], [
  VSR_INSTRUMENT_CFLAGS="$VSR_INSTRUMENT_CFLAGS -fprofile-instr-generate -fcoverage-mapping"
  VSR_INSTRUMENT_LDFLAGS="$VSR_INSTRUMENT_LDFLAGS -fprofile-instr-generate"
])
AS_IF([test "x$enable_fuzzing" = xyes], [
  # Instrument the library as well as the harness, without replacing test mains.
  VSR_INSTRUMENT_CFLAGS="$VSR_INSTRUMENT_CFLAGS -fsanitize=fuzzer-no-link"
  VSR_INSTRUMENT_LDFLAGS="$VSR_INSTRUMENT_LDFLAGS -fsanitize=fuzzer-no-link"
  vsr_save_CFLAGS=$CFLAGS
  vsr_save_LDFLAGS=$LDFLAGS
  CFLAGS="$CFLAGS -fsanitize=fuzzer"
  LDFLAGS="$LDFLAGS -fsanitize=fuzzer"
  AC_MSG_CHECKING([whether libFuzzer is available])
  AC_LINK_IFELSE([AC_LANG_SOURCE([[
    #include <stddef.h>
    int LLVMFuzzerTestOneInput(const unsigned char *data, size_t size) {
      (void)data; (void)size; return 0;
    }
  ]])], [AC_MSG_RESULT([yes])],
    [AC_MSG_FAILURE([libFuzzer is unavailable; install the Clang compiler-rt development package])])
  CFLAGS=$vsr_save_CFLAGS
  LDFLAGS=$vsr_save_LDFLAGS
])

# Fail at configure time if the compiler cannot compile AND link the chosen
# instrumentation. Never quietly drop a requested safety check.
vsr_save_CFLAGS=$CFLAGS
vsr_save_LDFLAGS=$LDFLAGS
CFLAGS="$CFLAGS $VSR_CFLAGS $VSR_INSTRUMENT_CFLAGS"
LDFLAGS="$LDFLAGS $VSR_LDFLAGS $VSR_INSTRUMENT_LDFLAGS"
AC_MSG_CHECKING([whether the selected C11 build flags work])
AC_LINK_IFELSE([AC_LANG_PROGRAM([[
  #include <stdint.h>
  _Static_assert(sizeof(uint64_t) == 8, "uint64_t required");
]], [[]])], [AC_MSG_RESULT([yes])],
  [AC_MSG_FAILURE([C11 build flags or sanitizer/coverage runtime unavailable; see config.log])])
CFLAGS=$vsr_save_CFLAGS
LDFLAGS=$vsr_save_LDFLAGS

# The I/O layer in the same libvsr.a drives io_uring through liburing. The
# baseline is Linux 7.2 with liburing 2.15, without probing or fallbacks
# (docs/io-design.md, section 2). URING_CFLAGS and URING_LIBS in the
# environment override pkg-config; the link check below verifies either way.
m4_ifndef([PKG_CHECK_MODULES],
  [m4_fatal([pkg.m4 is missing: install pkg-config (or pkgconf) and rerun ./bootstrap])])
PKG_PROG_PKG_CONFIG
PKG_CHECK_MODULES([URING], [liburing >= 2.15], [],
  [AC_MSG_ERROR([liburing >= 2.15 is required (Debian: liburing-dev); set URING_CFLAGS and URING_LIBS to override pkg-config:
$URING_PKG_ERRORS])])
vsr_save_CPPFLAGS=$CPPFLAGS
vsr_save_LIBS=$LIBS
CPPFLAGS="$CPPFLAGS $URING_CFLAGS"
LIBS="$URING_LIBS $LIBS"
AC_MSG_CHECKING([whether liburing >= 2.15 compiles and links])
AC_LINK_IFELSE([AC_LANG_PROGRAM([[
  #include <liburing.h>
  #if IO_URING_VERSION_MAJOR < 2 || (IO_URING_VERSION_MAJOR == 2 && IO_URING_VERSION_MINOR < 15)
  #error liburing headers older than 2.15
  #endif
]], [[return io_uring_major_version() == 2 && io_uring_minor_version() < 15;]])],
  [AC_MSG_RESULT([yes])],
  [AC_MSG_FAILURE([liburing >= 2.15 headers or library unusable; see config.log])])
CPPFLAGS=$vsr_save_CPPFLAGS
LIBS=$vsr_save_LIBS

AC_SUBST([VSR_CFLAGS])
AC_SUBST([VSR_LDFLAGS])
AC_SUBST([VSR_INSTRUMENT_CFLAGS])
AC_SUBST([VSR_INSTRUMENT_LDFLAGS])
AC_SUBST([VSR_SANITIZE], [$enable_sanitize])
AM_CONDITIONAL([COVERAGE], [test "x$enable_coverage" = xyes])
AM_CONDITIONAL([FUZZING], [test "x$enable_fuzzing" = xyes])
AC_CONFIG_FILES([Makefile vsr.pc])
AC_OUTPUT
AC_MSG_NOTICE([compiler: $CC; sanitizers: $enable_sanitize; coverage: $enable_coverage; fuzzing: $enable_fuzzing; liburing: $URING_LIBS])
