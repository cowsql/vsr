# Local probes keep bootstrap independent of autoconf-archive.
AC_DEFUN([VSR_CHECK_CFLAG], [
  AC_MSG_CHECKING([whether $CC supports $1])
  vsr_probe_save=$CFLAGS
  CFLAGS="$CFLAGS $VSR_CFLAGS $VSR_INSTRUMENT_CFLAGS -Werror $1"
  AC_COMPILE_IFELSE([AC_LANG_PROGRAM([], [])],
    [AC_MSG_RESULT([yes]); VSR_CFLAGS="$VSR_CFLAGS $1"],
    [AC_MSG_RESULT([no])])
  CFLAGS=$vsr_probe_save
])

AC_DEFUN([VSR_CHECK_LDFLAG], [
  AC_MSG_CHECKING([whether the linker supports $1])
  vsr_probe_save=$LDFLAGS
  LDFLAGS="$LDFLAGS $1"
  AC_LINK_IFELSE([AC_LANG_PROGRAM([], [])],
    [AC_MSG_RESULT([yes]); VSR_LDFLAGS="$VSR_LDFLAGS $1"],
    [AC_MSG_RESULT([no])])
  LDFLAGS=$vsr_probe_save
])
