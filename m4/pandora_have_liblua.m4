dnl  Copyright (C) 2012 Couchbase, Inc.
dnl This file is free software; Couchbase, Inc.
dnl gives unlimited permission to copy and/or distribute it,
dnl with or without modifications, as long as this notice is preserved.

AC_DEFUN([_PANDORA_SEARCH_LIBLUA],[
  AC_REQUIRE([AC_LIB_PREFIX])

  dnl --------------------------------------------------------------------
  dnl  Check for liblua
  dnl --------------------------------------------------------------------

  AC_ARG_ENABLE([liblua],
    [AS_HELP_STRING([--disable-liblua],
      [Build with liblua support @<:@default=on@:>@])],
    [ac_enable_liblua="$enableval"],
    [ac_enable_liblua="yes"])

  AS_IF([test "x$ac_enable_liblua" = "xyes"],[
    AC_LIB_HAVE_LINKFLAGS(lua,,[
      #include <stdio.h>
      #include <lua.h>
    ],[
      lua_State *l;
      l = lua_newstate(NULL, NULL);
    ])
  ],[
    ac_cv_liblua="no"
  ])

  AM_CONDITIONAL(HAVE_LIBLUA, [test "x${ac_cv_liblua}" = "xyes"])
])

AC_DEFUN([PANDORA_HAVE_LIBLUA],[
  AC_REQUIRE([_PANDORA_SEARCH_LIBLUA])
])

AC_DEFUN([PANDORA_REQUIRE_LIBLUA],[
  AC_REQUIRE([_PANDORA_SEARCH_LIBLUA])
  AS_IF([test "x${ac_cv_liblua}" = "xno"],
    AC_MSG_ERROR([liblua is required for ${PACKAGE}]))
])
