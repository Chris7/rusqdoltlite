#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([FULL-PACKAGE-NAME], [VERSION], [BUG-REPORT-ADDRESS])
AC_CONFIG_SRCDIR([src/simplexml.h])
AC_CONFIG_HEADERS([config.h])
AM_INIT_AUTOMAKE

# Checks for programs.
AC_PROG_CC

# Checks for libraries.
AC_SEARCH_LIBS(dlopen, dl)
AC_CANONICAL_HOST
case $host_os in
  *mingw*) ;;
  *msys*) ;;
  *) AC_SEARCH_LIBS(pthread_create, pthread)
esac

AC_CHECK_HEADERS(
  [winsock2.h], 
  [LIBS="-lws2_32 $LIBS" ], 
  , 
  [#include <windows.h>]
)

AC_ARG_WITH(ssl, [AS_HELP_STRING([--with-ssl], [specify openssl install])])
if test "x$with_ssl" != "xno" ; then
  if test "x$with_ssl" != "xyes" ; then
    CPPFLAGS="-I${withval}/include $CPPFLAGS"
    LDFLAGS="-L${withval}/lib $LDFLAGS"
  fi
fi
AC_SEARCH_LIBS(HMAC_Init_ex, ssl_static crypto_static ssl crypto, [], [
  AC_MSG_ERROR([cannot find libssl])
])

AC_ARG_WITH(curl, [AS_HELP_STRING([--with-curl], [specify libcurl install])])
if test "x$with_curl" != "xno" ; then
  if test "x$with_curl" != "xyes" ; then
    CPPFLAGS="-I${withval}/include $CPPFLAGS"
    LDFLAGS="-L${withval}/lib $LDFLAGS"
  fi
fi
AC_SEARCH_LIBS(curl_easy_setopt, curl, [], [
  AC_MSG_ERROR([cannot find libcurl])
])

# Look for Tcl:
use_tcl=0
AC_ARG_WITH(tcl, [AS_HELP_STRING([--with-tcl], [specify tcl install])])
if test "x$with_tcl" != "xno" ; then
  if test "x$with_tcl" != "xyes" ; then
    CPPFLAGS="-I${withval}/include $CPPFLAGS"
    LDFLAGS="-L${withval}/lib $LDFLAGS"
  fi
  use_tcl=1
  AC_CHECK_HEADERS(tcl.h, [], [use_tcl=0])
  AC_SEARCH_LIBS(Tcl_SetObjResult, tcl tcl86, [], [use_tcl=0])
fi
if test $use_tcl -eq 0 ; then
  echo "###########################################"
  echo "CANNOT FIND TCL - NOT BUILDING TEST PROGRAM"
  echo "###########################################"
  AC_SUBST([bcvtclsqlite], [])
else
  AC_SUBST([bcvtclsqlite], 'bcvtclsqlite$(EXEEXT)')
fi

AC_CONFIG_FILES([Makefile])

AC_OUTPUT

