dnl ============================================================================
dnl ArangoDB                                                   (c) ArangoDB GmbH
dnl ============================================================================

dnl ============================================================================
dnl --SECTION--                                  ArangoDB GmbH Build Environment
dnl ============================================================================

AC_INIT([arangodb-mesos], [0.0.1], [info@arangodb.com], [arangodb-mesos], [https://www.arangodb.com])

dnl ----------------------------------------------------------------------------
dnl auxillary directory for install-sh and missing
dnl ----------------------------------------------------------------------------

AC_CONFIG_AUX_DIR([config])
AC_CONFIG_MACRO_DIR([m4])

CURRENT_DIR=`pwd`

AC_MSG_NOTICE([configure started in '$CURRENT_DIR])
AC_MSG_NOTICE([with CC='$CC'])
AC_MSG_NOTICE([with CXX='$CXX'])
AC_MSG_NOTICE([with CPPFLAGS='$CPPFLAGS'])
AC_MSG_NOTICE([with CFLAGS='$CFLAGS'])
AC_MSG_NOTICE([with CXXFLAGS='$CXXFLAGS'])
AC_MSG_NOTICE([with LDFLAGS='$LDFLAGS'])

dnl ----------------------------------------------------------------------------
dnl use automake to generate Makfile.in
dnl ----------------------------------------------------------------------------

AM_INIT_AUTOMAKE([foreign subdir-objects])
AM_SILENT_RULES(yes)

dnl ----------------------------------------------------------------------------
dnl checks for compiler and basic settings
dnl ----------------------------------------------------------------------------

AC_PROG_CXXCPP
AC_PROG_CC
AC_PROG_CXX
AC_PROG_CPP
AM_PROG_CC_C_O
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_RANLIB

AX_CXX_COMPILE_STDCXX_11(noext, mandatory)

dnl ============================================================================
dnl --SECTION--                                               external libraries
dnl ============================================================================

dnl -----------------------------------------------------------------------------------------
dnl pthreads
dnl -----------------------------------------------------------------------------------------

AC_LANG(C)

ACX_PTHREAD(tr_PTHREAD=yes, tr_PTHREAD=no)

if test "x$tr_PTHREAD" = xno;  then
  AC_MSG_ERROR([please install the pthread library])
fi

CFLAGS="${CFLAGS} ${PTHREAD_CFLAGS}"
CXXFLAGS="${CXXFLAGS} ${PTHREAD_CFLAGS}"
LDFLAGS="${LDFLAGS} ${PTHREAD_CFLAGS}"
LIBS="${LIBS} ${PTHREAD_LIBS} "

dnl ----------------------------------------------------------------------------
dnl mesos
dnl ----------------------------------------------------------------------------

MESOS_CPPFLAGS="-I../mesos/src -I../mesos/build/src"
MESOS_LIBS="-lmesos"

AC_SUBST(MESOS_CPPFLAGS)
AC_SUBST(MESOS_LIBS)

dnl ----------------------------------------------------------------------------
dnl microhttpd
dnl ----------------------------------------------------------------------------

MICROHTTPD_CPPFLAGS=""
MICROHTTPD_LIBS="-lmicrohttpd"

AC_SUBST(MICROHTTPD_CPPFLAGS)
AC_SUBST(MICROHTTPD_LIBS)

dnl ============================================================================
dnl --SECTION--                                                   GENERATE FILES
dnl ============================================================================

AC_CONFIG_FILES([Makefile])
AC_OUTPUT

dnl ============================================================================
dnl --SECTION--                                                      END-OF-FILE
dnl ============================================================================

dnl Local Variables:
dnl mode: outline-minor
dnl outline-regexp: "dnl [a-zA-Z]\\|dnl --SECTION--"
dnl End:
