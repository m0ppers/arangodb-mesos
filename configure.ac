dnl ============================================================================
dnl ArangoDB                                                   (c) ArangoDB GmbH
dnl ============================================================================

dnl ============================================================================
dnl --SECTION--                                  ArangoDB GmbH Build Environment
dnl ============================================================================

AC_INIT([arangodb-mesos], [0.3.0], [info@arangodb.com], [arangodb-mesos], [https://www.arangodb.com])

dnl ----------------------------------------------------------------------------
dnl auxillary directory for install-sh and missing
dnl ----------------------------------------------------------------------------

AC_CONFIG_AUX_DIR([config])
AC_CONFIG_MACRO_DIR([m4])

dnl ----------------------------------------------------------------------------
dnl use automake to generate Makfile.in
dnl ----------------------------------------------------------------------------

AM_INIT_AUTOMAKE([foreign subdir-objects])
AM_SILENT_RULES(yes)

dnl ----------------------------------------------------------------------------
dnl output flags
dnl ----------------------------------------------------------------------------

CURRENT_DIR=`pwd`

AC_MSG_NOTICE([configure started in '$CURRENT_DIR])
AC_MSG_NOTICE([with CC='$CC'])
AC_MSG_NOTICE([with CXX='$CXX'])
AC_MSG_NOTICE([with CPPFLAGS='$CPPFLAGS'])
AC_MSG_NOTICE([with CFLAGS='$CFLAGS'])
AC_MSG_NOTICE([with CXXFLAGS='$CXXFLAGS'])
AC_MSG_NOTICE([with LDFLAGS='$LDFLAGS'])

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

if test x$GCC == xyes;  then
  WALL="-Wall -Winit-self -Wno-long-long -Wno-variadic-macros"
fi

CFLAGS="${CFLAGS} ${WALL}"
CXXFLAGS="${CXXFLAGS} ${WALL}"

dnl ============================================================================
dnl --SECTION--                                                          options
dnl ============================================================================

dnl ============================================================================
dnl --SECTION--                                               external libraries
dnl ============================================================================

dnl ----------------------------------------------------------------------------
dnl pthreads
dnl ----------------------------------------------------------------------------

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
dnl protobuf
dnl ----------------------------------------------------------------------------

AC_LANG(C++)

AC_CHECK_HEADERS(google/protobuf/stubs/common.h, [PROTOBUF=yes], [PROTOBUF=no])
AC_MSG_CHECKING([PROTOBUF])

if test "x$PROTOBUF" = "xno";  then
  AC_MSG_ERROR([Please install 'libprotobuf-dev'])
else
  AC_MSG_RESULT([found])
fi

AC_CHECK_TOOL([PROTOCOMPILER], [protoc], [], [])

if test "x$PROTOCOMPILER" = "x";  then
  AC_MSG_ERROR([Please install 'protobuf-compiler'])
fi

dnl ----------------------------------------------------------------------------
dnl glog
dnl ----------------------------------------------------------------------------

AC_LANG(C++)

AC_CHECK_HEADERS(glog/logging.h, [GLOG=yes], [GLOG=no])

AC_MSG_CHECKING([GLOG])

if test "x$GLOG" = "xno";  then
  AC_MSG_ERROR([Please install 'libgoogle-glog-dev'])
else
  AC_MSG_RESULT([found])
fi

dnl ----------------------------------------------------------------------------
dnl boost
dnl ----------------------------------------------------------------------------

AC_LANG(C++)

AC_CHECK_HEADERS(boost/lexical_cast.hpp, [BOOST=yes], [BOOST=no])

if test "x$BOOST" = "xno";  then
  AC_MSG_ERROR([Please install 'libboost-dev'])
fi

AC_CHECK_HEADERS(boost/regex.hpp, [BOOST=yes], [BOOST=no])

if test "x$BOOST" = "xno";  then
  AC_MSG_ERROR([Please install 'libboost-regex-dev'])
fi

LIBS="${LIBS} -lboost_regex"

dnl ----------------------------------------------------------------------------
dnl picojson.h
dnl ----------------------------------------------------------------------------

CPPFLAGS="${CPPFLAGS} -I`pwd`/${srcdir}/3rdParty/picojson"

dnl ----------------------------------------------------------------------------
dnl mesos
dnl ----------------------------------------------------------------------------

AC_LANG(C++)

topdir="${ac_pwd}"
MESOS_CPPFLAGS="-I${topdir}/mesos/include -I${topdir}/mesos/src -I${topdir}/mesos/build/include -I${topdir}/mesos/build/src -I${topdir}/mesos/3rdparty/libprocess/3rdparty/stout/include -I${topdir}/mesos/3rdparty/libprocess/include -I${topdir}/mesos/build/3rdparty/zookeeper-3.4.5/src/c/include -I${topdir}/mesos/build/3rdparty/zookeeper-3.4.5/src/c/generated"

MESOS_LIBS="-L${topdir}/mesos/build/src/.libs -lmesos_no_3rdparty"
MESOS_LIBS="${MESOS_LIBS} -L${topdir}/mesos/build/3rdparty/libprocess/.libs/ -lprocess"
MESOS_LIBS="${MESOS_LIBS} -L${topdir}/mesos/build/3rdparty/libprocess/3rdparty/libev-4.15/.libs -lev"
MESOS_LIBS="${MESOS_LIBS} ${topdir}/mesos/build/3rdparty/leveldb/libleveldb.a"
MESOS_LIBS="${MESOS_LIBS} -L${topdir}/mesos/build/3rdparty/zookeeper-3.4.5/src/c/.libs -lzookeeper_mt"
MESOS_LIBS="${MESOS_LIBS} -lprotobuf -lglog -lapr-1 -lsvn_delta-1 -lsvn_subr-1 -lcurl -lsasl2 -lz -ldl"

SAVE_CPPFLAGS="${CPPFLAGS}"
CPPFLAGS="${CPPFLAGS} ${MESOS_CPPFLAGS}"

AC_CHECK_HEADERS(mesos/resources.hpp, [MESOS=yes], [MESOS=no])

AC_MSG_CHECKING([MESOS])

if test "x$MESOS" = "xno";  then
  AC_MSG_ERROR([Please link mesos, i. e. 'ln -s ../mesos .'])
else
  AC_MSG_RESULT([found])
fi

AC_CHECK_HEADERS(logging/flags.hpp, [MESOS=yes], [MESOS=no])

if test "x$MESOS" = "xno";  then
  AC_MSG_ERROR([Please link mesos, i. e. 'ln -s ../mesos .'])
fi

CPPFLAGS="${SAVE_CPPFLAGS}"

AC_SUBST(MESOS_CPPFLAGS)
AC_SUBST(MESOS_LIBS)

dnl ----------------------------------------------------------------------------
dnl microhttpd
dnl ----------------------------------------------------------------------------

AC_LANG(C)

MICROHTTPD_CPPFLAGS=""
MICROHTTPD_LIBS="-lmicrohttpd"

AC_CHECK_HEADERS(microhttpd.h, [MICROHTTPD=yes], [MICROHTTPD=no])

AC_MSG_CHECKING([MICROHTTPD])

if test "x$MICROHTTPD" = "xno";  then
  AC_MSG_ERROR([Please install 'libmicrohttpd-dev'])
else
  AC_MSG_RESULT([found])
fi

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
