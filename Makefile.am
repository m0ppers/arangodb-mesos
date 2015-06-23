# -*- mode: Makefile; -*-

## -----------------------------------------------------------------------------
## --SECTION--                                                          protobuf
## -----------------------------------------------------------------------------

PROTOC = @PROTOCOMPILER@
PROTOCFLAGS = -I$(top_srcdir)/src -I$(srcdir) -I$(top_srcdir)/mesos/include

%.pb.cc %.pb.h: %.proto
	$(MKDIR_P) $(@D)
	$(PROTOC) $(PROTOCFLAGS) --cpp_out=$(@D) $^


## -----------------------------------------------------------------------------
## --SECTION--                                                             FILES
## -----------------------------------------------------------------------------

################################################################################
### @brief /bin programs
################################################################################

bin_PROGRAMS = \
	bin/arangodb-framework

################################################################################
### @brief auxiliary programs
################################################################################

noinst_PROGRAMS = \
        tst/res

################################################################################
### @brief auxiliary libraries
################################################################################

noinst_LIBRARIES = \
	lib/libarangodb-mesos.a

## -----------------------------------------------------------------------------
## --SECTION--                                                           library
## -----------------------------------------------------------------------------

################################################################################
### @brief source files
################################################################################

lib_libarangodb_mesos_a_SOURCES = \
	src/ArangoManager.cpp \
	src/ArangoScheduler.cpp \
	src/ArangoState.cpp \
	src/Caretaker.cpp \
	src/CaretakerStandalone.cpp \
	src/CaretakerReplication.cpp \
	src/Global.cpp \
	src/HttpServer.cpp \
	src/arangodb.pb.cc \
	src/utils.cpp \
	3rdParty/pbjson/src/pbjson.cpp

lib_libarangodb_mesos_a_CPPFLAGS = \
        -I@top_srcdir@ \
	-I@top_srcdir@/3rdParty/pbjson/src \
        $(AM_CPPFLAGS) \
	@MESOS_CPPFLAGS@ \
	@MICROHTTPD_CPPFLAGS@

## -----------------------------------------------------------------------------
## --SECTION--                                                         framework
## -----------------------------------------------------------------------------

################################################################################
### @brief source files
################################################################################

bin_arangodb_framework_SOURCES = \
	src/framework.cpp

bin_arangodb_framework_CPPFLAGS = \
        -I@top_srcdir@ \
        $(AM_CPPFLAGS) \
	@MESOS_CPPFLAGS@ \
	@MICROHTTPD_CPPFLAGS@

bin_arangodb_framework_LDADD = \
	lib/libarangodb-mesos.a \
	@MESOS_LIBS@ \
	@MICROHTTPD_LIBS@ \
	$(LIBS)

## -----------------------------------------------------------------------------
## --SECTION--                                                             tests
## -----------------------------------------------------------------------------

################################################################################
### @brief source files
################################################################################

tst_res_SOURCES = \
	tst/res.cpp

tst_res_CPPFLAGS = \
        -I@top_srcdir@ \
        $(AM_CPPFLAGS) \
	@MESOS_CPPFLAGS@

tst_res_LDADD = \
	@MESOS_LIBS@ \
	$(LIBS)

## -----------------------------------------------------------------------------
## --SECTION--                                                       END-OF-FILE
## -----------------------------------------------------------------------------

## Local Variables:
## mode: outline-minor
## outline-regexp: "### @brief\\|## --SECTION--\\|# -\\*-" 
## End:
