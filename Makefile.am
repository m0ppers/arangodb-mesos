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
	bin/arangodb-framework \
	bin/arangodb-executor

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
	src/arangodb.pb.cc \
	src/ArangoManager.cpp \
	src/ArangoScheduler.cpp \
	src/HttpServer.cpp \
	src/utils.cpp

lib_libarangodb_mesos_a_CPPFLAGS = \
        -I@top_srcdir@ \
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
## --SECTION--                                                          executor
## -----------------------------------------------------------------------------

################################################################################
### @brief source files
################################################################################

bin_arangodb_executor_SOURCES = \
	src/executor.cpp

bin_arangodb_executor_CPPFLAGS = \
        -I@top_srcdir@ \
        $(AM_CPPFLAGS) \
	@MESOS_CPPFLAGS@

bin_arangodb_executor_LDADD = \
	lib/libarangodb-mesos.a \
	@MESOS_LIBS@ \
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
