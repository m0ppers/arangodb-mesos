#include <libgen.h>

#include <iostream>
#include <string>

#include "ArangoScheduler.h"
#include "HttpServer.h"

#include <boost/lexical_cast.hpp>

#include <stout/check.hpp>
#include <stout/exit.hpp>
#include <stout/flags.hpp>
#include <stout/numify.hpp>
#include <stout/os.hpp>
#include <stout/stringify.hpp>

#include "common/type_utils.hpp"

#include "logging/flags.hpp"
#include "logging/logging.hpp"

using boost::lexical_cast;

using namespace std;
using namespace mesos;
using namespace arangodb;

////////////////////////////////////////////////////////////////////////////////
/// @brief prints help
////////////////////////////////////////////////////////////////////////////////

void usage (const char* argv0, const flags::FlagsBase& flags) {
  cerr << "Usage: " << os::basename(argv0).get() << " [...]" << "\n"
       << "\n"
       << "Supported options:" << "\n"
       << flags.usage();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief ArangoDB framework
////////////////////////////////////////////////////////////////////////////////

int main (int argc, char** argv) {

  // Find this executable's directory to locate executor.
  string path = os::realpath(dirname(argv[0])).get();
  string uri = path + "/arangodb-executor";
  if (getenv("ARANGODB_MESOS_DIR")) {
    uri = string(getenv("ARANGODB_MESOS_DIR")) + "/arangodb-executor";
  }

  mesos::internal::logging::Flags flags;

  string role;
  flags.add(&role,
            "role",
            "Role to use when registering",
            "*");

  Option<string> master;
  flags.add(&master,
            "master",
            "ip:port of master to connect");

  Try<Nothing> load = flags.load(None(), argc, argv);

  if (load.isError()) {
    cerr << load.error() << endl;
    usage(argv[0], flags);
    exit(1);
  }

  if (master.isNone()) {
    cerr << "Missing flag '--master'" << endl;
    usage(argv[0], flags);
    exit(1);
  }

  internal::logging::initialize(argv[0], flags, true); // Catch signals.

  ExecutorInfo executor;
  executor.mutable_executor_id()->set_value("default");
  executor.mutable_command()->set_value(uri);
  executor.set_name("Test Executor (C++)");
  executor.set_source("cpp_test");

  ArangoScheduler scheduler(executor, role);

  FrameworkInfo framework;
  framework.set_user(""); // Have Mesos fill in the current user.
  framework.set_name("Test Framework (C++)");
  framework.set_role(role);

  if (os::hasenv("MESOS_CHECKPOINT")) {
    framework.set_checkpoint(
        numify<bool>(os::getenv("MESOS_CHECKPOINT")).get());
  }

  MesosSchedulerDriver* driver;
  if (os::hasenv("MESOS_AUTHENTICATE")) {
    cout << "Enabling authentication for the framework" << endl;

    if (!os::hasenv("DEFAULT_PRINCIPAL")) {
      EXIT(1) << "Expecting authentication principal in the environment";
    }

    if (!os::hasenv("DEFAULT_SECRET")) {
      EXIT(1) << "Expecting authentication secret in the environment";
    }

    Credential credential;
    credential.set_principal(getenv("DEFAULT_PRINCIPAL"));
    credential.set_secret(getenv("DEFAULT_SECRET"));

    framework.set_principal(getenv("DEFAULT_PRINCIPAL"));

    driver = new MesosSchedulerDriver(
        &scheduler, framework, master.get(), credential);
  } else {
    framework.set_principal("test-framework-cpp");

    driver = new MesosSchedulerDriver(
        &scheduler, framework, master.get());
  }

  HttpServer http;
  http.start(8181);

  int status = driver->run() == DRIVER_STOPPED ? 0 : 1;

  http.stop();

  // Ensure that the driver process terminates.
  driver->stop();

  delete driver;
  return status;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------

// Local Variables:
// mode: outline-minor
// outline-regexp: "/// @brief\\|/// {@inheritDoc}\\|/// @page\\|// --SECTION--\\|/// @\\}"
// End:
