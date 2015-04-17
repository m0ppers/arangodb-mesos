////////////////////////////////////////////////////////////////////////////////
/// @brief ArangoDB Mesos Framework
///
/// @file
///
/// DISCLAIMER
///
/// Copyright 2015 ArangoDB GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Dr. Frank Celler
/// @author Copyright 2015, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include <libgen.h>

#include <iostream>
#include <string>

#include "ArangoScheduler.h"
// #include "ArangoState.h"
#include "HttpServer.h"

#include <stout/check.hpp>
#include <stout/exit.hpp>
#include <stout/flags.hpp>
#include <stout/numify.hpp>
#include <stout/os.hpp>
#include <stout/stringify.hpp>
#include <stout/net.hpp>

#include "logging/flags.hpp"
#include "logging/logging.hpp"

using namespace std;
using namespace mesos::internal;
using namespace arangodb;

// -----------------------------------------------------------------------------
// --SECTION--                                                 private functions
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief prints help
////////////////////////////////////////////////////////////////////////////////

static void usage (const char* argv0, const flags::FlagsBase& flags) {
  cerr << "Usage: " << os::basename(argv0).get() << " [...]" << "\n"
       << "\n"
       << "Supported options:" << "\n"
       << flags.usage();
}

// -----------------------------------------------------------------------------
// --SECTION--                                                  public functions
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief ArangoDB framework
////////////////////////////////////////////////////////////////////////////////

int main (int argc, char** argv) {

  // .............................................................................
  // command line options
  // .............................................................................

  // find this executable's directory to locate executor
  string path = os::realpath(dirname(argv[0])).get();
  string uri = path + "/arangodb-executor";

  if (getenv("ARANGODB_MESOS_DIR")) {
    uri = string(getenv("ARANGODB_MESOS_DIR")) + "/arangodb-executor";
  }

  // parse the command line flags
  logging::Flags flags;

  string role;
  flags.add(&role,
            "role",
            "Role to use when registering",
            "arangodb");

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

  logging::initialize(argv[0], flags, true); // Catch signals.

  // .............................................................................
  // executor
  // .............................................................................

  // create the executors
  ExecutorInfo executor;
  executor.mutable_executor_id()->set_value("arangodb:executor");
  executor.mutable_command()->set_value(uri);
  executor.set_name("arangodb:executor");
  executor.set_source("arangodb");
  *executor.mutable_resources() = Resources();

  // .............................................................................
  // framework
  // .............................................................................

  // create the framework
  FrameworkInfo framework;
  framework.set_user(""); // Have Mesos fill in the current user.
  framework.set_name("ArangoDB Framework");
  framework.set_role(role);
  framework.set_checkpoint(true);

  // .............................................................................
  // state
  // .............................................................................

/*
  ArangoState state("arangodb");
  state.init();
  state.load();
*/

  // .............................................................................
  // http server
  // .............................................................................

  Try<string> hostnameTry = net::hostname();
  string hostname = hostnameTry.get();

  int port = 8181;
  
  string url = "http://" + hostname + ":" + to_string(port) + "/index.html";

  framework.set_webui_url(url);

  if (os::hasenv("MESOS_CHECKPOINT")) {
    framework.set_checkpoint(
        numify<bool>(os::getenv("MESOS_CHECKPOINT")).get());
  }

  // .............................................................................
  // scheduler
  // .............................................................................

  string principal = "arangodb";

  // create the scheduler
  ArangoScheduler scheduler(role, principal, executor);

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
    framework.set_principal(principal);

    driver = new MesosSchedulerDriver(
        &scheduler, framework, master.get());
  }

  scheduler.setDriver(driver);

  // .............................................................................
  // run
  // .............................................................................

  // and the http server
  HttpServer http(scheduler.manager());

  // start and wait
  http.start(port);

  int status = driver->run() == DRIVER_STOPPED ? 0 : 1;

  http.stop();

  // ensure that the driver process terminates
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
