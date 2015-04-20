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
#include "ArangoState.h"
#include "Global.h"
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

static void usage (const string& argv0, const flags::FlagsBase& flags) {
  cerr << "Usage: " << argv0 << " [...]" << "\n"
       << "\n"
       << "Supported options:" << "\n"
       << flags.usage() << "\n"
       << "Supported environment:" << "\n"
       << "  MESOS_AUTHENTICATE   enable authentication\n"
       << "  ARANGODB_PRINCIPAL   principal for authentication\n"
       << "  ARANGODB_SECRET      secret for authentication\n"
       << "\n"
       << "  MESOS_MASTER         overrides '--master'\n"
       << "  ARANGODB_ROLE        overrides '--role'\n"
       << "  ARANGODB_ZK          overrides '--zk'\n"
       << "\n";
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

  string argv0 = argv[0];
  string path = os::realpath(dirname(argv[0])).get();
  string uri = path + "/arangodb-executor";

  if (getenv("ARANGODB_MESOS_DIR")) {
    uri = string(getenv("ARANGODB_MESOS_DIR")) + "/arangodb-executor";
  }

  // parse the command line flags
  logging::Flags flags;

  string mode;
  flags.add(&mode,
            "mode",
            "Mode of operation (standalone)",
            "standalone");

  string role;
  flags.add(&role,
            "role",
            "Role to use when registering",
            "arangodb");

  string principal;
  flags.add(&principal,
            "principal",
            "Principal for authentication and persistent volumes",
            "arangodb");

  string frameworkName;
  flags.add(&frameworkName,
            "framework-name",
            "custom framework name",
            "ArangoDB Framework");

  string zk;
  flags.add(&zk,
            "zk",
            "zookeeper for state",
            "");

  double failoverTimeout;
  flags.add(&failoverTimeout,
            "failover-timeout",
            "failover timeout in seconds",
            60 * 60 * 24 * 10);

  Option<string> master;
  flags.add(&master,
            "master",
            "ip:port of master to connect");

  Try<Nothing> load = flags.load(None(), argc, argv);

  if (load.isError()) {
    cerr << load.error() << endl;
    usage(argv0, flags);
    exit(1);
  }

  if (os::hasenv("MESOS_MASTER")) {
    master = getenv("MESOS_MASTER");
  }

  if (os::hasenv("ARANGODB_ROLE")) {
    role = getenv("ARANGODB_ROLE");
  }

  if (os::hasenv("ARANGODB_ZK")) {
    zk = getenv("ARANGODB_ZK");
  }

  if (master.isNone()) {
    cerr << "Missing flag '--master'" << endl;
    usage(argv[0], flags);
    exit(1);
  }

  logging::initialize(argv[0], flags, true); // Catch signals.

  if (mode == "standalone") {
    Global::setMode(OperationMode::STANDALONE);
  }
  else {
    cerr << argv0 << ": expecting mode '" << mode << "' to be "
         << "standalone" << "\n";
  }

  // .............................................................................
  // executor
  // .............................................................................

  // create the executors
  ExecutorInfo executor;
  executor.mutable_executor_id()->set_value("arangodb_executor");
  executor.mutable_command()->set_value(uri);
  executor.set_name("arangodb_executor");
  executor.set_source("arangodb");
  *executor.mutable_resources() = Resources();

  // .............................................................................
  // state
  // .............................................................................

  LOG(INFO) << "zookeeper: " << zk;

  ArangoState state(role, zk);
  state.init();
  state.load();

  Global::setState(&state);

  // .............................................................................
  // framework
  // .............................................................................

  // create the framework
  FrameworkInfo framework;
  framework.set_user(""); // Have Mesos fill in the current user.
  framework.set_checkpoint(true);

  framework.set_name(frameworkName);
  LOG(INFO) << "framework name: " << frameworkName;

  framework.set_role(role);
  LOG(INFO) << "role: " << role;

  if (0.0 < failoverTimeout) {
    framework.set_failover_timeout(failoverTimeout);
  }
  else {
    failoverTimeout = 0.0;
  }

  LOG(INFO) << "failover timeout: " << failoverTimeout;

  framework.mutable_id()->CopyFrom(Global::state().frameworkId());

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
  // global options
  // .............................................................................

  Global::setRole(role);
  Global::setPrincipal(role);

  // .............................................................................
  // scheduler
  // .............................................................................

  // create the scheduler
  ArangoScheduler scheduler(role, principal, executor);

  MesosSchedulerDriver* driver;

  if (os::hasenv("MESOS_AUTHENTICATE")) {
    cout << "Enabling authentication for the framework" << endl;

    if (!os::hasenv("ARANGODB_PRINCIPAL")) {
      EXIT(1) << "Expecting authentication principal in the environment";
    }

    if (!os::hasenv("ARANGODB_SECRET")) {
      EXIT(1) << "Expecting authentication secret in the environment";
    }

    Credential credential;
    credential.set_principal(getenv("ARANGODB_PRINCIPAL"));
    credential.set_secret(getenv("ARANGODB_SECRET"));

    framework.set_principal(getenv("ARANGODB_PRINCIPAL"));
    driver = new MesosSchedulerDriver(&scheduler, framework, master.get(), credential);
  }
  else {
    framework.set_principal(principal);
    driver = new MesosSchedulerDriver(&scheduler, framework, master.get());
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
