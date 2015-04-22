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

#include "ArangoManager.h"
#include "ArangoScheduler.h"
#include "ArangoState.h"
#include "CaretakerStandalone.h"
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
       << "  ARANGODB_SECRET      secret for authentication\n"
       << "\n"
       << "  ARANGODB_PRINCIPAL   overrides '--principal'\n"
       << "  ARANGODB_HTTP_PORT   overrides '--http-port'\n"
       << "  ARANGODB_ROLE        overrides '--role'\n"
       << "  ARANGODB_USER        overrides '--user'\n"
       << "  ARANGODB_WEBUI       overrides '--webui'\n"
       << "  ARANGODB_ZK          overrides '--zk'\n"
       << "\n"
       << "  MESOS_MASTER         overrides '--master'\n"
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
            "Principal for persistent volumes",
            "arangodb");

  string frameworkUser;
  flags.add(&frameworkUser,
            "user",
            "User for the framework",
            "");

  string frameworkName;
  flags.add(&frameworkName,
            "framework-name",
            "custom framework name",
            "ArangoDB Framework");

  string webui;
  flags.add(&webui,
            "webui",
            "URL to advertise for external access to the UI",
            "");

  int webuiPort;
  flags.add(&webuiPort,
            "http-port",
            "HTTP port to open for UI",
            8181);

  double failoverTimeout;
  flags.add(&failoverTimeout,
            "failover-timeout",
            "failover timeout in seconds",
            60 * 60 * 24 * 10);

  // address of master and zookeeper
  string master;
  flags.add(&master,
            "master",
            "ip:port of master to connect",
            "");

  string zk;
  flags.add(&zk,
            "zk",
            "zookeeper for state",
            "");

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

  if (os::hasenv("ARANGODB_USER")) {
    frameworkUser = getenv("ARANGODB_user");
  }

  if (os::hasenv("ARANGODB_ZK")) {
    zk = getenv("ARANGODB_ZK");
  }

  if (os::hasenv("ARANGODB_WEBUI")) {
    webui = getenv("ARANGODB_WEBUI");
  }

  if (os::hasenv("ARANGODB_HTTP_PORT")) {
    webuiPort = atoi(getenv("ARANGODB_HTTP_PORT"));
  }

  if (os::hasenv("ARANGODB_PRINCIPAL")) {
    principal = getenv("ARANGODB_PRINCIPAL");
  }

  if (master.empty()) {
    cerr << "Missing master, either use flag '--master' or set 'MESOS_MASTER'" << endl;
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
  framework.set_user(frameworkUser);
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

  bool found;
  mesos::FrameworkID frameworkId= Global::state().frameworkId(found);

  if (found) {
    framework.mutable_id()->CopyFrom(frameworkId);
  }

  // .............................................................................
  // http server
  // .............................................................................

  if (webui.empty()) {
    Try<string> hostnameTry = net::hostname();
    string hostname = hostnameTry.get();

    webui = "http://" + hostname + ":" + to_string(webuiPort) + "/index.html";
  }

  LOG(INFO) << "webui url: " << webui;

  framework.set_webui_url(webui);

  if (os::hasenv("MESOS_CHECKPOINT")) {
    framework.set_checkpoint(numify<bool>(os::getenv("MESOS_CHECKPOINT")).get());
  }

  // .............................................................................
  // global options
  // .............................................................................

  Global::setRole(role);
  Global::setPrincipal(principal);

  // .............................................................................
  // Caretaker
  // .............................................................................

  unique_ptr<Caretaker> caretaker;

  switch (Global::mode()) {
    case OperationMode::STANDALONE:
      caretaker.reset(new CaretakerStandalone);
  }

  Global::setCaretaker(caretaker.get());

  // .............................................................................
  // manager
  // .............................................................................

  ArangoManager manager(role, principal);
  Global::setManager(&manager);

  // .............................................................................
  // scheduler
  // .............................................................................

  // create the scheduler
  ArangoScheduler scheduler(role, principal, executor);

  MesosSchedulerDriver* driver;

  if (os::hasenv("MESOS_AUTHENTICATE")) {
    cout << "Enabling authentication for the framework" << endl;

    if (principal.empty()) {
      EXIT(1) << "Expecting authentication principal in the environment";
    }

    if (!os::hasenv("ARANGODB_SECRET")) {
      EXIT(1) << "Expecting authentication secret in the environment";
    }

    Credential credential;
    credential.set_principal(principal);
    credential.set_secret(getenv("ARANGODB_SECRET"));

    framework.set_principal(principal);
    driver = new MesosSchedulerDriver(&scheduler, framework, master, credential);
  }
  else {
    framework.set_principal(principal);
    driver = new MesosSchedulerDriver(&scheduler, framework, master);
  }

  scheduler.setDriver(driver);

  Global::setScheduler(&scheduler);

  // .............................................................................
  // run
  // .............................................................................

  // and the http server
  HttpServer http;

  // start and wait
  LOG(INFO) << "http port: " << webuiPort;
  http.start(webuiPort);

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
