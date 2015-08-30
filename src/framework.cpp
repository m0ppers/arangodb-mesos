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
#include "CaretakerCluster.h"
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
/// @brief update from env
////////////////////////////////////////////////////////////////////////////////

static void updateFromEnv (const string& name, string& var) {
  Option<string> env = os::getenv(name);

  if (env.isSome()) {
    var = env.get();
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief update from env
////////////////////////////////////////////////////////////////////////////////

static void updateFromEnv (const string& name, int& var) {
  Option<string> env = os::getenv(name);

  if (env.isSome()) {
    var = atoi(env.get().c_str());
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief update from env
////////////////////////////////////////////////////////////////////////////////

static void updateFromEnv (const string& name, double& var) {
  Option<string> env = os::getenv(name);

  if (env.isSome()) {
    var = atof(env.get().c_str());
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief update from env
////////////////////////////////////////////////////////////////////////////////

static void updateFromEnv (const string& name, bool& var) {
  Option<string> env = os::getenv(name);

  if (env.isSome()) {
    var = env.get() == "true";
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief prints help
////////////////////////////////////////////////////////////////////////////////

static void usage (const string& argv0, const flags::FlagsBase& flags) {
  cerr << "Usage: " << argv0 << " [...]" << "\n"
       << "\n"
       << "Supported options:" << "\n"
       << flags.usage() << "\n"
       << "Supported environment:" << "\n"
       << "  MESOS_MASTER         overrides '--master'\n"
       << "\n"
       << "  MESOS_AUTHENTICATE   enable authentication\n"
       << "  ARANGODB_SECRET      secret for authentication\n"
       << "\n"
       << "  ARANGODB_PRINCIPAL   overrides '--principal'\n"
       << "  ARANGODB_HTTP_PORT   overrides '--http_port'\n"
       << "  ARANGODB_ROLE        overrides '--role'\n"
       << "  ARANGODB_USER        overrides '--user'\n"
       << "  ARANGODB_VOLUME_PATH overrides '--volume_path'\n"
       << "  ARANGODB_WEBUI       overrides '--webui'\n"
       << "  ARANGODB_ZK          overrides '--zk'\n"
       << "  ARANGODB_MODE        overrides '--mode'\n"
       << "  ARANGODB_ASYNC_REPLICATION\n"
          "                       overrides '--async_replication'\n"
       << "  ARANGODB_FRAMEWORK_NAME\n"
       << "                       overrides '--framework_name'\n"
       << "  ARANGODB_MINIMAL_RESOURCES_AGENT\n"
       << "                       overrides '--minimal_resources_agent'\n"
       << "  ARANGODB_MINIMAL_RESOURCES_DBSERVER\n"
          "                       overrides '--minimal_resources_dbserver'\n"
       << "  ARANGODB_MINIMAL_RESOURCES_SECONDARY\n"
          "                       overrides '--minimal_resources_secondary'\n"
       << "  ARANGODB_MINIMAL_RESOURCES_COORDINATOR\n"
          "                       overrides '--minimal_resources_coordinator'\n"
       << "  ARANGODB_NR_AGENTS   overrides '--nr_agents'\n"
       << "  ARANGODB_NR_DBSERVERS\n"
       << "                       overrides '--nr_dbservers'\n"
       << "  ARANGODB_NR_COORDINATORS\n"
       << "                       overrides '--nr_coordinators'\n"
       << "\n";
}

// -----------------------------------------------------------------------------
// --SECTION--                                                  public functions
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief ArangoDB framework
////////////////////////////////////////////////////////////////////////////////

int main (int argc, char** argv) {

  // ...........................................................................
  // command line options
  // ...........................................................................

  // parse the command line flags
  logging::Flags flags;

  string mode;
  flags.add(&mode,
            "mode",
            "Mode of operation (standalone, cluster)",
            "cluster");

  string async_repl;
  flags.add(&async_repl,
            "async_replication",
            "Flag, whether we run secondaries for asynchronous replication",
            "false");

  string role;
  flags.add(&role,
            "role",
            "Role to use when registering",
            "*");

  string minimal_resources_agent;
  flags.add(&minimal_resources_agent,
            "minimal_resources_agent",
            "Minimal resources to accept for an agent",
            "");

  string minimal_resources_dbserver;
  flags.add(&minimal_resources_dbserver,
            "minimal_resources_dbserver",
            "Minimal resources to accept for a DBServer",
            "");

  string minimal_resources_secondary;
  flags.add(&minimal_resources_secondary,
            "minimal_resources_secondary",
            "Minimal resources to accept for a secondary DBServer",
            "");

  string minimal_resources_coordinator;
  flags.add(&minimal_resources_coordinator,
            "minimal_resources_coordinator",
            "Minimal resources to accept for a coordinator",
            "");

  int nragents;
  flags.add(&nragents,
            "nr_agents",
            "Number of agents in agency (etcd)",
            1);

  int nrdbservers;
  flags.add(&nrdbservers,
            "nr_dbservers",
            "Initial number of DBservers in cluster",
            2);

  int nrcoordinators;
  flags.add(&nrcoordinators,
            "nr_coordinators",
            "Initial number of coordinators in cluster",
            1);

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
            "framework_name",
            "custom framework name",
            "arangodb");

  string webui;
  flags.add(&webui,
            "webui",
            "URL to advertise for external access to the UI",
            "");

  int webuiPort;
  flags.add(&webuiPort,
            "http_port",
            "HTTP port to open for UI",
            8181);

  double failoverTimeout;
  flags.add(&failoverTimeout,
            "failover_timeout",
            "failover timeout in seconds",
            60 * 60 * 24 * 10);

  string volumePath;
  flags.add(&volumePath,
            "volume_path",
            "volume path (until persistent volumes become available)",
            "/tmp");

  bool resetState;
  flags.add(&resetState,
            "reset_state",
            "ignore any old state",
            false);

  bool coordinatorsPublic;
  flags.add(&coordinatorsPublic,
            "coordinators_public",
            "run coordinator tasks on public agents",
            false);

  bool secondariesWithDBservers;
  flags.add(&secondariesWithDBservers,
            "secondaries_with_dbservers",
            "run secondaries only on agents with DBservers",
            false);

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
    usage(argv[0], flags);
    exit(EXIT_FAILURE);
  }

  if (flags.help) {
    usage(argv[0], flags);
    exit(EXIT_SUCCESS);
  }

  updateFromEnv("ARANGODB_MODE", mode);
  updateFromEnv("ARANGODB_ASYNC_REPLICATION", async_repl);
  updateFromEnv("ARANGODB_ROLE", role);
  updateFromEnv("ARANGODB_MINIMAL_RESOURCES_AGENT", minimal_resources_agent);
  updateFromEnv("ARANGODB_MINIMAL_RESOURCES_DBSERVER", minimal_resources_dbserver);
  updateFromEnv("ARANGODB_MINIMAL_RESOURCES_SECONDARY",  minimal_resources_secondary);
  updateFromEnv("ARANGODB_MINIMAL_RESOURCES_COORDINATOR", minimal_resources_coordinator);
  updateFromEnv("ARANGODB_NR_AGENTS", nragents);

  if (nragents != 1) {
    nragents = 1;
  }

  updateFromEnv("ARANGODB_NR_DBSERVERS", nrdbservers);

  if (nrdbservers < 1) {
    nrdbservers = 1;
  }

  updateFromEnv("ARANGODB_NR_COORDINATORS", nrcoordinators);

  if (nrcoordinators < 1) {
    nrcoordinators = 1;
  }

  updateFromEnv("ARANGODB_PRINCIPAL", principal);
  updateFromEnv("ARANGODB_USER", frameworkUser);
  updateFromEnv("ARANGODB_FRAMEWORK_NAME", frameworkName);
  updateFromEnv("ARANGODB_WEBUI", webui);
  updateFromEnv("ARANGODB_HTTP_PORT", webuiPort);
  updateFromEnv("ARANGODB_FAILOVER_TIMEOUT", failoverTimeout);
  updateFromEnv("ARANGODB_VOLUME_PATH", volumePath);
  updateFromEnv("ARANGODB_RESET_STATE", resetState);
  updateFromEnv("ARANGODB_COORDINATORS_PUBLIC", coordinatorsPublic);
  updateFromEnv("ARANGODB_SECONDARIES_WITH_DBSERVERS", secondariesWithDBservers);

  updateFromEnv("MESOS_MASTER", master);
  updateFromEnv("ARANGODB_ZK", zk);

  if (master.empty()) {
    cerr << "Missing master, either use flag '--master' or set 'MESOS_MASTER'" << endl;
    usage(argv[0], flags);
    exit(EXIT_FAILURE);
  }

  logging::initialize(argv[0], flags, true); // Catch signals.

  if (mode == "standalone") {
    Global::setMode(OperationMode::STANDALONE);
  }
  else if (mode == "cluster") {
    Global::setMode(OperationMode::CLUSTER);
  }
  else {
    cerr << argv[0] << ": expecting mode '" << mode << "' to be "
         << "standalone, cluster" << "\n";
  }
  LOG(INFO) << "Mode: " << mode;

  if (async_repl == "yes" || async_repl == "true" || async_repl == "y") {
    Global::setAsyncReplication(true);
  }
  else {
    Global::setAsyncReplication(false);
  }
  LOG(INFO) << "Asynchronous replication flag: " << Global::asyncReplication();

  Global::setFrameworkName(frameworkName);
  Global::setVolumePath(volumePath);

  Global::setCoordinatorsPublic(coordinatorsPublic);
  Global::setSecondariesWithDBservers(secondariesWithDBservers);

  LOG(INFO) << "Minimal resources agent: " << minimal_resources_agent;
  Global::setMinResourcesAgent(minimal_resources_agent);
  LOG(INFO) << "Minimal resources DBserver: " << minimal_resources_dbserver;
  Global::setMinResourcesDBServer(minimal_resources_dbserver);
  LOG(INFO) << "Minimal resources secondary DBserver: " 
            << minimal_resources_secondary;
  Global::setMinResourcesSecondary(minimal_resources_secondary);
  LOG(INFO) << "Minimal resources coordinator: " 
            << minimal_resources_coordinator;
  Global::setMinResourcesCoordinator(minimal_resources_coordinator);
  LOG(INFO) << "Number of agents in agency: " << nragents;
  Global::setNrAgents(nragents);
  LOG(INFO) << "Number of DBservers: " << nrdbservers;
  Global::setNrDBServers(nrdbservers);
  LOG(INFO) << "Number of coordinators: " << nrcoordinators;
  Global::setNrCoordinators(nrcoordinators);

  // ...........................................................................
  // state
  // ...........................................................................

  LOG(INFO) << "zookeeper: " << zk;

  ArangoState state(frameworkName, zk);
  state.init();

  if (resetState) {
    state.destroy();
  }
  else {
    state.load();
  }

  Global::setState(&state);

  // ...........................................................................
  // framework
  // ...........................................................................

  // create the framework
  mesos::FrameworkInfo framework;
  framework.set_user(frameworkUser);
  LOG(INFO) << "framework user: " << frameworkUser;
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

  // ...........................................................................
  // http server
  // ...........................................................................

  if (webui.empty()) {
    Try<string> hostnameTry = net::hostname();
    string hostname = hostnameTry.get();

    webui = "http://" + hostname + ":" + to_string(webuiPort);
  }

  LOG(INFO) << "webui url: " << webui;

  framework.set_webui_url(webui);

  Option<string> mesosCheckpoint =  os::getenv("MESOS_CHECKPOINT");

  if (mesosCheckpoint.isSome()) {
    framework.set_checkpoint(numify<bool>(mesosCheckpoint).get());
  }

  // ...........................................................................
  // global options
  // ...........................................................................

  Global::setRole(role);
  Global::setPrincipal(principal);

  // ...........................................................................
  // Caretaker
  // ...........................................................................

  unique_ptr<Caretaker> caretaker;

  switch (Global::mode()) {
    case OperationMode::STANDALONE:
      caretaker.reset(new CaretakerStandalone);
      break;

    case OperationMode::CLUSTER:
      caretaker.reset(new CaretakerCluster);
      break;
  }

  Global::setCaretaker(caretaker.get());

  // ...........................................................................
  // manager
  // ...........................................................................

  ArangoManager* manager = ArangoManager::New();
  Global::setManager(manager);

  // ...........................................................................
  // scheduler
  // ...........................................................................

  // create the scheduler
  ArangoScheduler scheduler;

  mesos::MesosSchedulerDriver* driver;

  Option<string> mesosAuthenticate = os::getenv("MESOS_AUTHENTICATE");

  if (mesosAuthenticate.isSome() && mesosAuthenticate.get() == "true") {
    cout << "Enabling authentication for the framework" << endl;

    if (principal.empty()) {
      EXIT(EXIT_FAILURE) << "Expecting authentication principal in the environment";
    }

    Option<string> arangodbSecret = os::getenv("ARANGODB_SECRET");

    if (arangodbSecret.isNone()) {
      EXIT(EXIT_FAILURE) << "Expecting authentication secret in the environment";
    }

    mesos::Credential credential;
    credential.set_principal(principal);
    credential.set_secret(arangodbSecret.get());

    framework.set_principal(principal);
    driver = new mesos::MesosSchedulerDriver(&scheduler, framework, master, credential);
  }
  else {
    framework.set_principal(principal);
    driver = new mesos::MesosSchedulerDriver(&scheduler, framework, master);
  }

  scheduler.setDriver(driver);

  Global::setScheduler(&scheduler);

  // ...........................................................................
  // run
  // ...........................................................................

  // and the http server
  HttpServer http;

  // start and wait
  LOG(INFO) << "http port: " << webuiPort;
  http.start(webuiPort);

  int status = driver->run() == mesos::DRIVER_STOPPED ? 0 : 1;

  // ensure that the driver process terminates
  driver->stop();

  delete driver;
  delete manager;

  sleep(120);   // Wait some more time before terminating the process to
                // allow the user to use 
                //   dcos package uninstall arangodb
                // to remove the Marathon job
  http.stop();

  return status;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
