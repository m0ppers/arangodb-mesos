///////////////////////////////////////////////////////////////////////////////
/// @brief standalone caretaker
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

#include "CaretakerStandalone.h"

#include <chrono>

#include "ArangoState.h"
#include "Global.h"

#include "arangodb.pb.h"

using namespace arangodb;
using namespace std;

// -----------------------------------------------------------------------------
// --SECTION--                                                   class Caretaker
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

CaretakerStandalone::CaretakerStandalone () {
  Targets targets = Global::state().targets();

  // AGENCY
  Target* agency = targets.mutable_agents();
  agency->set_instances(0);
  agency->clear_minimal_resources();

  // COORDINATOR
  Target* coordinator = targets.mutable_coordinators();
  coordinator->set_instances(0);
  coordinator->clear_minimal_resources();

  // DBSERVER
  Target* dbserver = targets.mutable_dbservers();
  dbserver->set_instances(Global::nrDBServers());
  dbserver->clear_minimal_resources();
  dbserver->set_number_ports(1);

  if (Global::minResourcesDBServer().empty()) {
    setStandardMinimum(dbserver, 1);
  }
  else {
    Try<mesos::Resources> x
        = mesos::Resources::parse(Global::minResourcesDBServer());
    if (x.isError()) {
      LOG(ERROR) << "cannot parse minimum resources for DBServer:\n  '"
                 << Global::minResourcesDBServer() << "'";
      setStandardMinimum(dbserver, 1);
    }
    else {
      auto m = dbserver->mutable_minimal_resources();
      m->CopyFrom(x.get());
    }
  }

  // SECONDARIES
  Target* secondaries = targets.mutable_secondaries();
  secondaries->set_instances(0);
  secondaries->clear_minimal_resources();

  Global::state().setTargets(targets);
}

// -----------------------------------------------------------------------------
// --SECTION--                                            virtual public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// {@inheritDoc}
////////////////////////////////////////////////////////////////////////////////

void CaretakerStandalone::updatePlan () {
  Targets targets = Global::state().targets();
  Plan plan = Global::state().plan();
  Current current = Global::state().current();

  // need exactly one DB server
  int t = (int) targets.dbservers().instances();

  if (t != 1) {
    LOG(ERROR)
    << "FATAL running in standalone mode, exactly one db-server is supported, got " << t;

    exit(EXIT_FAILURE);
  }

  TasksPlan* dbservers = plan.mutable_dbservers();
  int p = dbservers->entries_size();

  if (1 < p) {
    LOG(ERROR)
    << "ERROR running in standalone mode, but got " << p << " db-servers";

    TaskPlan entry = dbservers->entries(0);

    dbservers->clear_entries();
    dbservers->add_entries()->CopyFrom(entry);
  }

  else if (p < 1) {
    LOG(INFO)
    << "DEBUG creating one db-server in plan";

    double now = chrono::duration_cast<chrono::seconds>(
      chrono::steady_clock::now().time_since_epoch()).count();

    TaskPlan* task = dbservers->add_entries();
    task->set_state(TASK_STATE_NEW);
    task->set_started(now);
    task->set_is_primary(true);

    current.mutable_primary_dbserver_resources()->add_entries();

    auto instance = current.mutable_primary_dbservers()->add_entries();
    instance->set_state(INSTANCE_STATE_UNUSED);
  }

  Global::state().setPlan(plan);
  Global::state().setCurrent(current);
}

////////////////////////////////////////////////////////////////////////////////
/// {@inheritDoc}
////////////////////////////////////////////////////////////////////////////////

InstanceAction CaretakerStandalone::checkInstance () {
  Plan plan = Global::state().plan();
  Current current = Global::state().current();

  auto res = checkStartInstance(
    AspectType::PRIMARY_DBSERVER,
    InstanceActionState::START_PRIMARY_DBSERVER,
    plan.mutable_dbservers(),
    current.mutable_primary_dbserver_resources(),
    current.mutable_primary_dbservers());

  Global::state().setPlan(plan);
  Global::state().setCurrent(current);

  return res;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
