///////////////////////////////////////////////////////////////////////////////
/// @brief cluster caretaker
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

#include "CaretakerCluster.h"

#include "ArangoState.h"
#include "Global.h"

#include "arangodb.pb.h"

using namespace arangodb;

// -----------------------------------------------------------------------------
// --SECTION--                                                   class Caretaker
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

CaretakerCluster::CaretakerCluster () {
  mesos::Resource* m;

  Target target = Global::state().target();

  // AGENCY
  TargetEntry* agency = target.mutable_agents();
  agency->set_instances(1);
  agency->clear_minimal_resources();
  agency->set_number_ports(1);
  // FIXME: make minimal resources for AGENCY configurable via command line
  m = agency->add_minimal_resources();
  m->set_role("*");
  m->set_name("cpus");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(1);

  m = agency->add_minimal_resources();
  m->set_role("*");
  m->set_name("mem");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(512);

  m = agency->add_minimal_resources();
  m->set_role("*");
  m->set_name("disk");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(512);

  // COORDINATOR
  TargetEntry* coordinator = target.mutable_coordinators();
  coordinator->set_instances(1);
  coordinator->clear_minimal_resources();
  coordinator->set_number_ports(1);
  // FIXME: make minimal resources for COORDINATORS configurable via command line
  m = coordinator->add_minimal_resources();
  m->set_role("*");
  m->set_name("cpus");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(1);

  m = coordinator->add_minimal_resources();
  m->set_role("*");
  m->set_name("mem");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(512);

  m = coordinator->add_minimal_resources();
  m->set_role("*");
  m->set_name("disk");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(512);

  // DBSERVER
  TargetEntry* dbserver = target.mutable_dbservers();
  dbserver->set_instances(2);
  dbserver->clear_minimal_resources();
  dbserver->set_number_ports(1);

  // FIXME: make minimal resources for DBSERVERS configurable via command line
  m = dbserver->add_minimal_resources();
  m->set_role("*");
  m->set_name("cpus");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(1);

  m = dbserver->add_minimal_resources();
  m->set_role("*");
  m->set_name("mem");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(1024);

  m = dbserver->add_minimal_resources();
  m->set_role("*");
  m->set_name("disk");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(1024);

  Global::state().setTarget(target);
}

// -----------------------------------------------------------------------------
// --SECTION--                                            virtual public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// {@inheritDoc}
////////////////////////////////////////////////////////////////////////////////

void CaretakerCluster::updatePlan () {
  // This updates the Plan according to what is in the Target, this is
  // used to scale up coordinators or DBServers

  Target target = Global::state().target();
  Plan plan = Global::state().plan();
  Current current = Global::state().current();
  int t, p;

  // First the agency, currently, we only support a single agency:
  t = (int) target.agents().instances();
  if (t < 1) {
    LOG(ERROR)
    << "ERROR running in cluster mode, need at least one agency";

    exit(EXIT_FAILURE);
  }
  if (t > 1) {
    LOG(INFO)
    << "INFO currently we support only a single server agency";
    t = 1;
    TargetEntry* te = target.mutable_agents();
    te->set_instances(1);
  }
  TasksPlan* tasks = plan.mutable_agents();
  p = tasks->entries_size();
  if (t < p) {
    LOG(INFO)
    << "INFO reducing number of agents from " << p << " to " << t;

    TasksPlan original;
    original.CopyFrom(*tasks);
    
    tasks->clear_entries();

    for (int i = 0;  i < t;  ++i) {
      TasksPlanEntry entry = original.entries(i);

      tasks->add_entries()->CopyFrom(entry);
    }
  }
  if (p < t) {
    LOG(INFO)
    << "DEBUG creating " << (t - p) << " more agents in plan";

    for (int i = p;  i < t;  ++i) {
      TasksPlanEntry* planEntry = tasks->add_entries();
      planEntry->set_is_primary(true);

      ResourcesCurrent* resources = current.mutable_agency_resources();
      ResourcesCurrentEntry* resEntry = resources->add_entries();
      resEntry->set_state(RESOURCE_STATE_REQUIRED);

      InstancesCurrent* instances = current.mutable_agents();
      instances->add_entries();
    }
  }
  
  // need at least one DB server
  t = (int) target.dbservers().instances();

  if (t < 1) {
    LOG(ERROR)
    << "ERROR running in cluster mode, need at least one db-server";

    exit(EXIT_FAILURE);
  }

  tasks = plan.mutable_dbservers();
  p = tasks->entries_size();

  if (t < p) {
    LOG(INFO)
    << "INFO refusing to reduce number of db-servers from " << p << " to " << t
    << " NOT YET IMPLEMENTED.";
    target.mutable_dbservers()->set_instances(p);
  }

  if (p < t) {
    LOG(INFO)
    << "DEBUG creating " << (t - p) << " more db-servers in plan";

    for (int i = p;  i < t;  ++i) {
      TasksPlanEntry* planEntry = tasks->add_entries();
      planEntry->set_is_primary(true);

      ResourcesCurrent* resources = current.mutable_primary_dbserver_resources();
      ResourcesCurrentEntry* resEntry = resources->add_entries();
      resEntry->set_state(RESOURCE_STATE_REQUIRED);

      InstancesCurrent* instances = current.mutable_primary_dbservers();
      instances->add_entries();
    }
  }

  // need at least one coordinator
  t = (int) target.coordinators().instances();

  if (t < 1) {
    LOG(ERROR)
    << "ERROR running in cluster mode, need at least one coordinator";

    exit(EXIT_FAILURE);
  }

  tasks = plan.mutable_coordinators();
  p = tasks->entries_size();

  if (t < p) {
    LOG(INFO)
    << "INFO reducing the number of coordinators from " << p << " to " << t;
    TasksPlan original;
    original.CopyFrom(*tasks);
    
    tasks->clear_entries();
    for (int i = 0;  i < t;  ++i) {
      TasksPlanEntry entry = original.entries(i);
      tasks->add_entries()->CopyFrom(entry);
    }
  }

  if (p < t) {
    LOG(INFO)
    << "DEBUG creating " << (t - p) << " more coordinators in plan";

    for (int i = p;  i < t;  ++i) {
      TasksPlanEntry* planEntry = tasks->add_entries();
      planEntry->set_is_primary(true);

      ResourcesCurrent* resources = current.mutable_coordinator_resources();
      ResourcesCurrentEntry* resEntry = resources->add_entries();
      resEntry->set_state(RESOURCE_STATE_REQUIRED);

      InstancesCurrent* instances = current.mutable_coordinators();
      instances->add_entries();
    }
  }

  Global::state().setPlan(plan);
  Global::state().setCurrent(current);
}

////////////////////////////////////////////////////////////////////////////////
/// {@inheritDoc}
////////////////////////////////////////////////////////////////////////////////

InstanceAction CaretakerCluster::checkInstance () {
  Target target = Global::state().target();
  Plan plan = Global::state().plan();
  Current current = Global::state().current();

  auto res = checkStartInstance(
    "dbserver",
    AspectType::PRIMARY_DBSERVER,
    InstanceActionState::START_PRIMARY_DBSERVER,
    plan.dbservers(),
    current.mutable_primary_dbserver_resources(),
    current.mutable_primary_dbservers());

  Global::state().setPlan(plan);
  Global::state().setCurrent(current);

  return res;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
