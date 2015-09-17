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
#include "ArangoScheduler.h"

#include "mesos/resources.hpp"
#include "arangodb.pb.h"
#include "pbjson.hpp"
#include "utils.h"

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
  Targets targets = Global::state().targets();

  // AGENCY
  Target* agency = targets.mutable_agents();
  // Will be: agency->set_instances(Global::nrAgents());
  agency->set_instances(1);
  agency->clear_minimal_resources();
  agency->set_number_ports(1);

  if (Global::minResourcesAgent().empty()) {
    setStandardMinimum(agency, 0);
  }
  else {
    Try<mesos::Resources> x
        = mesos::Resources::parse(Global::minResourcesAgent());
    if (x.isError()) {
      LOG(ERROR) << "cannot parse minimum resources for agent:\n  '"
                 << Global::minResourcesAgent() << "'";
      setStandardMinimum(agency, 0);
    }
    else {
      mesos::Resources res = x.get().flatten();   // always flatten to role "*"
      auto m = agency->mutable_minimal_resources();
      m->CopyFrom(res);
    }
  }

  // COORDINATOR
  Target* coordinator = targets.mutable_coordinators();
  coordinator->set_instances(Global::nrCoordinators());
  coordinator->clear_minimal_resources();
  coordinator->set_number_ports(1);
  if (Global::minResourcesCoordinator().empty()) {
    setStandardMinimum(coordinator, 1);
  }
  else {
    Try<mesos::Resources> x
        = mesos::Resources::parse(Global::minResourcesCoordinator());
    if (x.isError()) {
      LOG(ERROR) << "cannot parse minimum resources for coordinator:\n  '"
                 << Global::minResourcesCoordinator() << "'";
      setStandardMinimum(coordinator, 1);
    }
    else {
      mesos::Resources res = x.get().flatten();   // always flatten to role "*"
      auto m = coordinator->mutable_minimal_resources();
      m->CopyFrom(res);
    }
  }

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
      mesos::Resources res = x.get().flatten();   // always flatten to role "*"
      auto m = dbserver->mutable_minimal_resources();
      m->CopyFrom(res);
    }
  }

  // SECONDARIES
  Target* secondary = targets.mutable_secondaries();
  secondary->set_instances(Global::nrDBServers());
  secondary->clear_minimal_resources();
  secondary->set_number_ports(1);
  if (Global::minResourcesSecondary().empty()) {
    setStandardMinimum(secondary, 1);
  }
  else {
    Try<mesos::Resources> x
        = mesos::Resources::parse(Global::minResourcesSecondary());
    if (x.isError()) {
      LOG(ERROR) << "cannot parse minimum resources for Secondary:\n  '"
                 << Global::minResourcesSecondary() << "'";
      setStandardMinimum(secondary, 1);
    }
    else {
      mesos::Resources res = x.get().flatten();   // always flatten to role "*"
      auto m = secondary->mutable_minimal_resources();
      m->CopyFrom(res);
    }
  }

  Global::state().setTargets(targets);
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

  Targets targets = Global::state().targets();
  Plan plan = Global::state().plan();
  Current current = Global::state().current();
  int t, p;

  // First the agency, currently, we only support a single agency:
  t = (int) targets.agents().instances();
  if (t < 1) {
    LOG(ERROR)
    << "ERROR running in cluster mode, need at least one agency";

    exit(EXIT_FAILURE);
  }
  if (t > 1) {
    LOG(INFO)
    << "INFO currently we support only a single server agency";
    t = 1;
    Target* te = targets.mutable_agents();
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
      TaskPlan entry = original.entries(i);

      tasks->add_entries()->CopyFrom(entry);
    }
  }
  if (p < t) {
    LOG(INFO)
    << "DEBUG creating " << (t - p) << " more agents in plan";

    for (int i = p;  i < t;  ++i) {
      TaskPlan* task = tasks->add_entries();
      task->set_state(TASK_STATE_NEW);
      task->set_is_primary(true);

      TasksCurrent* agents = current.mutable_agents();
      TaskCurrent* inst = agents->add_entries();
      inst->set_state(INSTANCE_STATE_UNUSED);
    }
  }
  
  // need at least one DB server
  t = (int) targets.dbservers().instances();

  if (t < 1) {
    LOG(ERROR)
    << "ERROR running in cluster mode, need at least one db-server";

    exit(EXIT_FAILURE);
  }

  tasks = plan.mutable_dbservers();
  TasksPlan* tasks2 = plan.mutable_secondaries();
  p = tasks->entries_size();

  if (t < p) {
    LOG(INFO)
    << "INFO refusing to reduce number of db-servers from " << p << " to " << t
    << " NOT YET IMPLEMENTED.";
    targets.mutable_dbservers()->set_instances(p);
  }

  if (p < t) {
    LOG(INFO)
    << "DEBUG creating " << (t - p) << " more db-servers in plan";

    for (int i = p;  i < t;  ++i) {
      TaskPlan* task = tasks->add_entries();
      task->set_state(TASK_STATE_NEW);
      task->set_is_primary(true);

      TasksCurrent* dbservers = current.mutable_dbservers();
      TaskCurrent* inst = dbservers->add_entries();
      inst->set_state(INSTANCE_STATE_UNUSED);

      if (Global::asyncReplication()) {
        task = tasks2->add_entries();
        task->set_state(TASK_STATE_NEW);
        task->set_is_primary(false);

        TasksCurrent* secondaries = current.mutable_secondaries();
        inst = secondaries->add_entries();
        inst->set_state(INSTANCE_STATE_UNUSED);
      }
    }
  }

  // need at least one coordinator
  t = (int) targets.coordinators().instances();

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
      TaskPlan entry = original.entries(i);
      tasks->add_entries()->CopyFrom(entry);
    }
  }

  if (p < t) {
    LOG(INFO)
    << "DEBUG creating " << (t - p) << " more coordinators in plan";

    for (int i = p;  i < t;  ++i) {
      TaskPlan* task = tasks->add_entries();
      task->set_state(TASK_STATE_NEW);
      task->set_is_primary(true);

      TasksCurrent* coordinators = current.mutable_coordinators();
      TaskCurrent* inst = coordinators->add_entries();
      inst->set_state(INSTANCE_STATE_UNUSED);
    }
  }

  Global::state().setPlan(plan);
  Global::state().setCurrent(current);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief count the number of running instances of a certain kind
////////////////////////////////////////////////////////////////////////////////

static int countRunningInstances (TasksCurrent const& currents) {
  int runningInstances = 0;

  for (int i = 0; i < currents.entries_size(); i++) {
    TaskCurrent const& entry = currents.entries(i);

    if (entry.state() == INSTANCE_STATE_RUNNING) {
      runningInstances += 1;
    }
  }

  return runningInstances;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief check an incoming offer against a certain kind of server
////////////////////////////////////////////////////////////////////////////////

void CaretakerCluster::checkOffer (const mesos::Offer& offer) {
  // We proceed as follows:
  //   If not all agencies are up and running, then we check whether
  //   this offer is good for an agency.
  //   If all agencies are running, we ask the first one if it is initialized
  //   properly. If not, we wait and decline the offer.
  //   Otherwise, if not all DBservers are up and running, we check first
  //   whether this offer is good for one of them.
  //   If all DBservers are up, and we use asynchronous replication,
  //   we check whether all secondaries are up, lastly, we check with 
  //   the coordinators. If all is well, we decline politely.

  Targets targets = Global::state().targets();
  Plan plan = Global::state().plan();
  Current current = Global::state().current();

  bool offerUsed = false;

#if 0
  // Further debugging output:
  LOG(INFO) 
  << "checkOffer, here is the state:\n"
  << "TARGETS:" << Global::state().jsonTargets() << "\n"
  << "PLAN:"   << Global::state().jsonPlan() << "\n"
  << "CURRENT:"<< Global::state().jsonCurrent() << "\n";

  std::string offerString;
  pbjson::pb2json(&offer, offerString);

  LOG(INFO)
  << "And here the offer:\n" << offerString << "\n";
#endif

  int plannedInstances = plan.agents().entries_size();
  int runningInstances = countRunningInstances(current.agents());
  LOG(INFO)
  << "planned agent instances: " << plannedInstances << ", "
  << "running agent instances: " << runningInstances;
  if (runningInstances < plannedInstances) {
    // Try to use the offer for a new agent:
    offerUsed = checkResourceOffer("agency", true,
                                   targets.agents(),
                                   plan.mutable_agents(),
                                   current.mutable_agents(),
                                   offer, ! current.cluster_initialized(),
                                   TaskType::AGENT);

    // Save new state:
    Global::state().setPlan(plan);
    Global::state().setCurrent(current);

    LOG(INFO) 
    << "checkOffer, here is the state:\n"
    << "TARGETS:" << Global::state().jsonTargets() << "\n"
    << "PLAN:"   << Global::state().jsonPlan() << "\n"
    << "CURRENT:"<< Global::state().jsonCurrent() << "\n";

    if (offerUsed) {
      return;
    }

    // Otherwise, fall through to give other task types a chance to look
    // at the offer. This only happens when the cluster is already
    // initialized.
  }

  if (! current.cluster_initialized()) {
    // Agency is running, make sure it is initialized:
    std::string agentHost 
      = Global::state().current().agents().entries(0).hostname();
    uint32_t port 
      = Global::state().current().agents().entries(0).ports(0);
    std::string url = "http://" + agentHost + ":" + to_string(port) +
                      "/v2/keys/arango/InitDone";
    std::string body;
    int res = doHTTPGet(url, body);
    if (res == 0) {
      // Quickly check the JSON result:
      picojson::value s;
      std::string err = picojson::parse(s, body);

      res = -2;  // Will rest to 0 if all is OK
      if (err.empty()) {
        if (s.is<picojson::object>()) {
          auto& o = s.get<picojson::object>();
          auto& n = o["node"];
          if (n.is<picojson::object>()) {
            auto& oo = n.get<picojson::object>();
            auto& v = oo["value"];
            if (v.is<string>()) {
              string vv = v.get<string>();
              if (vv == "true") {
                res = 0;  // all OK
              }
            }
          }
        }
      }
    }
    if (res != 0) {
      // Ignore the offer, since the agency is not yet ready:
      LOG(INFO)
      << "agency is not yet properly initialized, decline offer.";
      Global::scheduler().declineOffer(offer.id());
      return;
    }
    LOG(INFO)
    << "agency is up and running.";
  }
  
  // Now look after the DBservers:
  plannedInstances = plan.dbservers().entries_size();
  runningInstances = countRunningInstances(current.dbservers());
  LOG(INFO)
  << "planned DBServer instances: " << plannedInstances << ", "
  << "running DBServer instances: " << runningInstances;
  if (runningInstances < plannedInstances) {
    // Try to use the offer for a new DBserver:
    offerUsed = checkResourceOffer("primary", true,
                                   targets.dbservers(),
                                   plan.mutable_dbservers(),
                                   current.mutable_dbservers(),
                                   offer, ! current.cluster_initialized(),
                                   TaskType::PRIMARY_DBSERVER);

    // Save new state:
    Global::state().setPlan(plan);
    Global::state().setCurrent(current);
    if (offerUsed) {
      return;
    }
    // Otherwise, fall through to give other task types a chance to look
    // at the offer. This only happens when the cluster is already
    // initialized.
  }

  // Now the secondaries, if needed:
  if (Global::asyncReplication()) {
    plannedInstances = plan.dbservers().entries_size();
    runningInstances = countRunningInstances(current.secondaries());
    LOG(INFO)
    << "planned secondary DBServer instances: " << plannedInstances << ", "
    << "running secondary DBServer instances: " << runningInstances;
    if (runningInstances < plannedInstances) {
      // Try to use the offer for a new DBserver:
      offerUsed = checkResourceOffer("secondary", true,
                                     targets.secondaries(),
                                     plan.mutable_secondaries(),
                                     current.mutable_secondaries(),
                                     offer, ! current.cluster_initialized(),
                                     TaskType::SECONDARY_DBSERVER);

      // Save new state:
      Global::state().setPlan(plan);
      Global::state().setCurrent(current);
      if (offerUsed) {
        return;
      }
      // Otherwise, fall through to give other task types a chance to look
      // at the offer. This only happens when the cluster is already
      // initialized.
    }
  }

  // Finally, look after the coordinators:
  plannedInstances = plan.coordinators().entries_size();
  runningInstances = countRunningInstances(current.coordinators());
  LOG(INFO)
  << "planned coordinator instances: " << plannedInstances << ", "
  << "running coordinator instances: " << runningInstances;
  if (runningInstances < plannedInstances) {
    // Try to use the offer for a new DBserver:
    checkResourceOffer("coordinator", false,
                       targets.coordinators(),
                       plan.mutable_coordinators(),
                       current.mutable_coordinators(),
                       offer, true, TaskType::COORDINATOR);

    // Save new state:
    Global::state().setPlan(plan);
    Global::state().setCurrent(current);
    return;
  }

  LOG(INFO) << "Cluster is complete.";
  if (! current.cluster_complete()) {
    LOG(INFO) << "Initiating cluster initialisation procedure...";
    current.set_cluster_complete(true);
    Global::state().setCurrent(current);
  }

  // All is good, decline offer:
  Global::scheduler().declineOffer(offer.id());
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
