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
  Target target = Global::state().target();

  // AGENCY
  TargetEntry* agency = target.mutable_agents();
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
      auto m = agency->mutable_minimal_resources();
      m->CopyFrom(x.get());
    }
  }

  // COORDINATOR
  TargetEntry* coordinator = target.mutable_coordinators();
  coordinator->set_instances(1);
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
      auto m = coordinator->mutable_minimal_resources();
      m->CopyFrom(x.get());
    }
  }

  // DBSERVER
  TargetEntry* dbserver = target.mutable_dbservers();
  dbserver->set_instances(2);
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
/// @brief count the number of running instances of a certain kind
////////////////////////////////////////////////////////////////////////////////

static int countRunningInstances (InstancesCurrent const& instances) {
  int runningInstances = 0;
  for (int i = 0; i < instances.entries_size(); i++) {
    InstancesCurrentEntry const& entry = instances.entries(i);
    if (entry.state() == INSTANCE_STATE_RUNNING) {
      runningInstances += 1;
    }
  }
  return runningInstances;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief check an incoming offer against a certain kind of server
////////////////////////////////////////////////////////////////////////////////

OfferAction CaretakerCluster::checkOffer (const mesos::Offer& offer) {
  // We proceed as follows:
  //   If not all agencies are up and running, then we check whether
  //   this offer is good for an agency.
  //   If all agencies are running, we ask the first one if it is initialized
  //   properly. If not, we wait and decline the offer.
  //   Otherwise, if not all DBservers are up and running, we check first
  //   whether this offer is good for one of them.
  //   If all DBservers are up, we check with the coordinators.
  //   If all is well, we decline politely.

  Target target = Global::state().target();
  Plan plan = Global::state().plan();
  Current current = Global::state().current();

  OfferAction action;

#if 0
  // Further debugging output:
  LOG(INFO) 
  << "checkOffer, here is the state:\n"
  << "TARGET:" << Global::state().jsonTarget() << "\n"
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
    action = checkResourceOffer("agency", true,
                                target.agents(),
                                plan.mutable_agents(),
                                current.mutable_agency_resources(),
                                offer);

    // Save new state:
    Global::state().setPlan(plan);
    Global::state().setCurrent(current);
    return action;
  }

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
    return { OfferActionState::IGNORE };
  }
  LOG(INFO)
  << "agency is up and running.";
  
  // Now look after the DBservers:
  plannedInstances = plan.dbservers().entries_size();
  runningInstances = countRunningInstances(current.primary_dbservers());
  LOG(INFO)
  << "planned DBServer instances: " << plannedInstances << ", "
  << "running DBServer instances: " << runningInstances;
  if (runningInstances < plannedInstances) {
    // Try to use the offer for a new DBserver:
    action = checkResourceOffer("primary", true,
                                target.dbservers(),
                                plan.mutable_dbservers(),
                                current.mutable_primary_dbserver_resources(),
                                offer);

    // Save new state:
    Global::state().setPlan(plan);
    Global::state().setCurrent(current);
    return action;
  }

  // Finally, look after the coordinators:
  plannedInstances = plan.coordinators().entries_size();
  runningInstances = countRunningInstances(current.coordinators());
  LOG(INFO)
  << "planned coordinator instances: " << plannedInstances << ", "
  << "running coordinator instances: " << runningInstances;
  if (runningInstances < plannedInstances) {
    // Try to use the offer for a new DBserver:
    action = checkResourceOffer("coordinator", false,
                                target.coordinators(),
                                plan.mutable_coordinators(),
                                current.mutable_coordinator_resources(),
                                offer);

    // Save new state:
    Global::state().setPlan(plan);
    Global::state().setCurrent(current);
    return action;
  }

  LOG(INFO) << "Cluster is complete.";
  if (! current.cluster_complete()) {
    LOG(INFO) << "Initiating cluster initialisation procedure...";
    current.set_cluster_complete(true);
    Global::state().setCurrent(current);
  }

  // All is good, ignore offer:
  return { OfferActionState::IGNORE };
}

////////////////////////////////////////////////////////////////////////////////
/// {@inheritDoc}
////////////////////////////////////////////////////////////////////////////////

InstanceAction CaretakerCluster::checkInstance () {
  Target target = Global::state().target();
  Plan plan = Global::state().plan();
  Current current = Global::state().current();

  InstanceAction res = checkStartInstance(
    "agency",
    AspectType::AGENT,
    InstanceActionState::START_AGENT,
    plan.agents(),
    current.mutable_agency_resources(),
    current.mutable_agents());

  if (res._state != InstanceActionState::DONE) {
    Global::state().setPlan(plan);
    Global::state().setCurrent(current);

    return res;
  }

  // OK, agents are fine, move on to DBservers:

  res = checkStartInstance(
    "dbserver",
    AspectType::PRIMARY_DBSERVER,
    InstanceActionState::START_PRIMARY_DBSERVER,
    plan.dbservers(),
    current.mutable_primary_dbserver_resources(),
    current.mutable_primary_dbservers());

  if (res._state != InstanceActionState::DONE) {
    Global::state().setPlan(plan);
    Global::state().setCurrent(current);

    return res;
  }

  // Finally, the coordinators:

  res = checkStartInstance(
    "coordinator",
    AspectType::COORDINATOR,
    InstanceActionState::START_COORDINATOR,
    plan.coordinators(),
    current.mutable_coordinator_resources(),
    current.mutable_coordinators());

  Global::state().setPlan(plan);
  Global::state().setCurrent(current);

  return res;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
