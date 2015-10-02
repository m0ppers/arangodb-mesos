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
  auto lease = Global::state().lease(true);
  Targets* targets = lease.state().mutable_targets();

  // AGENCY
  Target* agency = targets->mutable_agents();
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
  Target* coordinator = targets->mutable_coordinators();
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
  Target* dbserver = targets->mutable_dbservers();
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
  Target* secondary = targets->mutable_secondaries();
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
}

// -----------------------------------------------------------------------------
// --SECTION--                                               some static helpers
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief count the number of planned instances of a certain kind
////////////////////////////////////////////////////////////////////////////////

static int countPlannedInstances (TasksPlan const& plans) {
  int plannedInstances = 0;

  for (int i = 0; i < plans.entries_size(); i++) {
    TaskPlan const& entry = plans.entries(i);

    if (entry.state() != TASK_STATE_DEAD) {
      plannedInstances += 1;
    }
  }

  return plannedInstances;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief count the number of running instances of a certain kind
////////////////////////////////////////////////////////////////////////////////

static int countRunningInstances (TasksPlan const& plans) {
  int runningInstances = 0;

  for (int i = 0; i < plans.entries_size(); i++) {
    TaskPlan const& entry = plans.entries(i);

    if (entry.state() == TASK_STATE_RUNNING ||
        entry.state() == TASK_STATE_TRYING_TO_START ||
        entry.state() == TASK_STATE_TRYING_TO_RESTART) {
      runningInstances += 1;
    }
  }

  return runningInstances;
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

  auto lease = Global::state().lease(true);

  Targets* targets = lease.state().mutable_targets();
  Plan* plan = lease.state().mutable_plan();
  Current* current = lease.state().mutable_current();
  int t, p;

  // First the agency, currently, we only support a single agency:
  t = (int) targets->agents().instances();
  if (t > 1) {
    LOG(INFO)
    << "INFO currently we support only a single server agency";
    t = 1;
    Target* te = targets->mutable_agents();
    te->set_instances(1);
  }
  TasksPlan* tasks = plan->mutable_agents();
  p = countPlannedInstances(plan->agents());
  if (t < p) {
    LOG(INFO)
    << "INFO reducing number of agents from " << p << " to " << t;

    TasksPlan original;
    original.CopyFrom(*tasks);
    
    // FIXME: This does not work at all, we need to mark them as DEAD and
    // kill them explicitly.
    tasks->clear_entries();

    for (int i = 0;  i < t;  ++i) {
      TaskPlan entry = original.entries(i);

      tasks->add_entries()->CopyFrom(entry);
    }
  }
  if (p < t) {
    LOG(INFO)
    << "DEBUG creating " << (t - p) << " more agents in plan";

    for (int i = p; i < t; ++i) {
      TaskPlan* task = tasks->add_entries();
      task->set_state(TASK_STATE_NEW);
      std::string name = "Agent" 
                         + std::to_string(tasks->entries_size());
      task->set_name(name);

      TasksCurrent* agents = current->mutable_agents();
      agents->add_entries();
    }
  }
  
  // need at least one DB server
  t = (int) targets->dbservers().instances();
  tasks = plan->mutable_dbservers();
  TasksPlan* tasks2 = plan->mutable_secondaries();
  p = countPlannedInstances(plan->dbservers());

  if (t < p) {
    LOG(INFO)
    << "INFO refusing to reduce number of db-servers from " << p << " to " << t
    << " NOT YET IMPLEMENTED.";
    targets->mutable_dbservers()->set_instances(p);
  }

  if (p < t) {
    LOG(INFO)
    << "DEBUG creating " << (t - p) << " more db-servers in plan";

    for (int i = p;  i < t;  ++i) {
      TaskPlan* task = tasks->add_entries();
      task->set_state(TASK_STATE_NEW);
      std::string name = "DBServer" 
                         + std::to_string(tasks->entries_size());
      task->set_name(name);

      TasksCurrent* dbservers = current->mutable_dbservers();
      dbservers->add_entries();

      if (Global::asyncReplication()) {
        TaskPlan* task2 = tasks2->add_entries();
        task2->set_state(TASK_STATE_NEW);
        std::string name2 = "Secondary"
                            + std::to_string(tasks2->entries_size());
        task2->set_name(name2);
        task->set_sync_partner(name2);
        task2->set_sync_partner(name);

        TasksCurrent* secondaries = current->mutable_secondaries();
        secondaries->add_entries();
      }
    }
  }

  // need at least one coordinator
  t = (int) targets->coordinators().instances();
  tasks = plan->mutable_coordinators();
  p = countPlannedInstances(plan->coordinators());

  if (t < p) {
    LOG(INFO)
    << "INFO reducing the number of coordinators from " << p << " to " << t;
    TasksPlan original;
    original.CopyFrom(*tasks);
    
    // FIXME: This does not work at all, we need to mark them as dead!
    // and then explicitly kill them!

    tasks->clear_entries();
    for (int i = 0;  i < t;  ++i) {
      TaskPlan entry = original.entries(i);
      tasks->add_entries()->CopyFrom(entry);
    }
  }

  if (p < t) {
    LOG(INFO)
    << "DEBUG creating " << (t - p) << " more coordinators in plan";

    for (int i = p; i < t; ++i) {
      TaskPlan* task = tasks->add_entries();
      task->set_state(TASK_STATE_NEW);
      std::string name = "Coordinator" 
                         + std::to_string(tasks->entries_size());
      task->set_name(name);

      TasksCurrent* coordinators = current->mutable_coordinators();
      coordinators->add_entries();
    }
  }
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

  auto lease = Global::state().lease();

  Targets* targets = lease.state().mutable_targets();
  Plan* plan = lease.state().mutable_plan();
  Current* current = lease.state().mutable_current();

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

  int plannedInstances = countPlannedInstances(plan->agents());
  int runningInstances = countRunningInstances(plan->agents());
  LOG(INFO)
  << "planned agent instances: " << plannedInstances << ", "
  << "running agent instances: " << runningInstances;
  if (runningInstances < plannedInstances) {
    // Try to use the offer for a new agent:
    offerUsed = checkOfferOneType(lease, "agency", true,
                                  targets->agents(),
                                  plan->mutable_agents(),
                                  current->mutable_agents(),
                                  offer, ! current->cluster_initialized(),
                                  TaskType::AGENT);

    if (offerUsed) {
      lease.changed();   // save new state
      return;
    }

    // Otherwise, fall through to give other task types a chance to look
    // at the offer. This only happens when the cluster is already
    // initialized.
  }

  if (! current->cluster_initialized()) {
    // Agency is running, make sure it is initialized:
    std::string agentHost 
      = current->agents().entries(0).hostname();
    uint32_t port 
      = current->agents().entries(0).ports(0);
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
  plannedInstances = countPlannedInstances(plan->dbservers());
  runningInstances = countRunningInstances(plan->dbservers());
  LOG(INFO)
  << "planned DBServer instances: " << plannedInstances << ", "
  << "running DBServer instances: " << runningInstances;
  if (runningInstances < plannedInstances) {
    // Try to use the offer for a new DBserver:
    offerUsed = checkOfferOneType(lease, "primary", true,
                                  targets->dbservers(),
                                  plan->mutable_dbservers(),
                                  current->mutable_dbservers(),
                                  offer, ! current->cluster_initialized(),
                                  TaskType::PRIMARY_DBSERVER);

    if (offerUsed) {
      lease.changed();  // make sure new state is saved
      return;
    }
    // Otherwise, fall through to give other task types a chance to look
    // at the offer. This only happens when the cluster is already
    // initialized.
  }

  // Now the secondaries, if needed:
  if (Global::asyncReplication()) {
    plannedInstances = countPlannedInstances(plan->secondaries());
    runningInstances = countRunningInstances(plan->secondaries());
    LOG(INFO)
    << "planned secondary DBServer instances: " << plannedInstances << ", "
    << "running secondary DBServer instances: " << runningInstances;
    if (runningInstances < plannedInstances) {
      // Try to use the offer for a new DBserver:
      offerUsed = checkOfferOneType(lease, "secondary", true,
                                    targets->secondaries(),
                                    plan->mutable_secondaries(),
                                    current->mutable_secondaries(),
                                    offer, ! current->cluster_initialized(),
                                    TaskType::SECONDARY_DBSERVER);

      if (offerUsed) {
        lease.changed();  // make sure new state is saved
        return;
      }
      // Otherwise, fall through to give other task types a chance to look
      // at the offer. This only happens when the cluster is already
      // initialized.
    }
  }

  // Finally, look after the coordinators:
  plannedInstances = countPlannedInstances(plan->coordinators());
  runningInstances = countRunningInstances(plan->coordinators());
  LOG(INFO)
  << "planned coordinator instances: " << plannedInstances << ", "
  << "running coordinator instances: " << runningInstances;
  if (runningInstances < plannedInstances) {
    // Try to use the offer for a new coordinator:
    if (checkOfferOneType(lease, "coordinator", false,
                          targets->coordinators(),
                          plan->mutable_coordinators(),
                          current->mutable_coordinators(),
                          offer, true, TaskType::COORDINATOR)) {
      lease.changed();  // make sure that the new state is saved
    }
    return;
  }

  // plannedInstances is 0 if and only if we have shut down the cluster,
  // if this happened before the cluster was complete, there would be 
  // chaos.
  if (plannedInstances > 0 && ! current->cluster_complete()) {
    LOG(INFO) << "Cluster is complete.";
    LOG(INFO) << "Initiating cluster initialisation procedure...";
    current->set_cluster_complete(true);
    lease.changed();
  }

  // Nobody wanted this offer, see whether there is a persistent disk
  // in there and destroy it:
  mesos::Resources offered = offer.resources();
  mesos::Resources offeredDisk = filterIsDisk(offered);
  mesos::Resources toDestroy;
  for (auto& res : offeredDisk) {
    if (res.role() == Global::role() &&
        res.has_disk() &&
        res.disk().has_persistence() &&
        res.has_reservation() &&
        res.reservation().principal() == Global::principal().principal()) {
      toDestroy += res;
    }
  }
  if (! toDestroy.empty()) {
    LOG(INFO) << "Found a persistent disk(s) that nobody wants, "
              << "will destroy:" << toDestroy;
    Global::scheduler().destroyPersistent(offer, toDestroy);
    return;
  }

  // If there was no persistent disk, maybe there is a dynamic reservation,
  // if so, unreserve it:
  mesos::Resources toUnreserve;
  for (auto& res : offered) {
    if (res.role() == Global::role() &&
        res.has_reservation() &&
        res.reservation().principal() == Global::principal().principal()) {
      toUnreserve += res;
    }
  }
  if (! toUnreserve.empty()) {
    LOG(INFO) << "Found dynamically reserved resources that nobody wants, "
              << "will unreserve:" << toUnreserve;
    Global::scheduler().unreserveDynamically(offer, toUnreserve);
    return;
  }

  // All is good, simply decline offer:
  Global::scheduler().declineOffer(offer.id());
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
