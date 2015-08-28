///////////////////////////////////////////////////////////////////////////////
/// @brief caretaker for resources and instances
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

#include "Caretaker.h"

#include "ArangoState.h"
#include "Global.h"
#include "utils.h"

#include <random>

#include "pbjson.hpp"

using namespace arangodb;
using namespace std;

// -----------------------------------------------------------------------------
// --SECTION--                                                 private functions
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief adjusts the planned offers
////////////////////////////////////////////////////////////////////////////////

static void adjustPlan (const string& name,
                        const TargetEntry& target,
                        TasksPlan* tasks,
                        ResourcesCurrent* resources,
                        InstancesCurrent* instances) {
  int t = (int) target.instances();
  int p = tasks->entries_size();

  if (t == p) {
    /*
    LOG(INFO)
    << "DEBUG adjustPlan("  << name << "): "
    << "already reached maximal number of entries " << t;
    */

    return;
  }

  if (t < p) {
    LOG(INFO)
    << "DEBUG adjustPlan("  << name << "): "
    << "got too many number of entries " << p
    << ", need only " << t;

    return; // TODO(fc) need to shrink number of instances
  }

  for (; p < t;  ++p) {
    tasks->add_entries();

    ResourcesCurrentEntry* resEntry = resources->add_entries();
    resEntry->set_state(RESOURCE_STATE_REQUIRED);

    instances->add_entries();
  }
  
  return;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks the number of ports
////////////////////////////////////////////////////////////////////////////////

static bool checkPorts (size_t numberOfPorts, const mesos::Offer& offer) {
  if (numberPorts(offer) < numberOfPorts) {
    return false;
  }

  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks is the minimum resources are satisfied
////////////////////////////////////////////////////////////////////////////////

static bool isSuitableOffer (const TargetEntry& target,
                             const mesos::Offer& offer) {
  if (! checkPorts(target.number_ports(), offer)) {
    LOG(INFO) 
    << "DEBUG isSuitableOffer: "
    << "offer " << offer.id().value() << " does not have " 
    << target.number_ports() << " ports";

    return false;
  }

  mesos::Resources offered = offer.resources();
  offered = offered.flatten();

  mesos::Resources minimum = target.minimal_resources();

  if (! offered.contains(minimum)) {
    std::string offerString;
    pbjson::pb2json(&offer, offerString);
     
    LOG(INFO) 
    << "DEBUG isSuitableOffer: "
    << "offer " << offer.id().value() << " does not have " 
    << "minimal resource requirements " << minimum
    << "\noffer: " << offerString;

    return false;
  }

  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks is the reservered resources are satisfied
////////////////////////////////////////////////////////////////////////////////

static bool isSuitableReservedOffer (const mesos::Resources& resources,
                                     const mesos::Offer& offer) {
  mesos::Resources offered = offer.resources();

#if MESOS_PRINCIPAL
  mesos::Resources required = resources.flatten(Global::role(), Global::principal());
#else
  mesos::Resources required = resources.flatten(Global::role());
#endif

  if (! offered.contains(required)) {
    std::string offerString;
    pbjson::pb2json(&offer, offerString);

    LOG(INFO) 
    << "DEBUG isSuitableReservedOffer: "
    << "offer " << offer.id().value() << " [" << offer.resources()
    << "] does not have minimal resource requirements "
    << required
    << "\noffer: " << offerString;

    return false;
  }

  return true;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief finds free ports from an offer
///////////////////////////////////////////////////////////////////////////////

static vector<uint32_t> findFreePorts (const mesos::Offer& offer, size_t len) {
  static const size_t MAX_ITERATIONS = 1000;

  vector<uint32_t> result;
  vector<mesos::Value::Range> resources;

  for (int i = 0; i < offer.resources_size(); ++i) {
    const auto& resource = offer.resources(i);

    if (resource.name() == "ports" && resource.type() == mesos::Value::RANGES) {
      const auto& ranges = resource.ranges();

      for (int j = 0; j < ranges.range_size(); ++j) {
        const auto& range = ranges.range(j);

        resources.push_back(range);
      }
    }
  }

  default_random_engine generator;
  uniform_int_distribution<int> d1(0, resources.size() - 1);

  for (size_t i = 0;  i < MAX_ITERATIONS;  ++i) {
    if (result.size() == len) {
      return result;
    }

    const auto& resource = resources.at(d1(generator));

    uniform_int_distribution<uint32_t> d2(resource.begin(), resource.end());

    result.push_back(d2(generator));
  }

  return result;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief generates resources from a list of ports
///////////////////////////////////////////////////////////////////////////////

static mesos::Resources resourcesPorts (const vector<uint32_t>& ports) {
  mesos::Resources resources;

  mesos::Resource res;
  res.set_name("ports");
  res.set_type(mesos::Value::RANGES);

  for (uint32_t p : ports) {
    mesos::Value_Range* range = res.mutable_ranges()->add_range();

    range->set_begin(p);
    range->set_end(p);
  }

  resources += res;

  return resources;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief resources required for reservation
////////////////////////////////////////////////////////////////////////////////

static mesos::Resources resourcesForReservation (const TargetEntry& target,
                                                 const mesos::Offer& offer,
                                                 vector<uint32_t>& ports) {
  mesos::Resources minimum = target.minimal_resources();
  ports = findFreePorts(offer, target.number_ports());

  minimum += resourcesPorts(ports);

  // TODO(fc) check if we could use additional resources
  
  return minimum;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief resources required for persistent volume
////////////////////////////////////////////////////////////////////////////////

static mesos::Resources resourcesForPersistence (const TargetEntry& target,
                                                 const mesos::Offer& offer) {
  mesos::Resources minimum = target.minimal_resources();
  minimum = filterIsDisk(minimum);

  // TODO(fc) check if we could use additional resources

#if MESOS_PRINCIPAL
  minimum = minimum.flatten(Global::role(), Global::principal());
#else
  minimum = minimum.flatten(Global::role());
#endif  


  return minimum;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief resources required for starts with persistent volume
////////////////////////////////////////////////////////////////////////////////

static mesos::Resources suitablePersistent (const string& name,
                                            const mesos::Resources& resources,
                                            const mesos::Offer& offer,
                                            string& persistenceId,
                                            string& containerPath) {
  mesos::Resources offered = offer.resources();
  mesos::Resources offerDisk = filterIsDisk(offered);
  mesos::Resources offerNoneDisk = filterNotIsDisk(offered);

  mesos::Resources resourcesNoneDisk = filterNotIsDisk(resources);
  mesos::Resources resourcesDisk = filterIsDisk(resources);

#if MESOS_PRINCIPAL
  mesos::Resources required = resourcesNoneDisk.flatten(Global::role(), Global::principal());
#else
  mesos::Resources required = resourcesNoneDisk.flatten(Global::role());
#endif

  if (! offerNoneDisk.contains(required)) {
    std::string offerString;
    pbjson::pb2json(&offer, offerString);

    LOG(INFO) 
    << "DEBUG suitablePersistent(" << name << "): "
    << "offer " << offer.id().value() << " [" << offer.resources()
    << "] does not have minimal resource requirements "
    << required
    << "\noffer: " << offerString;

    return mesos::Resources();
  }

  size_t mds = diskspace(resourcesDisk);

  string role = Global::role();

  for (const auto& res : offerDisk) {
    if (res.role() != role) {
      continue;
    }

    if (diskspace(res) < mds) {
      continue;
    }

    if (! res.has_disk()) {
      continue;
    }

    if (! res.disk().has_persistence()) {
      continue;
    }

    string diskId = res.disk().persistence().id();

    if (diskId.find(name + "_") != 0) {
      continue;
    }

    persistenceId = diskId;
    containerPath = res.disk().volume().container_path();

    return resourcesNoneDisk + res;
  }

  return mesos::Resources();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if an offer fits
////////////////////////////////////////////////////////////////////////////////

OfferAction Caretaker::checkResourceOffer (string const& name,
                                           bool persistent,
                                           TargetEntry const& target,
                                           TasksPlan* plan,
                                           ResourcesCurrent* current,
                                           mesos::Offer const& offer) {

  // Note
  // ====
  //
  // We have added support for dynamic reservation and persistent
  // volumes. They should become available (at least experimental)
  // in Mesos 0.23.
  //
  // However, the current Mesos 0.22 does not support these. Therefore
  // we force 'persistent' to be false. In this case, none of the 
  // persistent code below will be used.


#if MESOS_DYNAMIC_RESERVATION
#else
  persistent = false;
#endif

  string upper = name;
  for (auto& c : upper) { c = toupper(c); }
          
  // ...........................................................................
  // check that the minimal resources are satisfied
  // ...........................................................................

  if (! isSuitableOffer(target, offer)) {
    LOG(INFO) << "offer not suitable";
    return { OfferActionState::IGNORE };
  }

  int p = plan->entries_size();

  if (p == 0) {
    LOG(INFO) << "p == 0";
    return { OfferActionState::IGNORE };
  }

  // ...........................................................................
  // we do not want to start two instances on the same slave
  // ...........................................................................

  // Note
  // ====
  //
  // Most if this code is in preparation of persistent volumes and currently
  // not used. See above. As Mesos 0.23 should contain at least experimental
  // code for persistent volumes, we did not remove this part.

  int required = -1;
  const string& offerSlaveId = offer.slave_id().value();

  for (int i = 0;  i < p;  ++i) {
    TasksPlanEntry* entry = plan->mutable_entries(i);
    ResourcesCurrentEntry* resEntry = current->mutable_entries(i);

    if (entry->slave_id().value() == offerSlaveId) {
      auto state = resEntry->state();

      // we do not need a reservation
      if (state == RESOURCE_STATE_REQUIRED) {
        required = i;
        break;
      }

      // we have to check if the dynamic reservation succeeded
      else if (state == RESOURCE_STATE_TRYING_TO_RESERVE) {
        mesos::Resources resources = resEntry->resources();

        if (isSuitableReservedOffer(resources, offer)) {
#if MESOS_PRINCIPAL
          resources = resources.flatten(Global::role(), Global::principal());
#else
          resources = resources.flatten(Global::role());
#endif

          resEntry->set_state(RESOURCE_STATE_TRYING_TO_PERSIST);
          resEntry->mutable_offer_id()->CopyFrom(offer.id());
          resEntry->mutable_resources()->CopyFrom(resources);

          mesos::Resources volume = resourcesForPersistence(target, offer);

          return { OfferActionState::MAKE_PERSISTENT_VOLUME, volume, upper };
        }
      }

      // we have to check if the persistent volume succeeded
      else if (state == RESOURCE_STATE_TRYING_TO_PERSIST) {
        mesos::Resources resources = resEntry->resources();
        string persistenceId;
        string containerPath;

        mesos::Resources persistentRes = suitablePersistent(upper,
                                                            resources,
                                                            offer,
                                                            persistenceId,
                                                            containerPath);

        if (! persistentRes.empty()) {
          entry->set_persistence_id(persistenceId);

          resEntry->set_state(RESOURCE_STATE_USEABLE);
          resEntry->mutable_offer_id()->CopyFrom(offer.id());
          resEntry->mutable_resources()->CopyFrom(persistentRes);
          resEntry->set_container_path(containerPath);

          return { OfferActionState::USABLE };
        }
      }

      LOG(INFO)
      << "DEBUG checkResourceOffer(" << name << "): "
      << "already using slave " << entry->slave_id().value();

      return { OfferActionState::STORE_FOR_LATER };
    }
  }

  // ...........................................................................
  // check if we have a free slot
  // ...........................................................................

  if (required == -1) {
    for (int i = 0;  i < p;  ++i) {
      const ResourcesCurrentEntry& resEntry = current->entries(i);

      if (resEntry.state() == RESOURCE_STATE_REQUIRED) {
        required = i;
        break;
      }
    }
  }

  if (required == -1) {
    LOG(INFO) << "nothing required";
    return { OfferActionState::STORE_FOR_LATER };
  }

  // ...........................................................................
  // do not put a secondary on the same slave than its primary:
  // ...........................................................................

  if (name == "secondary") {
    Plan globalPlan = Global::state().plan();
    TasksPlanEntry const& primaryEntry = globalPlan.dbservers().entries(required);
    if (offer.slave_id().value() == primaryEntry.slave_id().value()) {
      // we decline this offer, there will be another one
      LOG(INFO) << "secondary not on same slave as its primary";
      return { OfferActionState::STORE_FOR_LATER };
    }
  }

  // ...........................................................................
  // do not put a secondary on a slave that we have not yet used
  // at all for a primary:
  // ...........................................................................

  if (Global::secondariesWithDBservers() && name == "secondary") {
    Plan globalPlan = Global::state().plan();
    TasksPlan const& primaryEntries = globalPlan.dbservers();
    int found = -1;
    for (int i = 0; i < primaryEntries.entries_size(); i++) {
      if (offer.slave_id().value() == 
          primaryEntries.entries(i).slave_id().value()) {
        found = i;
        break;
      }
    }
    if (found == -1) {
      // we decline this offer, there will be another one
      LOG(INFO) << "secondary not alone on a slave";
      return { OfferActionState::STORE_FOR_LATER };
    }
  }

  // ...........................................................................
  // make a reservation
  // ...........................................................................

  TasksPlanEntry* entry = plan->mutable_entries(required);

  entry->mutable_slave_id()->CopyFrom(offer.slave_id());

  ResourcesCurrentEntry* resEntry = current->mutable_entries(required);

  resEntry->mutable_slave_id()->CopyFrom(offer.slave_id());
  resEntry->mutable_offer_id()->CopyFrom(offer.id());

  if (! persistent) {
    vector<uint32_t> ports;
    mesos::Resources resources = resourcesForReservation(target, offer, ports);

    resEntry->clear_ports();

    for (auto port : ports) {
      resEntry->add_ports(port);
    }

    resEntry->set_state(RESOURCE_STATE_USEABLE);
    resEntry->mutable_resources()->CopyFrom(resources);
    resEntry->set_hostname(offer.hostname());

    return { OfferActionState::USABLE };
  }

  vector<uint32_t> ports;
  mesos::Resources resources = resourcesForReservation(target, offer, ports);

  resEntry->set_state(RESOURCE_STATE_TRYING_TO_RESERVE);
  resEntry->mutable_resources()->CopyFrom(resources);
  resEntry->set_hostname(offer.hostname());

  resEntry->clear_ports();

  for (auto port : ports) {
    resEntry->add_ports(port);
  }

  LOG(INFO)
  << "DEBUG checkResourceOffer(" << name << "): "
  << "need to make dynamic reservation " << resources;

  return { OfferActionState::MAKE_DYNAMIC_RESERVATION, resources };
}

// -----------------------------------------------------------------------------
// --Section--                                                   class Caretaker
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

Caretaker::Caretaker () {
}

////////////////////////////////////////////////////////////////////////////////
/// @brief destructor
////////////////////////////////////////////////////////////////////////////////

Caretaker::~Caretaker () {
}

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief tries to update the plan
////////////////////////////////////////////////////////////////////////////////

void Caretaker::updatePlan () {
  Target target = Global::state().target();
  Plan plan = Global::state().plan();
  Current current = Global::state().current();

  adjustPlan("agency",
             target.agents(),
             plan.mutable_agents(),
             current.mutable_agency_resources(),
             current.mutable_agents());

  adjustPlan("dbserver",
             target.dbservers(),
             plan.mutable_dbservers(),
             current.mutable_primary_dbserver_resources(),
             current.mutable_primary_dbservers());

  if (Global::asyncReplication()) {
    adjustPlan("secondaries",
               target.dbservers(),  // This is also the number of secondaries
               plan.mutable_secondaries(),
               current.mutable_secondary_dbserver_resources(),
               current.mutable_secondary_dbservers());
  }

  adjustPlan("coordinator",
             target.coordinators(),
             plan.mutable_coordinators(),
             current.mutable_coordinator_resources(),
             current.mutable_coordinators());

  Global::state().setPlan(plan);
  Global::state().setCurrent(current);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if we can use a resource offer
////////////////////////////////////////////////////////////////////////////////

OfferAction Caretaker::checkOffer (const mesos::Offer& offer) {
  bool store = false;

  Target target = Global::state().target();
  Plan plan = Global::state().plan();
  Current current = Global::state().current();

  OfferAction action;
  action = checkResourceOffer("primary", true,
                              target.dbservers(),
                              plan.mutable_dbservers(),
                              current.mutable_primary_dbserver_resources(),
                              offer);

  Global::state().setPlan(plan);
  Global::state().setCurrent(current);

  switch (action._state) {
    case OfferActionState::USABLE:
    case OfferActionState::MAKE_DYNAMIC_RESERVATION:
    case OfferActionState::MAKE_PERSISTENT_VOLUME:
      return action;

    case OfferActionState::STORE_FOR_LATER:
      store = true;
      break;

    case OfferActionState::IGNORE:
      break;
  }

  if (store) {
    return { OfferActionState::STORE_FOR_LATER };
  }
  else {
    return { OfferActionState::IGNORE };
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the task id, clears the task info and status
////////////////////////////////////////////////////////////////////////////////

void Caretaker::setTaskId (const AspectPosition& pos,
                           const mesos::TaskID& taskId) {
  Current current = Global::state().current();
  int p = pos._pos;

  mesos::SlaveID slaveId;
  slaveId.set_value("");

  mesos::TaskInfo info;
  info.set_name("embryo");
  info.mutable_task_id()->CopyFrom(taskId);
  info.mutable_slave_id()->CopyFrom(slaveId);

  switch (pos._type) {
    case AspectType::AGENT:
      current.mutable_agents()
        ->mutable_entries(p)
        ->mutable_task_info()
        ->CopyFrom(info);

      current.mutable_agents()
        ->mutable_entries(p)
        ->clear_task_status();
      break;

    case AspectType::PRIMARY_DBSERVER:
      current.mutable_primary_dbservers()
        ->mutable_entries(p)
        ->mutable_task_info()
        ->CopyFrom(info);

      current.mutable_primary_dbservers()
        ->mutable_entries(p)
        ->clear_task_status();
      break;

    case AspectType::SECONDARY_DBSERVER:
      current.mutable_secondary_dbservers()
        ->mutable_entries(p)
        ->mutable_task_info()
        ->CopyFrom(info);

      current.mutable_secondary_dbservers()
        ->mutable_entries(p)
        ->clear_task_status();
      break;

    case AspectType::COORDINATOR:
      current.mutable_coordinators()
        ->mutable_entries(p)
        ->mutable_task_info()
        ->CopyFrom(info);

      current.mutable_coordinators()
        ->mutable_entries(p)
        ->clear_task_status();
      break;

    case AspectType::UNKNOWN:
      LOG(INFO)
      << "unknown task type " << (int) pos._type
      << " for " << taskId.value();
      break;
  }

  Global::state().setCurrent(current);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the task info
////////////////////////////////////////////////////////////////////////////////

void Caretaker::setTaskInfo (const AspectPosition& pos,
                             const mesos::TaskInfo& taskInfo) {
  Current current = Global::state().current();
  int p = pos._pos;

  switch (pos._type) {
    case AspectType::AGENT:
      current.mutable_agents()
        ->mutable_entries(p)
        ->mutable_task_info()
        ->CopyFrom(taskInfo);
      break;

    case AspectType::PRIMARY_DBSERVER:
      current.mutable_primary_dbservers()
        ->mutable_entries(p)
        ->mutable_task_info()
        ->CopyFrom(taskInfo);
      break;

    case AspectType::SECONDARY_DBSERVER:
      current.mutable_secondary_dbservers()
        ->mutable_entries(p)
        ->mutable_task_info()
        ->CopyFrom(taskInfo);
      break;

    case AspectType::COORDINATOR:
      current.mutable_coordinators()
        ->mutable_entries(p)
        ->mutable_task_info()
        ->CopyFrom(taskInfo);
      break;

    case AspectType::UNKNOWN:
      LOG(INFO)
      << "unknown task type " << (int) pos._type
      << " for " << taskInfo.task_id().value();
      break;
  }

  Global::state().setCurrent(current);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the task status
////////////////////////////////////////////////////////////////////////////////

void Caretaker::setTaskStatus (const AspectPosition& pos,
                               const mesos::TaskStatus& taskStatus) {
  Current current = Global::state().current();
  int p = pos._pos;

  switch (pos._type) {
    case AspectType::AGENT:
      current.mutable_agents()
        ->mutable_entries(p)
        ->mutable_task_status()
        ->CopyFrom(taskStatus);
      break;

    case AspectType::PRIMARY_DBSERVER:
      current.mutable_primary_dbservers()
        ->mutable_entries(p)
        ->mutable_task_status()
        ->CopyFrom(taskStatus);
      break;

    case AspectType::SECONDARY_DBSERVER:
      current.mutable_secondary_dbservers()
        ->mutable_entries(p)
        ->mutable_task_status()
        ->CopyFrom(taskStatus);
      break;

    case AspectType::COORDINATOR:
      current.mutable_coordinators()
        ->mutable_entries(p)
        ->mutable_task_status()
        ->CopyFrom(taskStatus);
      break;

    case AspectType::UNKNOWN:
      LOG(INFO)
      << "unknown task type " << (int) pos._type
      << " for " << taskStatus.task_id().value();
      break;
  }

  Global::state().setCurrent(current);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the instance state
////////////////////////////////////////////////////////////////////////////////

void Caretaker::setInstanceState (const AspectPosition& pos,
                                  InstancesCurrentState state) {
  Current current = Global::state().current();
  int p = pos._pos;

  switch (pos._type) {
    case AspectType::AGENT:
      current.mutable_agents()
        ->mutable_entries(p)
        ->set_state(state);
      break;

    case AspectType::PRIMARY_DBSERVER:
      current.mutable_primary_dbservers()
        ->mutable_entries(p)
        ->set_state(state);
      break;

    case AspectType::SECONDARY_DBSERVER:
      current.mutable_secondary_dbservers()
        ->mutable_entries(p)
        ->set_state(state);
      break;

    case AspectType::COORDINATOR:
      current.mutable_coordinators()
        ->mutable_entries(p)
        ->set_state(state);
      break;

    case AspectType::UNKNOWN:
      LOG(INFO)
      << "unknown task type " << (int) pos._type;
      break;
  }

  Global::state().setCurrent(current);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief frees the resource for an instance
////////////////////////////////////////////////////////////////////////////////

void Caretaker::freeResourceForInstance (const AspectPosition& pos) {
  Current current = Global::state().current();
  int p = pos._pos;

  switch (pos._type) {
    case AspectType::AGENT:
      current.mutable_agency_resources()
        ->mutable_entries(p)
        ->set_state(RESOURCE_STATE_REQUIRED);
      break;

    case AspectType::PRIMARY_DBSERVER:
      current.mutable_primary_dbserver_resources()
        ->mutable_entries(p)
        ->set_state(RESOURCE_STATE_REQUIRED);
      break;

    case AspectType::SECONDARY_DBSERVER:
      current.mutable_secondary_dbserver_resources()
        ->mutable_entries(p)
        ->set_state(RESOURCE_STATE_REQUIRED);
      break;

    case AspectType::COORDINATOR:
      current.mutable_coordinator_resources()
        ->mutable_entries(p)
        ->set_state(RESOURCE_STATE_REQUIRED);
      break;

    case AspectType::UNKNOWN:
      LOG(INFO)
      << "unknown task type " << (int) pos._type;
      break;
  }

  Global::state().setCurrent(current);
}

// -----------------------------------------------------------------------------
// --SECTION--                                          static protected methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if we can/should start a new instance
////////////////////////////////////////////////////////////////////////////////

// FIXME: remove name argument which is unused
InstanceAction Caretaker::checkStartInstance (const string& name,
                                              AspectType aspect,
                                              InstanceActionState startState,
                                              const TasksPlan& plan,
                                              ResourcesCurrent* resources,
                                              InstancesCurrent* instances) {
  for (int i = 0;  i < plan.entries_size();  ++i) {
    InstancesCurrentEntry* instance = instances->mutable_entries(i);
    bool start = false;

    switch (instance->state()) {
      case INSTANCE_STATE_UNUSED:
      case INSTANCE_STATE_STOPPED:
        start = true;
        break;

      case INSTANCE_STATE_STARTING:
      case INSTANCE_STATE_RUNNING:
        break;
    }

    if (start) {
      ResourcesCurrentEntry* resEntry = resources->mutable_entries(i);

      if (resEntry->state() == RESOURCE_STATE_USEABLE) {
        instance->set_state(INSTANCE_STATE_STARTING);
        instance->set_hostname(resEntry->hostname());
        instance->clear_ports();

        for (int j = 0;  j < resEntry->ports_size();  ++j) {
          instance->add_ports(resEntry->ports(j));
        }

        resEntry->set_state(RESOURCE_STATE_USED);

        mesos::OfferID offerId = resEntry->offer_id();
        mesos::SlaveID slaveId = resEntry->slave_id();
        mesos::Resources resources = resEntry->resources();

        return { startState, *resEntry, { aspect, (size_t) i } };
      }
    }
  }

  return { InstanceActionState::DONE };
}

////////////////////////////////////////////////////////////////////////////////
/// @brief set a default minimum resource set for a Targetentry
////////////////////////////////////////////////////////////////////////////////

void Caretaker::setStandardMinimum (TargetEntry* te, int size) {
  mesos::Resource* m = te->add_minimal_resources();
  m->set_role("*");
  m->set_name("cpus");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(size == 0 ? 0.2 : 1);

  m = te->add_minimal_resources();
  m->set_role("*");
  m->set_name("mem");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(size == 0 ? 512 : 1024);
  
  m = te->add_minimal_resources();
  m->set_role("*");
  m->set_name("disk");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(size == 0 ? 512 : 1024);
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
