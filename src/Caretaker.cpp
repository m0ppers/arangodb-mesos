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

#include <stout/uuid.hpp>

using namespace arangodb;
using namespace std;

// -----------------------------------------------------------------------------
// --SECTION--                                                 private functions
// -----------------------------------------------------------------------------

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

static bool isSuitableOffer (const Target& target,
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
  mesos::Resources required = resources.flatten(Global::role(), Global::principal());

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
/// @brief finds free ports from a ranges
///////////////////////////////////////////////////////////////////////////////

static void findFreePortsFromRange (set<uint32_t>& result,
                                    const vector<mesos::Value::Range>& ranges,
                                    size_t len) {
  static const size_t MAX_ITERATIONS = 1000;

  default_random_engine generator;
  uniform_int_distribution<int> d1(0, ranges.size() - 1);

  for (size_t i = 0;  i < MAX_ITERATIONS;  ++i) {
    if (result.size() >= len) {
      return;
    }

    const auto& resource = ranges.at(d1(generator));
    uniform_int_distribution<uint32_t> d2(resource.begin(), resource.end());

    result.insert(d2(generator));
  }
}

///////////////////////////////////////////////////////////////////////////////
/// @brief finds free ports from an offer
///////////////////////////////////////////////////////////////////////////////

static vector<uint32_t> findFreePorts (const mesos::Offer& offer, size_t len,
                                       std::string& role) {
  vector<mesos::Value::Range> resources;
  vector<mesos::Value::Range> reserved;
  auto principal = Global::principal();

  for (int i = 0; i < offer.resources_size(); ++i) {
    const auto& resource = offer.resources(i);
    if (resource.has_role()) {
      role = resource.role();
    }
    else {
      role = "*";
    }

    if (resource.name() == "ports" && resource.type() == mesos::Value::RANGES) {
      const auto& ranges = resource.ranges();

      for (int j = 0; j < ranges.range_size(); ++j) {
        const auto& range = ranges.range(j);

        // reserved resources: they must either be statically or
        // dynamically with matching principal
        // Do not insist on a role here.
        if (mesos::Resources::isReserved(resource, Option<std::string>())) {
          if (mesos::Resources::isDynamicallyReserved(resource)) {
            if (resource.reservation().principal() == principal.principal()) {
              reserved.push_back(range);
            }
          }
          else {
            reserved.push_back(range);
          }
        }

        // unreserved
        else if (mesos::Resources::isUnreserved(resource)) {
          resources.push_back(range);
        }
      }
    }
  }

  size_t n = min(len, reserved.size());

  set<uint32_t> result;
  findFreePortsFromRange(result, reserved, n);
  findFreePortsFromRange(result, resources, len);

  return vector<uint32_t>(result.begin(), result.end());
}

///////////////////////////////////////////////////////////////////////////////
/// @brief generates resources from a list of ports
///////////////////////////////////////////////////////////////////////////////

static mesos::Resources resourcesPorts (const vector<uint32_t>& ports,
                                        std::string const& role) {
  mesos::Resources resources;

  mesos::Resource res;
  res.set_name("ports");
  res.set_type(mesos::Value::RANGES);
  res.set_role(role);

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

static mesos::Resources resourcesForTask (const mesos::Offer& offer,
                                          const Target& target,
                                          vector<uint32_t>& ports) {
  mesos::Resources offered = offer.resources();
  mesos::Resources minimum = target.minimal_resources();

  // add the ports
  std::string role;
  ports = findFreePorts(offer, target.number_ports(), role);
  minimum += resourcesPorts(ports, role);

  // TODO(fc) check if we could use additional resources

  // and set the principal
  return minimum = minimum.flatten(Global::role(), Global::principal());
}

////////////////////////////////////////////////////////////////////////////////
/// @brief resources required for persistent volume
////////////////////////////////////////////////////////////////////////////////

static mesos::Resources resourcesForPersistence (const Target& target,
                                                 const mesos::Offer& offer) {
  mesos::Resources minimum = target.minimal_resources();
  minimum = filterIsDisk(minimum);

  // TODO(fc) check if we could use additional resources

  minimum = minimum.flatten(Global::role(), Global::principal());

  return minimum;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief resources required for starts with persistent volume
////////////////////////////////////////////////////////////////////////////////

static mesos::Resources suitablePersistent (const string& name,
                                            const mesos::Resources& resources,
                                            const mesos::Offer& offer,
                                            const string& persistenceId,
                                            string& containerPath) {
  mesos::Resources offered = offer.resources();
  mesos::Resources offerDisk = filterIsDisk(offered);
  mesos::Resources offerNoneDisk = filterNotIsDisk(offered);

  mesos::Resources resourcesNoneDisk = filterNotIsDisk(resources);
  mesos::Resources resourcesDisk = filterIsDisk(resources);
  mesos::Resources required = resourcesNoneDisk.flatten(Global::role(), Global::principal());

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

    if (persistenceId != res.disk().persistence().id()) {
      continue;
    }

    containerPath = res.disk().volume().container_path();

    return resourcesNoneDisk + res;
  }

  return mesos::Resources();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief request to make a reservation
////////////////////////////////////////////////////////////////////////////////

static OfferAction requestReservation (const mesos::Offer& offer,
                                       TaskPlan* task,
                                       ResourceCurrent* resource,
                                       const mesos::Resources& resources,
                                       const vector<uint32_t>& ports) {
  double now = chrono::duration_cast<chrono::seconds>(
    chrono::steady_clock::now().time_since_epoch()).count();

  task->set_state(TASK_STATE_TRYING_TO_RESERVE);
  task->set_started(now);

  resource->mutable_slave_id()->CopyFrom(offer.slave_id());
  resource->mutable_offer_id()->CopyFrom(offer.id());
  resource->mutable_resources()->CopyFrom(resources);
  resource->set_hostname(offer.hostname());

  resource->clear_ports();

  for (auto port : ports) {
    resource->add_ports(port);
  }

  return { OfferActionState::MAKE_DYNAMIC_RESERVATION, resources - offer.resources() };
}

////////////////////////////////////////////////////////////////////////////////
/// @brief request to make offer persistent
////////////////////////////////////////////////////////////////////////////////

static OfferAction requestPersistent (const string& upper,
                                      const mesos::Offer& offer,
                                      const Target& target,
                                      TaskPlan* task,
                                      ResourceCurrent* resource) {
  mesos::Resources resources = resource->resources();

  if (isSuitableReservedOffer(resources, offer)) {
    double now = chrono::duration_cast<chrono::seconds>(
      chrono::steady_clock::now().time_since_epoch()).count();

    string persistentId = upper + "_" + UUID::random().toString();

    task->set_state(TASK_STATE_TRYING_TO_PERSIST);
    task->set_started(now);
    task->set_persistence_id(persistentId);

    resource->mutable_offer_id()->CopyFrom(offer.id());

    resources = resources.flatten(Global::role(), Global::principal());
    resource->mutable_resources()->CopyFrom(resources);

    mesos::Resources volume = resourcesForPersistence(target, offer);

    return { OfferActionState::MAKE_PERSISTENT_VOLUME, volume, upper, persistentId };
  }

  return { OfferActionState::IGNORE };
}

////////////////////////////////////////////////////////////////////////////////
/// @brief request to start with persistent volume
////////////////////////////////////////////////////////////////////////////////

static OfferAction requestStart (const string& upper,
                                 const mesos::Offer& offer,
                                 TaskPlan* task,
                                 ResourceCurrent* resource) {
  string persistenceId = task->persistence_id();
  string containerPath;

  mesos::Resources resources = suitablePersistent(
    upper, resource->resources(), offer, persistenceId, containerPath);

  if (! resources.empty()) {
    double now = chrono::duration_cast<chrono::seconds>(
      chrono::steady_clock::now().time_since_epoch()).count();

    task->set_state(TASK_STATE_TRYING_TO_START);
    task->set_persistence_id(persistenceId);
    task->set_started(now);

    resource->mutable_offer_id()->CopyFrom(offer.id());
    resource->mutable_resources()->CopyFrom(resources);
    resource->set_container_path(containerPath);

    return { OfferActionState::USABLE };
  }

  return { OfferActionState::IGNORE };
}                                  

////////////////////////////////////////////////////////////////////////////////
/// @brief request to start without persistent volume
////////////////////////////////////////////////////////////////////////////////

static OfferAction requestStart (const mesos::Offer& offer,
                                 TaskPlan* task,
                                 ResourceCurrent* resource,
                                 const mesos::Resources& resources,
                                 const vector<uint32_t>& ports) {
  double now = chrono::duration_cast<chrono::seconds>(
    chrono::steady_clock::now().time_since_epoch()).count();

  task->set_state(TASK_STATE_TRYING_TO_START);
  task->set_started(now);

  resource->mutable_slave_id()->CopyFrom(offer.slave_id());
  resource->mutable_offer_id()->CopyFrom(offer.id());
  resource->mutable_resources()->CopyFrom(resources.flatten());
  resource->set_hostname(offer.hostname());

  resource->clear_ports();

  for (auto port : ports) {
    resource->add_ports(port);
  }

  return { OfferActionState::USABLE };
}                                  

////////////////////////////////////////////////////////////////////////////////
/// @brief request to restart
////////////////////////////////////////////////////////////////////////////////

static OfferAction requestRestart (const string& upper,
                                   const mesos::Offer& offer,
                                   TaskPlan* task,
                                   ResourceCurrent* resource) {
  string persistenceId = task->persistence_id();
  string containerPath;

  mesos::Resources resources = suitablePersistent(
    upper, resource->resources(), offer, persistenceId, containerPath);

  if (! resources.empty()) {
    double now = chrono::duration_cast<chrono::seconds>(
      chrono::steady_clock::now().time_since_epoch()).count();

    task->set_state(TASK_STATE_TRYING_TO_RESTART);
    task->set_persistence_id(persistenceId);
    task->set_started(now);

    resource->mutable_offer_id()->CopyFrom(offer.id());
    resource->mutable_resources()->CopyFrom(resources);
    resource->set_container_path(containerPath);

    return { OfferActionState::USABLE };
  }

  return { OfferActionState::IGNORE };
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
// --SECTION--                                                   private methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if an offer fits
////////////////////////////////////////////////////////////////////////////////

OfferAction Caretaker::checkResourceOffer (const string& name,
                                           bool persistent,
                                           const Target& target,
                                           TasksPlan* tasks,
                                           ResourcesCurrent* current,
                                           const mesos::Offer& offer) {
  string upper = name;
  for (auto& c : upper) { c = toupper(c); }
          
  // ...........................................................................
  // check that the minimal resources are satisfied
  // ...........................................................................

  if (! isSuitableOffer(target, offer)) {
    return { OfferActionState::IGNORE };
  }

  int p = tasks->entries_size();

  if (p == 0) {
    LOG(INFO) << "nothing planed for " << name;
    return { OfferActionState::IGNORE };
  }

  // ...........................................................................
  // we do not want to start two instances on the same slave; if we get an offer
  // for the same slave, check if we are currently trying to reserve or persist
  // for this slave; if not, ignore the offer.
  // ...........................................................................

  int required = -1;
  const string& offerSlaveId = offer.slave_id().value();

  for (int i = 0;  i < p;  ++i) {
    TaskPlan* task = tasks->mutable_entries(i);
    ResourceCurrent* resource = current->mutable_entries(i);

    if (task->state() == TASK_STATE_NEW) {
      required = i;
      continue;
    }

    if (resource->slave_id().value() == offerSlaveId) {
      switch (task->state()) {
        case TASK_STATE_TRYING_TO_RESERVE:
          return requestPersistent(upper, offer, target, task, resource);

        case TASK_STATE_TRYING_TO_PERSIST:
          return requestStart(upper, offer, task, resource);

        case TASK_STATE_KILLED:
        case TASK_STATE_FAILED_OVER:
          return requestRestart(upper, offer, task, resource);

        default:
          return { OfferActionState::IGNORE };
      }
    }
  }

  // ...........................................................................
  // check if we need an offer
  // ...........................................................................

  if (required == -1) {
    LOG(INFO) << "nothing required";
    return { OfferActionState::IGNORE };
  }

  // ...........................................................................
  // do not put a secondary on the same slave than its primary
  // ...........................................................................

  if (name == "secondary") {
    Current globalCurrent = Global::state().current();
    ResourceCurrent const& primaryResEntry
      = globalCurrent.primary_dbserver_resources().entries(required);

    if (offer.slave_id().value() == primaryResEntry.slave_id().value()) {
      // we decline this offer, there will be another one
      LOG(INFO) << "secondary not on same slave as its primary";
      return { OfferActionState::IGNORE };
    }
  }

  // ...........................................................................
  // do not put a secondary on a slave that we have not yet used at all for a
  // primary
  // ...........................................................................

  if (Global::secondariesWithDBservers() && name == "secondary") {
    Current globalCurrent = Global::state().current();
    ResourcesCurrent const& primaryResEntries
      = globalCurrent.primary_dbserver_resources();

    int found = -1;

    for (int i = 0; i < primaryResEntries.entries_size(); i++) {
      if (offer.slave_id().value()
          == primaryResEntries.entries(i).slave_id().value()) {
        found = i;
        break;
      }
    }

    if (found == -1) {
      // we decline this offer, there will be another one
      LOG(INFO) << "secondary not alone on a slave";
      return { OfferActionState::IGNORE };
    }
  }

  // ...........................................................................
  // try to start directly, if we do not need a reservation
  // ...........................................................................

  TaskPlan* task = tasks->mutable_entries(required);
  ResourceCurrent* resource = current->mutable_entries(required);

  vector<uint32_t> ports;
  mesos::Resources resources = resourcesForTask(offer, target, ports);

  if (! persistent) {
    return requestStart(offer, task, resource, resources, ports);
  }

  // ...........................................................................
  // make a reservation, if we need a persistent volume
  // ...........................................................................

  return requestReservation(offer, task, resource, resources, ports);
}

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if we can use a resource offer
////////////////////////////////////////////////////////////////////////////////

OfferAction Caretaker::checkOffer (const mesos::Offer& offer) {
  Targets targets = Global::state().targets();
  Plan plan = Global::state().plan();
  Current current = Global::state().current();

  OfferAction action;
  action = checkResourceOffer("primary", true,
                              targets.dbservers(),
                              plan.mutable_dbservers(),
                              current.mutable_primary_dbserver_resources(),
                              offer);

  Global::state().setPlan(plan);
  Global::state().setCurrent(current);

  return action;
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
                                  InstanceCurrentState state) {
  double now = chrono::duration_cast<chrono::seconds>(
    chrono::steady_clock::now().time_since_epoch()).count();

  Current current = Global::state().current();
  Plan plan = Global::state().plan();

  InstancesCurrent* instances = nullptr;
  TasksPlan* tasks = nullptr;
  int p = pos._pos;

  switch (pos._type) {
    case AspectType::AGENT:
      instances = current.mutable_agents();
      tasks = plan.mutable_agents();
      break;

    case AspectType::PRIMARY_DBSERVER:
      instances = current.mutable_primary_dbservers();
      tasks = plan.mutable_dbservers();
      break;

    case AspectType::SECONDARY_DBSERVER:
      instances = current.mutable_secondary_dbservers();
      tasks = plan.mutable_secondaries();
      break;

    case AspectType::COORDINATOR:
      instances = current.mutable_coordinators();
      tasks = plan.mutable_coordinators();
      break;

    default:
      LOG(INFO)
      << "unknown task type " << (int) pos._type;
      return;
  }

  instances->mutable_entries(p)->set_state(state);

  switch (state) {
    case INSTANCE_STATE_UNUSED:
    case INSTANCE_STATE_STARTING:
      LOG(INFO)
      << "unexpected state " << (int) state;
      break;

    case INSTANCE_STATE_RUNNING:
      // TODO: check old state?
      tasks->mutable_entries(p)->set_state(TASK_STATE_RUNNING);
      tasks->mutable_entries(p)->set_started(now);
      break;

    case INSTANCE_STATE_STOPPED:
      // TODO: check old state?
      tasks->mutable_entries(p)->set_state(TASK_STATE_KILLED);
      tasks->mutable_entries(p)->set_started(now);
      break;
  }

  Global::state().setPlan(plan);
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
                                              TasksPlan* tasks,
                                              ResourcesCurrent* resources,
                                              InstancesCurrent* instances) {
  double now = chrono::duration_cast<chrono::seconds>(
    chrono::steady_clock::now().time_since_epoch()).count();

  for (int i = 0;  i < tasks->entries_size();  ++i) {
    TaskPlan* task = tasks->mutable_entries(i);
    auto state = task->state();

    if (state == TASK_STATE_TRYING_TO_START || state == TASK_STATE_TRYING_TO_RESTART) {
      InstanceCurrent* instance = instances->mutable_entries(i);

      switch (instance->state()) {
        case INSTANCE_STATE_UNUSED:
        case INSTANCE_STATE_STOPPED:
          break;

        case INSTANCE_STATE_STARTING:
        case INSTANCE_STATE_RUNNING:
          // TODO: this should not happen! What now?
          break;
      }

      ResourceCurrent* resource = resources->mutable_entries(i);

      task->set_state(TASK_STATE_RUNNING);
      task->set_started(now);

      instance->set_state(INSTANCE_STATE_STARTING);
      instance->set_hostname(resource->hostname());
      instance->clear_ports();

      for (int j = 0;  j < resource->ports_size();  ++j) {
        instance->add_ports(resource->ports(j));
      }

      mesos::OfferID offerId = resource->offer_id();
      mesos::SlaveID slaveId = resource->slave_id();
      mesos::Resources resources = resource->resources();

      return { startState, *resource, { aspect, (size_t) i } };
    }
  }

  return { InstanceActionState::DONE };
}

////////////////////////////////////////////////////////////////////////////////
/// @brief set a default minimum resource set for a target
////////////////////////////////////////////////////////////////////////////////

void Caretaker::setStandardMinimum (Target* te, int size) {
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
