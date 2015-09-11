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
/// @brief checks the number of ports, if the given role is empty, all ports
/// in the offer are taken, otherwise, only ports with that role are counted.
////////////////////////////////////////////////////////////////////////////////

static bool checkPorts (size_t numberOfPorts, const mesos::Offer& offer,
                        std::string const& role) {
  if (numberPorts(offer, role) < numberOfPorts) {
    return false;
  }

  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if the minimum resources are satisfied, the flag withRole
/// indicates whether or not the roles in the offer are taken into account.
/// With withRole==false, the offer as well as the minimum resources are
/// flattened to our role before the comparison and ports for all roles
/// in the offer are counted. With withRole==true, the minimum resources
/// are flattened to Global::role() and the offer is untouched to check
/// the minimum.
/// For the ports we do not care about reservations, we simply see whether
/// any ports for our role or "*" are included in the offer.
////////////////////////////////////////////////////////////////////////////////

static bool isSuitableOffer (Target const& target,
                             mesos::Offer const& offer,
                             bool withRole) {
  // Note that we do not care whether or not ports are reserved for us
  // or are role "*".
  if (! checkPorts(target.number_ports(), offer, "")) {
    LOG(INFO) 
    << "DEBUG isSuitableOffer: "
    << "offer " << offer.id().value() << " does not have " 
    << target.number_ports() << " ports"
    << (withRole ? " for Role" + Global::role() : " for any role");

    return false;
  }

  // Never need to flatten the offered resources, since we use find:
  mesos::Resources offered = offer.resources();

  // Always flatten the minimal resources with our role, because find is 
  // flexible:
  mesos::Resources minimum = target.minimal_resources();
  minimum = minimum.flatten(Global::role());

  Option<mesos::Resources> found = offered.find(minimum);
  std::string offerString;
  if (! found.isSome()) {
    pbjson::pb2json(&offer, offerString);
     
    LOG(INFO) 
    << "DEBUG isSuitableOffer: "
    << "offer " << offer.id().value() << " does not have " 
    << "minimal resource requirements " << minimum
    << "\noffer: " << offerString;

    return false;
  }

  if (withRole) {
    mesos::Resources defaultRole = arangodb::filterIsDefaultRole(found.get());
    if (! defaultRole.empty()) {
      pbjson::pb2json(&offer, offerString);
       
      LOG(INFO) 
      << "DEBUG isSuitableOffer: "
      << "offer " << offer.id().value() << " meets the " 
      << "minimal resource requirements " << minimum
      << " but needs role \"*\" for this: " << defaultRole
      << "\noffer: " << offerString;

      return false;
    }
  }

  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if we have enough reserved resources to get a persistent vol
////////////////////////////////////////////////////////////////////////////////

static bool isSuitableReservedOffer (mesos::Offer const& offer,
                                     Target const& target,
                                     ResourceCurrent const* resCur,
                                     mesos::Resources& toMakePersistent) {
  // The condition here is that our minimal resources are all met with
  // reserved resources and that there is a single disk resource that
  // is not yet persistent for anybody and that we can make persistent
  // for us:
  if (! isSuitableOffer(target, offer, true)) {
    LOG(INFO) << "Offer does not contain enough reserved resources.";
    return false;
  }

  // Now study the offered and needed disk resources only:
  mesos::Resources offered = offer.resources();
  offered = arangodb::filterIsDisk(offered);

  mesos::Resources required = target.minimal_resources();
  required = arangodb::filterIsDisk(required);
  required = required.flatten(Global::role(), Global::principal());
  // Now required is a single resource of type disk with our role.

  for (mesos::Resource& res : offered) {
    mesos::Resources oneResource;
    oneResource += res;
    if (oneResource.contains(required)) {
      toMakePersistent = required;
      return true;
    }
  }

  std::string offerString;
  pbjson::pb2json(&offer, offerString);
   
  LOG(INFO) 
  << "DEBUG isSuitableReservedOffer: "
  << "offer " << offer.id().value() << " meets the " 
  << "minimal resource requirements "
  << "but lacks a reserved disk resource to make persistent"
  << "\noffer: " << offerString;

  return false;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief finds free ports from a ranges
///////////////////////////////////////////////////////////////////////////////

static void findFreePortsFromRange (mesos::Resources& result,
                                    vector<mesos::Value::Range> const& ranges,
                                    vector<bool> const& isDynRes,
                                    bool reserved,
                                    size_t& found,
                                    size_t len) {
  for (size_t rangeChoice = 0; rangeChoice < ranges.size(); rangeChoice++) {

    const auto& resource = ranges.at(rangeChoice);

    for (uint32_t port = resource.begin(); port <= resource.end(); port++) {
      if (found >= len) {
        return;
      }
      mesos::Resource onePort;
      onePort.set_name("ports");
      onePort.set_type(mesos::Value::RANGES);
      auto* r = onePort.mutable_ranges()->add_range();
      r->set_begin(port);
      r->set_end(port);
      if (! reserved) {
        onePort.set_role("*");
      }
      else {
        onePort.set_role(Global::role());
        if (isDynRes[rangeChoice]) {
          onePort.mutable_reservation()->CopyFrom(Global::principal());
        }
      }
      result += onePort;
      found++;
    }
  }
}

///////////////////////////////////////////////////////////////////////////////
/// @brief finds free ports from an offer
///////////////////////////////////////////////////////////////////////////////

static mesos::Resources findFreePorts (const mesos::Offer& offer, size_t len) {
  vector<mesos::Value::Range> resources;
  vector<mesos::Value::Range> reserved;
  vector<bool>                isDynamicallyReserved;

  auto const& principal = Global::principal();

  for (int i = 0; i < offer.resources_size(); ++i) {
    const auto& resource = offer.resources(i);

    if (resource.name() == "ports" && resource.type() == mesos::Value::RANGES) {
      const auto& ranges = resource.ranges();

      for (int j = 0; j < ranges.range_size(); ++j) {
        const auto& range = ranges.range(j);

        // reserved resources: they must either be statically or
        // dynamically with matching principal
        if (mesos::Resources::isReserved(resource, Option<std::string>())) {
          if (mesos::Resources::isDynamicallyReserved(resource)) {
            if (resource.reservation().principal() == principal.principal()) {
              reserved.push_back(range);
              isDynamicallyReserved.push_back(true);
            }
          }
          else {
            reserved.push_back(range);
            isDynamicallyReserved.push_back(false);
          }
        }

        // unreserved
        else if (mesos::Resources::isUnreserved(resource)) {
          resources.push_back(range);
        }
      }
    }
  }

  mesos::Resources result;
  size_t found = 0;
  findFreePortsFromRange(result, reserved, isDynamicallyReserved, true, 
                         found, len);
  findFreePortsFromRange(result, resources, isDynamicallyReserved, false, 
                         found, len);

  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief resources required for the start of an ephemeral task
////////////////////////////////////////////////////////////////////////////////

static mesos::Resources resourcesForStartEphemeral (mesos::Offer const& offer,
                                                    Target const& target,
                                                    ResourceCurrent* resCur) {
  mesos::Resources offered = offer.resources();
  mesos::Resources minimum = target.minimal_resources();
  
  // We know that the minimal resources fit into the offered resources,
  // when we ignore roles. We now have to grab as much as the minimal 
  // resources prescribe (always with role "*"), but prefer the role
  // specific resources and only turn to the "*" resources if the others
  // are not enough.
#if 0  
  // Old approach without find:
  minimum = minimum.flatten(Global.role());
  mesos::Resources roleSpecificPart 
      = arangodb::intersectResources(offered, minimum);
  mesos::Resources defaultPart = minimum - roleSpecificPart;
  defaultPart.flatten();
  mesos::Resources toUse = roleSpecificPart + defaultPart;
#endif
  Option<mesos::Resources> toUseOpt = offered.find(minimum);
  mesos::Resources toUse;
  if (toUseOpt.isSome()) {
    toUse = toUseOpt.get();
  }
  // toUse will be empty, when it does not fit, we will run into an error later.

  // Add ports with the role we actually found in the resource offer:
  toUse += findFreePorts(offer, target.number_ports());

  // TODO(fc) check if we could use additional resources

  return toUse;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief resources required for a dynamic reservation
////////////////////////////////////////////////////////////////////////////////

static mesos::Resources resourcesForRequestReservation (
                                    mesos::Offer const& offer,
                                    Target const& target,
                                    ResourceCurrent* resCur) {
  mesos::Resources offered = offer.resources();
  mesos::Resources minimum = target.minimal_resources();
  
  // We know that the minimal resources fit into the offered resources,
  // when we ignore roles. We now have to reserve that part of the 
  // resources with role "*" that is necessary to have all of the minimal
  // resources with our role.
  minimum = minimum.flatten(Global::role());
  mesos::Resources roleSpecificPart 
      = arangodb::intersectResources(offered, minimum);
  mesos::Resources defaultPart = minimum - roleSpecificPart;
  defaultPart.flatten();

  // TODO(fc) check if we could use additional resources

  return defaultPart;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief resources required for starts with persistent volume
////////////////////////////////////////////////////////////////////////////////

static mesos::Resources suitablePersistent (string const& name,
                                            mesos::Offer const& offer,
                                            Target const& target,
                                            ResourceCurrent* resCur,
                                            string const& persistenceId,
                                            string& containerPath) {

  // However, we have to check that there is a single disk resource that
  // is large enough and has the right persistent ID for us. Therefore
  // we have to separate disk and non-disk resources and proceed similar
  // to resourcesForStartEphemeral for the non-disk resources and
  // special for the disk-resources:

  // For logging:
  std::string offerString;

  mesos::Resources offered = offer.resources();
  mesos::Resources offeredDisk = filterIsDisk(offered);
  offered = filterNotIsDisk(offered);

  mesos::Resources minimum = target.minimal_resources();
  minimum = minimum.flatten(Global::role());
  mesos::Resources minimumDisk = filterIsDisk(minimum);
  minimum = filterNotIsDisk(minimum);

  Option<mesos::Resources> toUseOpt = offered.find(minimum);
  if (! toUseOpt.isSome()) {
    pbjson::pb2json(&offer, offerString);
    LOG(INFO) 
    << "DEBUG suitablePersistent(" << name << "): "
    << "offer " << offer.id().value() << " [" << offer.resources()
    << "] does not have minimal resource requirements "
    << minimum
    << "\noffer: " << offerString;
    return mesos::Resources();    // this indicates an error, ignore offer
  }
  mesos::Resources toUse = toUseOpt.get();

  // Now look at the disk resources:
  size_t mds = diskspace(minimumDisk);

  bool found = false;

  for (const auto& res : offeredDisk) {
    if (res.role() != Global::role()) {
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

    containerPath = "volumes/roles/" + Global::role() + "/" + persistenceId;

    toUse += res;
    found = true;
    break;
  }

  if (! found) {
    pbjson::pb2json(&offer, offerString);
    LOG(INFO) 
    << "DEBUG suitablePersistent(" << name << "): "
    << "offer " << offer.id().value() << " [" << offer.resources()
    << "] does not have enough persistent disk resources"
    << minimumDisk
    << "\noffer: " << offerString;
    return mesos::Resources();  // indicates failure
  }

  // Add ports with the role we actually found in the resource offer:
  toUse += findFreePorts(offer, target.number_ports());

  LOG(INFO)
  << "DEBUG suitablePersistent(" << name << "): SUCCESS";

  return toUse;
}


////////////////////////////////////////////////////////////////////////////////
/// @brief request to make offer persistent
////////////////////////////////////////////////////////////////////////////////

static OfferAction requestPersistent (string const& upper,
                                      mesos::Offer const& offer,
                                      Target const& target,
                                      TaskPlan* task,
                                      ResourceCurrent* resCur) {
  mesos::Resources volume;

  if (isSuitableReservedOffer(offer, target, resCur, volume)) {
    double now = chrono::duration_cast<chrono::seconds>(
      chrono::steady_clock::now().time_since_epoch()).count();

    string persistentId = upper + "_" + UUID::random().toString();

    task->set_state(TASK_STATE_TRYING_TO_PERSIST);
    task->set_started(now);
    task->set_persistence_id(persistentId);

    resCur->mutable_offer_id()->CopyFrom(offer.id());

    volume = volume.flatten(Global::role(), Global::principal());
    resCur->mutable_resources()->CopyFrom(volume);

    return { OfferActionState::MAKE_PERSISTENT_VOLUME, volume, 
             upper, persistentId };
  }

  return { OfferActionState::IGNORE };
}

////////////////////////////////////////////////////////////////////////////////
/// @brief request to make a reservation
////////////////////////////////////////////////////////////////////////////////

static OfferAction requestReservation (std::string const& upper,
                                       mesos::Offer const& offer,
                                       Target const& target,
                                       TaskPlan* task,
                                       ResourceCurrent* resCur) {
  mesos::Resources resources
        = resourcesForRequestReservation(offer, target, resCur);

  if (resources.empty()) {
    // We have everything needed reserved for our role, so we can
    // directly move on to the persistent volume:
    return requestPersistent(upper, offer, target, task, resCur);
  }

  double now = chrono::duration_cast<chrono::seconds>(
    chrono::steady_clock::now().time_since_epoch()).count();

  task->set_state(TASK_STATE_TRYING_TO_RESERVE);
  task->set_started(now);

  resCur->mutable_slave_id()->CopyFrom(offer.slave_id());
  resCur->mutable_offer_id()->CopyFrom(offer.id());
  resCur->mutable_resources()->CopyFrom(resources);
  resCur->set_hostname(offer.hostname());

  resCur->clear_ports();

  return { OfferActionState::MAKE_DYNAMIC_RESERVATION, resources };
}

////////////////////////////////////////////////////////////////////////////////
/// @brief request to start with persistent volume
////////////////////////////////////////////////////////////////////////////////

static OfferAction requestStartPersistent (string const& upper,
                                           mesos::Offer const& offer,
                                           Target const& target,
                                           TaskPlan* task,
                                           ResourceCurrent* resCur) {
  string persistenceId = task->persistence_id();
  string containerPath;

  mesos::Resources resources = suitablePersistent(
    upper, offer, target, resCur, persistenceId, containerPath);

  if (! resources.empty()) {
    double now = chrono::duration_cast<chrono::seconds>(
      chrono::steady_clock::now().time_since_epoch()).count();

    task->set_state(TASK_STATE_TRYING_TO_START);
    task->set_persistence_id(persistenceId);
    task->set_started(now);

    resCur->mutable_offer_id()->CopyFrom(offer.id());
    resCur->mutable_resources()->CopyFrom(resources);
    resCur->set_container_path(containerPath);

    resCur->clear_ports();

    for (auto& res : resources) {
      if (res.name() == "ports" && res.type() == mesos::Value::RANGES) {
        auto const& ranges = res.ranges();
        for (int r = 0; r < ranges.range_size(); r++) {
          for (uint64_t i = ranges.range(r).begin();
               i <= ranges.range(r).end(); i++) {
            resCur->add_ports(i);
          }
        }
      }
    }

    LOG(INFO) << "Trying to start with resources:\n"
              << resources;

    return { OfferActionState::USABLE };
  }

  return { OfferActionState::IGNORE };
}                                  

////////////////////////////////////////////////////////////////////////////////
/// @brief request to start without persistent volume
////////////////////////////////////////////////////////////////////////////////

static OfferAction requestStartEphemeral (mesos::Offer const& offer,
                                          Target const& target,
                                          TaskPlan* task,
                                          ResourceCurrent* resCur) {

  mesos::Resources resources 
      = resourcesForStartEphemeral(offer, target, resCur);

  double now = chrono::duration_cast<chrono::seconds>(
    chrono::steady_clock::now().time_since_epoch()).count();

  task->set_state(TASK_STATE_TRYING_TO_START);
  task->set_started(now);

  resCur->mutable_slave_id()->CopyFrom(offer.slave_id());
  resCur->mutable_offer_id()->CopyFrom(offer.id());
  resCur->mutable_resources()->CopyFrom(resources);
  resCur->set_hostname(offer.hostname());

  resCur->clear_ports();

  for (auto& res : resources) {
    if (res.name() == "ports" && res.type() == mesos::Value::RANGES) {
      auto const& ranges = res.ranges();
      for (int r = 0; r < ranges.range_size(); r++) {
        for (uint64_t i = ranges.range(r).begin();
             i <= ranges.range(r).end(); i++) {
          resCur->add_ports(i);
        }
      }
    }
  }

  return { OfferActionState::USABLE };
}                                  

////////////////////////////////////////////////////////////////////////////////
/// @brief request to restart
////////////////////////////////////////////////////////////////////////////////

static OfferAction requestRestart (string const& upper,
                                   mesos::Offer const& offer,
                                   Target const& target,
                                   TaskPlan* task,
                                   ResourceCurrent* resCur) {

  if (! isSuitableOffer(target, offer, true)) {
    return { OfferActionState::IGNORE };
  }

  string persistenceId = task->persistence_id();
  string containerPath;

  mesos::Resources resources = suitablePersistent(
    upper, offer, target, resCur, persistenceId, containerPath);

  if (! resources.empty()) {
    double now = chrono::duration_cast<chrono::seconds>(
      chrono::steady_clock::now().time_since_epoch()).count();

    task->set_state(TASK_STATE_TRYING_TO_RESTART);
    task->set_persistence_id(persistenceId);
    task->set_started(now);

    resCur->mutable_offer_id()->CopyFrom(offer.id());
    resCur->mutable_resources()->CopyFrom(resources);
    resCur->set_container_path(containerPath);

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
                                           Target const& target,
                                           TasksPlan* tasks,
                                           ResourcesCurrent* current,
                                           mesos::Offer const& offer) {
  string upper = name;
  for (auto& c : upper) { 
    c = toupper(c);
  }
          
  // ...........................................................................
  // check that the minimal resources are satisfied, here we ignore roles, if
  // we are after a persistent volume, since we can always reserve more
  // resources for our role dynamically. If we are after ephemeral resources,
  // we do
  // ...........................................................................

  if (! isSuitableOffer(target, offer, false)) {
    return { OfferActionState::IGNORE };
  }

  int p = tasks->entries_size();

  if (p == 0) {
    LOG(INFO) << "nothing planned for " << name;
    return { OfferActionState::IGNORE };
  }

  // ...........................................................................
  // we do not want to start two instances of the same type on the same
  // slave; if we get an offer for the same slave, check if we are
  // currently trying to reserve or persist for this slave; if not,
  // ignore the offer.
  // ...........................................................................

  int required = -1;
  string const& offerSlaveId = offer.slave_id().value();

  for (int i = p-1; i >= 0; --i) {  // backwards to prefer earlier ones in
                                    // the required variable
    TaskPlan* task = tasks->mutable_entries(i);
    ResourceCurrent* resCur = current->mutable_entries(i);

    if (task->state() == TASK_STATE_NEW) {
      required = i;
      continue;
    }

    if (resCur->slave_id().value() == offerSlaveId) {
      switch (task->state()) {
        case TASK_STATE_TRYING_TO_RESERVE:
          return requestPersistent(upper, offer, target, task, resCur);

        case TASK_STATE_TRYING_TO_PERSIST:
          return requestStartPersistent(upper, offer, target, task, resCur);

        case TASK_STATE_KILLED:
        case TASK_STATE_FAILED_OVER:
          return requestRestart(upper, offer, target, task, resCur);

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

    if (primaryResEntry.has_slave_id() &&
        offer.slave_id().value() == primaryResEntry.slave_id().value()) {
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
      if (primaryResEntries.entries(i).has_slave_id() &&
          offer.slave_id().value()
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
  ResourceCurrent* resCur = current->mutable_entries(required);

  if (! persistent) {
    return requestStartEphemeral(offer, target, task, resCur);
  }

  // ...........................................................................
  // make a reservation, if we need a persistent volume
  // ...........................................................................

  return requestReservation(upper, offer, target, task, resCur);
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

      ResourceCurrent* resCur = resources->mutable_entries(i);

      task->set_state(TASK_STATE_RUNNING);
      task->set_started(now);

      instance->set_state(INSTANCE_STATE_STARTING);
      instance->set_hostname(resCur->hostname());
      instance->clear_ports();

      for (int j = 0;  j < resCur->ports_size();  ++j) {
        instance->add_ports(resCur->ports(j));
      }

      mesos::OfferID offerId = resCur->offer_id();
      mesos::SlaveID slaveId = resCur->slave_id();
      mesos::Resources resources = resCur->resources();

      return { startState, *resCur, { aspect, (size_t) i } };
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
