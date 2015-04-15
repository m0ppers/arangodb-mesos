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

#include "Global.h"
#include "utils.h"

#include "pbjson.hpp"

using namespace arangodb;
using namespace std;

// -----------------------------------------------------------------------------
// --SECTION--                                                 private functions
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief adjusts the planned offers
////////////////////////////////////////////////////////////////////////////////

static void adjustPlannedOffers (const string& name,
                                 const TargetEntry& target,
                                 OfferPlan* plan,
                                 ResourcesCurrent* current) {
  int t = (int) target.instances();
  int p = plan->entries_size();

  if (t == p) {
    LOG(INFO)
    << "DEBUG adjustPlannedOffers("  << name << "): "
    << "already reached maximal number of entries " << t;

    return;
  }

  if (t < p) {
    LOG(INFO)
    << "DEBUG adjustPlannedOffers("  << name << "): "
    << "got too many number of entries " << p
    << ", need only " << t;

    return; // TODO(fc) need to shrink number of instances
  }

  for (; p < t;  ++p) {
    OfferPlanEntry* entry = plan->add_entries();
    entry->set_state(OFFER_STATE_REQUIRED);

    current->add_entries();
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
    << "offer " << offer.id() << " does not have " 
    << target.number_ports() << " ports";

    return false;
  }

  mesos::Resources offered = offer.resources();
  offered = offered.flatten();

  mesos::Resources minimum = target.minimal_resources();

  if (! offered.contains(minimum)) {
    LOG(INFO) 
    << "DEBUG isSuitableOffer: "
    << "offer " << offer.id() << " does not have " 
    << "minimal resource requirements " << minimum;

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
    LOG(INFO) 
    << "DEBUG isSuitableReservedOffer: "
    << "offer " << offer.id() << " [" << offer.resources()
    << "] does not have minimal resource requirements "
    << required;

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
                                                 const mesos::Offer& offer) {
  mesos::Resources minimum = target.minimal_resources();
  auto&& ports = findFreePorts(offer, target.number_ports());

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
  minimum = minimum.filter(isDisk);

  // TODO(fc) check if we could use additional resources
  
  return minimum.flatten(Global::role(), Global::principal());
}

////////////////////////////////////////////////////////////////////////////////
/// @brief resources required for starts with persistent volume
////////////////////////////////////////////////////////////////////////////////

static mesos::Resources suitablePersistent (const string& name,
                                            const mesos::Resources& resources,
                                            const mesos::Offer& offer) {
  mesos::Resources offered = offer.resources();
  mesos::Resources offerDisk = offered.filter(isDisk);
  mesos::Resources offerNoneDisk = offered.filter(notIsDisk);

  mesos::Resources resourcesNoneDisk = resources.filter(notIsDisk);
  mesos::Resources resourcesDisk = resources.filter(isDisk);

  mesos::Resources required = resourcesNoneDisk.flatten(Global::role(), Global::principal());

  if (! offerNoneDisk.contains(required)) {
    LOG(INFO) 
    << "DEBUG suitablePersistent(" << name << "): "
    << "offer " << offer.id() << " [" << offer.resources()
    << "] does not have minimal resource requirements "
    << required;

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

    return resourcesNoneDisk + res;
  }

  return mesos::Resources();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if an offer fits
////////////////////////////////////////////////////////////////////////////////

static OfferAction checkResourceOffer (const string& name,
                                       bool persistent,
                                       const TargetEntry& target,
                                       OfferPlan* plan,
                                       ResourcesCurrent* current,
                                       const mesos::Offer& offer) {

  string upper = name;
  for (auto& c : upper) { c = toupper(c); }
          
  // .............................................................................
  // check that the minimal resources are satisfied
  // .............................................................................

  if (! isSuitableOffer(target, offer)) {
    return { OfferState::IGNORE };
  }

  // .............................................................................
  // we do not want to start two instance on the same slave
  // .............................................................................

  int p = plan->entries_size();
  
  for (int i = 0;  i < p;  ++i) {
    OfferPlanEntry* entry = plan->mutable_entries(i);
    ResourcesCurrentEntry* resEntry = current->mutable_entries(i);

    if (entry->slave_id() == offer.slave_id()) {
      if (entry->state() == OFFER_STATE_TRYING_TO_RESERVE) {
        mesos::Resources resources = resEntry->resources();

        if (isSuitableReservedOffer(resources, offer)) {
          entry->set_state(OFFER_STATE_TRYING_TO_PERSIST);
          entry->mutable_offer_id()->CopyFrom(offer.id());

          resources = resources.flatten(Global::role(), Global::principal());
          resEntry->mutable_offer_id()->CopyFrom(offer.id());
          resEntry->mutable_resources()->CopyFrom(resources);

          mesos::Resources volume = resourcesForPersistence(target, offer);

          return { OfferState::MAKE_PERSISTENT_VOLUME, volume, upper };
        }
      }

      else if (entry->state() == OFFER_STATE_TRYING_TO_PERSIST) {
        mesos::Resources resources = resEntry->resources();
        mesos::Resources persistent = suitablePersistent(upper, resources, offer);

        if (! persistent.empty()) {
          entry->set_state(OFFER_STATE_USEABLE);
          entry->mutable_offer_id()->CopyFrom(offer.id());

          resEntry->mutable_offer_id()->CopyFrom(offer.id());
          resEntry->mutable_resources()->CopyFrom(persistent);

          return { OfferState::IGNORE };
        }
      }

      LOG(INFO)
      << "DEBUG checkResourceOffer(" << name << "): "
      << "already using slave " << entry->slave_id();

      return { OfferState::STORE_FOR_LATER };
    }
  }

  // .............................................................................
  // check if we have a free slot
  // .............................................................................

  int required = -1;

  for (int i = 0;  i < p;  ++i) {
    const OfferPlanEntry& entry = plan->entries(i);

    if (entry.state() == OFFER_STATE_REQUIRED) {
      required = i;
      break;
    }
  }

  if (required == -1) {
    return { OfferState::STORE_FOR_LATER };
  }

  // .............................................................................
  // make a reservation
  // .............................................................................

  OfferPlanEntry* entry = plan->mutable_entries(required);

  entry->mutable_slave_id()->CopyFrom(offer.slave_id());
  entry->mutable_offer_id()->CopyFrom(offer.id());

  ResourcesCurrentEntry* resEntry = current->mutable_entries(required);

  resEntry->mutable_slave_id()->CopyFrom(offer.slave_id());
  resEntry->mutable_offer_id()->CopyFrom(offer.id());

  if (! persistent) {
    entry->set_state(OFFER_STATE_USEABLE);

    return { OfferState::IGNORE };
  }

  mesos::Resources resources = resourcesForReservation(target, offer);

  entry->set_state(OFFER_STATE_TRYING_TO_RESERVE);
  resEntry->mutable_resources()->CopyFrom(resources);

  LOG(INFO)
  << "DEBUG checkResourceOffer(" << name << "): "
  << "need to make dynamic reservation " << resources;

  return { OfferState::MAKE_DYNAMIC_RESERVATION, resources };
}

// -----------------------------------------------------------------------------
// --SECTION--                                                   class Caretaker
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

Caretaker::Caretaker () {
  mesos::Resource* m;

  // AGENCY
  TargetEntry* agency = _target.mutable_agencies();

  agency->set_instances(2);
  agency->set_number_ports(2);

  m = agency->add_minimal_resources();
  m->set_role("*");
  m->set_name("cpus");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(1);

  m = agency->add_minimal_resources();
  m->set_role("*");
  m->set_name("mem");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(1024);

  m = agency->add_minimal_resources();
  m->set_role("*");
  m->set_name("disk");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(1024);

  // COORDINATOR
  TargetEntry* coordinator = _target.mutable_coordinators();

  coordinator->set_instances(1);
  coordinator->set_number_ports(1);

  m = coordinator->add_minimal_resources();
  m->set_role("*");
  m->set_name("cpus");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(1);

  m = coordinator->add_minimal_resources();
  m->set_role("*");
  m->set_name("mem");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(1024);

  m = coordinator->add_minimal_resources();
  m->set_role("*");
  m->set_name("disk");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(1024);

  // DBSERVER
  TargetEntry* dbserver = _target.mutable_dbservers();

  dbserver->set_instances(1);
  dbserver->set_number_ports(1);

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
}

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief tries to update the plan
////////////////////////////////////////////////////////////////////////////////

void Caretaker::updatePlan () {
  adjustPlannedOffers("agency",
                      _target.agencies(),
                      _plan.mutable_agency_offers(),
                      _current.mutable_agency_resources());

  // adjustPlannedOffers("dbserver", _target.dbservers(), _plan.mutable_dbserver_offers());
}

////////////////////////////////////////////////////////////////////////////////
/// @brief check if we can use a resource offer
////////////////////////////////////////////////////////////////////////////////

OfferAction Caretaker::checkOffer (const mesos::Offer& offer) {
  bool store = false;

  OfferAction agency
    = checkResourceOffer("agency", true,
                         _target.agencies(),
                         _plan.mutable_agency_offers(),
                         _current.mutable_agency_resources(),
                         offer);

  switch (agency._state) {
    case OfferState::MAKE_DYNAMIC_RESERVATION:
    case OfferState::MAKE_PERSISTENT_VOLUME:
    case OfferState::START_INSTANCE:
      return agency;

    case OfferState::STORE_FOR_LATER:
      store = true;
      break;

    case OfferState::IGNORE:
      break;
  }

/*
  OfferAction dbservers
    = checkResourceOffer("dbserver", true,
                         _target.dbservers(),
                         _plan.mutable_dbserver_offers(),
                         offer);

  switch (dbservers._state) {
    case OfferState::MAKE_DYNAMIC_RESERVATION:
    case OfferState::MAKE_PERSISTENT_VOLUME:
    case OfferState::START_INSTANCE:
      return dbservers;

    case OfferState::STORE_FOR_LATER:
      store = true;
      break;

    case OfferState::IGNORE:
      break;
  }

  OfferAction coordinators
    = checkResourceOffer("coordinator", false,
                         _target.coordinators(),
                         _plan.mutable_coordinator_offers(),
                         offer);

  switch (coordinators._state) {
    case OfferState::MAKE_DYNAMIC_RESERVATION:
    case OfferState::MAKE_PERSISTENT_VOLUME:
    case OfferState::START_INSTANCE:
      return coordinators;

    case OfferState::STORE_FOR_LATER:
      store = true;
      break;

    case OfferState::IGNORE:
      break;
  }
*/

  if (store) {
    return { OfferState::STORE_FOR_LATER };
  }
  else {
    return { OfferState::IGNORE };
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief target as json string
////////////////////////////////////////////////////////////////////////////////

string Caretaker::jsonTarget () const {
  string result;

  pbjson::pb2json(&_target, result);
  
  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief plan as json string
////////////////////////////////////////////////////////////////////////////////

string Caretaker::jsonPlan () const {
  string result;

  pbjson::pb2json(&_plan, result);
  
  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief current as json string
////////////////////////////////////////////////////////////////////////////////

string Caretaker::jsonCurrent () const {
  string result;

  pbjson::pb2json(&_current, result);
  
  return result;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
