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

static void adjustPlan (const string& name,
                        const TargetEntry& target,
                        OfferPlan* offers,
                        TasksPlan* tasks,
                        ResourcesCurrent* resources,
                        InstancesCurrent* instances) {
  int t = (int) target.instances();
  int p = offers->entries_size();

  if (t == p) {
    LOG(INFO)
    << "DEBUG adjustPlan("  << name << "): "
    << "already reached maximal number of entries " << t;

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
    offers->add_entries();
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
    LOG(INFO) 
    << "DEBUG isSuitableOffer: "
    << "offer " << offer.id().value() << " does not have " 
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

#if MESOS_PRINCIPAL
  mesos::Resources required = resources.flatten(Global::role(), Global::principal());
#else
  mesos::Resources required = resources.flatten(Global::role());
#endif

  if (! offered.contains(required)) {
    LOG(INFO) 
    << "DEBUG isSuitableReservedOffer: "
    << "offer " << offer.id().value() << " [" << offer.resources()
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
    LOG(INFO) 
    << "DEBUG suitablePersistent(" << name << "): "
    << "offer " << offer.id().value() << " [" << offer.resources()
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

    persistenceId = diskId;
    containerPath = res.disk().volume().container_path();

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

#if MESOS_DYNAMIC_RESERVATION
#else
  persistent = false;
#endif

  string upper = name;
  for (auto& c : upper) { c = toupper(c); }
          
  // .............................................................................
  // check that the minimal resources are satisfied
  // .............................................................................

  if (! isSuitableOffer(target, offer)) {
    return { OfferActionState::IGNORE };
  }

  // .............................................................................
  // we do not want to start two instance on the same slave
  // .............................................................................

  int p = plan->entries_size();
  
  for (int i = 0;  i < p;  ++i) {
    OfferPlanEntry* entry = plan->mutable_entries(i);
    ResourcesCurrentEntry* resEntry = current->mutable_entries(i);

    if (entry->slave_id().value() == offer.slave_id().value()) {

      // we have to check if the dynamic reservation succeeded
      if (resEntry->state() == RESOURCE_STATE_TRYING_TO_RESERVE) {
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
      else if (resEntry->state() == RESOURCE_STATE_TRYING_TO_PERSIST) {
        mesos::Resources resources = resEntry->resources();
        string persistenceId;
        string containerPath;

        mesos::Resources persistent = suitablePersistent(upper,
                                                         resources,
                                                         offer,
                                                         persistenceId,
                                                         containerPath);

        if (! persistent.empty()) {
          entry->set_persistence_id(persistenceId);

          resEntry->set_state(RESOURCE_STATE_USEABLE);
          resEntry->mutable_offer_id()->CopyFrom(offer.id());
          resEntry->mutable_resources()->CopyFrom(persistent);
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

  // .............................................................................
  // check if we have a free slot
  // .............................................................................

  int required = -1;

  for (int i = 0;  i < p;  ++i) {
    const ResourcesCurrentEntry& resEntry = current->entries(i);

    if (resEntry.state() == RESOURCE_STATE_REQUIRED) {
      required = i;
      break;
    }
  }

  if (required == -1) {
    return { OfferActionState::STORE_FOR_LATER };
  }

  // .............................................................................
  // make a reservation
  // .............................................................................

  OfferPlanEntry* entry = plan->mutable_entries(required);

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
// --SECTION--                                          static protected methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if we can/should start a new instance
////////////////////////////////////////////////////////////////////////////////

InstanceAction Caretaker::checkStartInstance (const string& name,
                                              InstanceActionState startState,
                                              TasksPlan* plan,
                                              ResourcesCurrent* resources,
                                              InstancesCurrent* instances) {
  for (int i = 0;  i < plan->entries_size();  ++i) {
    InstancesCurrentEntry* instance = instances->mutable_entries(i);
    bool start = false;

    switch (instance->state()) {
      case INSTANCE_STATE_UNUSED:
        start = true;
        break;

      case INSTANCE_STATE_STARTING:
        break;
    }

    if (start) {
      ResourcesCurrentEntry* resEntry = resources->mutable_entries(i);

      if (resEntry->state() == RESOURCE_STATE_USEABLE) {
        instance->set_state(INSTANCE_STATE_STARTING);

        mesos::OfferID offerId = resEntry->offer_id();
        mesos::SlaveID slaveId = resEntry->slave_id();
        mesos::Resources resources = resEntry->resources();

        return { startState, *resEntry };
      }
    }
  }

  return { InstanceActionState::DONE };
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
#if 0
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
#endif
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
  adjustPlan("agency",
             _target.agencies(),
             _plan.mutable_agency_offers(),
             _plan.mutable_agencies(),
             _current.mutable_agency_resources(),
             _current.mutable_agencies());

  adjustPlan("dbserver",
             _target.dbservers(),
             _plan.mutable_primary_dbserver_offers(),
             _plan.mutable_primary_dbservers(),
             _current.mutable_primary_dbserver_resources(),
             _current.mutable_primary_dbservers());

  adjustPlan("coordinator",
             _target.coordinators(),
             _plan.mutable_coordinator_offers(),
             _plan.mutable_coordinators(),
             _current.mutable_coordinator_resources(),
             _current.mutable_coordinators());
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
    case OfferActionState::USABLE:
    case OfferActionState::MAKE_DYNAMIC_RESERVATION:
    case OfferActionState::MAKE_PERSISTENT_VOLUME:
      return agency;

    case OfferActionState::STORE_FOR_LATER:
      store = true;
      break;

    case OfferActionState::IGNORE:
      break;
  }

  OfferAction dbservers
    = checkResourceOffer("dbserver", true,
                         _target.dbservers(),
                         _plan.mutable_primary_dbserver_offers(),
                         _current.mutable_primary_dbserver_resources(),
                         offer);

  switch (dbservers._state) {
    case OfferActionState::USABLE:
    case OfferActionState::MAKE_DYNAMIC_RESERVATION:
    case OfferActionState::MAKE_PERSISTENT_VOLUME:
      return dbservers;

    case OfferActionState::STORE_FOR_LATER:
      store = true;
      break;

    case OfferActionState::IGNORE:
      break;
  }

  OfferAction coordinators
    = checkResourceOffer("coordinator", false,
                         _target.coordinators(),
                         _plan.mutable_coordinator_offers(),
                         _current.mutable_coordinator_resources(),
                         offer);

  switch (coordinators._state) {
    case OfferActionState::USABLE:
    case OfferActionState::MAKE_DYNAMIC_RESERVATION:
    case OfferActionState::MAKE_PERSISTENT_VOLUME:
      return coordinators;

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
