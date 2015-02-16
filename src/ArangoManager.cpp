///////////////////////////////////////////////////////////////////////////////
/// @brief manager for the ArangoDB framework
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

#include "ArangoManager.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <set>
#include <unordered_map>

#include <mesos/resources.hpp>

#include "common/type_utils.hpp"

#include "ArangoScheduler.h"

using namespace mesos;
using namespace arangodb;

using std::chrono::system_clock;
using InstanceState = ArangoManager::InstanceState;
using InstanceType = ArangoManager::InstanceType;
using UsedResources = ArangoManager::UsedResources;

// -----------------------------------------------------------------------------
// --SECTION--                                                  helper functions
// -----------------------------------------------------------------------------

namespace {
  double scalarResource (const Offer& offer, const std::string& name) {
    double value = 0.0;

    for (size_t i = 0; i < offer.resources_size(); ++i) {
      const auto& resource = offer.resources(i);

      if (resource.name() == name &&
          resource.type() == Value::SCALAR) {
        value = resource.scalar().value();
      }
    }

    return value;
  }


  size_t rangeResource (const Offer& offer, const std::string& name) {
    size_t value = 0;

    for (size_t i = 0; i < offer.resources_size(); ++i) {
      const auto& resource = offer.resources(i);

      if (resource.name() == name &&
          resource.type() == Value::RANGES) {
        const auto& ranges = resource.ranges();

        for (size_t j = 0; j < ranges.range_size(); ++j) {
          const auto& range = ranges.range(j);

          value += range.end() - range.begin() + 1;
        }
      }
    }

    return value;
  }


  bool checkOfferAgainstResources (const Offer& offer,
                                   const ArangoManager::Resources& resources,
                                   const string& name) {
    double cpus = scalarResource(offer, "cpus");

    if (cpus < resources._cpus) {
      LOG(INFO)
      << "OFFER declined for " << name << ": not enough CPU power "
      << "(got " << cpus << ", need " << resources._cpus << ")\n";
      return false;
    }

    double mem = scalarResource(offer, "mem");

    if (mem < resources._mem) {
      LOG(INFO)
      << "OFFER declined for " << name << ": not enough memory "
      << "(got " << mem << ", need " << resources._mem << ")\n";
      return false;
    }

    double disk = scalarResource(offer, "disk");

    if (disk < resources._disk) {
      LOG(INFO)
      << "OFFER declined for " << name << ": not enough disk "
      << "(got " << disk << ", need " << resources._disk << ")\n";
      return false;
    }

    size_t ports = rangeResource(offer, "ports");

    if (ports < resources._ports) {
      LOG(INFO)
      << "OFFER declined for " << name << ": not enough ports "
      << "(got " << ports << ", need " << resources._ports << ")\n";
      return false;
    }

    LOG(INFO)
    << "OFFER suitable for " << name << ": "
    << "cpus:" << cpus << ", "
    << "mem:" << mem << ", "
    << "disk:" << disk << ", "
    << "ports:" << ports << "\n";

    return true;
  }



  vector<uint32_t> findFreePorts (const Offer& offer, size_t len) {
    static const size_t MAX_ITERATIONS = 1000;

    vector<uint32_t> result;

    vector<Value::Range> resources;

    for (size_t i = 0; i < offer.resources_size(); ++i) {
      const auto& resource = offer.resources(i);

      if (resource.name() == "ports" &&
          resource.type() == Value::RANGES) {
        const auto& ranges = resource.ranges();

        for (size_t j = 0; j < ranges.range_size(); ++j) {
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
  }


  Resources convertUsedResources (
      const UsedResources& taskResources,
      const string& role) {

    // set MESOS resources
    Resources resources;

    Resource cpu;
    cpu.set_name("cpus");
    cpu.set_role(role);
    cpu.set_type(Value::SCALAR);
    cpu.mutable_scalar()->set_value(taskResources._cpus);

    resources += cpu;

    Resource mem;
    mem.set_name("mem");
    mem.set_role(role);
    mem.set_type(Value::SCALAR);
    mem.mutable_scalar()->set_value(taskResources._mem);

    resources += mem;

    Resource disk;
    disk.set_name("disk");
    disk.set_role(role);
    disk.set_type(Value::SCALAR);
    disk.mutable_scalar()->set_value(taskResources._disk);

    resources += disk;

    Resource ports;
    ports.set_name("ports");
    ports.set_role(role);
    ports.set_type(Value::RANGES);

    Value_Range* range;

    range = ports.mutable_ranges()->add_range();
    range->set_begin(taskResources._usedPorts.at(0));
    range->set_end(taskResources._usedPorts.at(0));

    range = ports.mutable_ranges()->add_range();
    range->set_begin(taskResources._usedPorts.at(1));
    range->set_end(taskResources._usedPorts.at(1));

    resources += ports;

    return resources;
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                                    class Instance
// -----------------------------------------------------------------------------

struct Instance {
  public:
    uint64_t _taskId;
    InstanceState _state;
    InstanceType _type;
    UsedResources _resources;
    string _slaveId;
    system_clock::time_point _started;
    system_clock::time_point _lastUpdate;
};

// -----------------------------------------------------------------------------
// --SECTION--                                                     class Aspects
// -----------------------------------------------------------------------------

class Aspects {
  public:
    size_t _plannedInstances;
    size_t _minimumInstances;

    size_t _startedInstances;
    size_t _runningInstances;

    set<string> _offers;
    set<uint64_t> _instances;
    unordered_map<string, set<uint64_t>> _slaves;

    ArangoManager::Resources _minimumResources;
};

// -----------------------------------------------------------------------------
// --SECTION--                                           class ArangoManagerImpl
// -----------------------------------------------------------------------------

class arangodb::ArangoManagerImpl {
  public:
    ArangoManagerImpl (const string& role, ArangoScheduler* _scheduler);

  public:
    void dispatch ();
    void addOffer (const Offer& offer);
    void removeOffer (const OfferID& offerId);
    void statusUpdate (uint64_t, InstanceState);
    vector<Offer> currentOffers ();

  private:
    void removeOffer (const string& offerId);
    void checkAgencyInstances ();
    Offer findAgencyOffer (bool&);
    void startAgencyInstance (const Offer&, const UsedResources&);

    void taskRunning (uint64_t);
    void taskFinished (uint64_t);

    bool checkOfferAgency (const Offer& offer);
    bool checkOfferCoordinator (const Offer& offer);
    bool checkOfferDBServer (const Offer& offer);

    Aspects* aspectsByType (InstanceType);

  public:
    const string _role;
    ArangoScheduler* _scheduler;
    mutex _lock;
    atomic<bool> _stopDispatcher;

    Aspects _agency;
    Aspects _coordinator;
    Aspects _dbserver;

  private:
    unordered_map<string, Offer> _offers;
    unordered_map<uint64_t, Instance> _instances;
};

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

ArangoManagerImpl::ArangoManagerImpl (const string& role, ArangoScheduler* scheduler)
  : _role(role),
    _scheduler(scheduler),
    _lock(),
    _stopDispatcher(false),
    _agency(),
    _coordinator(),
    _dbserver(),
    _offers() {

  // TODO(fc) how to persist & change these values

  // fill the defaults for an agency
  _agency._plannedInstances = 3;
  _agency._minimumInstances = 1;
  _agency._minimumResources = ArangoManager::Resources(0.2, 10, 10, 2);

  // fill the defaults for a coordinator
  _coordinator._plannedInstances = 4;
  _coordinator._minimumInstances = 1;
  _coordinator._minimumResources = ArangoManager::Resources(4, 2048, 1024, 1);

  // fill the defaults for a DBserver
  _coordinator._plannedInstances = 2;
  _coordinator._minimumInstances = 2;
  _coordinator._minimumResources = ArangoManager::Resources(2, 1024, 4096, 1);
}

// -----------------------------------------------------------------------------
// --SECTION--                                                  public functions
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief main dispatcher
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::dispatch () {
  static const int SLEEP_SEC = 10;

  while (! _stopDispatcher) {
    LOG(INFO) << "DISPATCHER checking state\n";

    {
      lock_guard<mutex> lock(_lock);

      checkAgencyInstances();
    }

    this_thread::sleep_for(chrono::seconds(SLEEP_SEC));
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief adds an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::addOffer (const Offer& offer) {
  lock_guard<mutex> lock(_lock);

  string const& id = offer.id().value();

  LOG(INFO)
  << "OFFER received: " << id << ": " << offer.resources() << "\n";

  bool agency = checkOfferAgency(offer);
  bool coordinator = checkOfferCoordinator(offer);
  bool dbserver = checkOfferDBServer(offer);

  if (! dbserver && ! coordinator && ! agency) {
    LOG(INFO)
    << "OFFER ignored: " << id << ": " << offer.resources() << "\n";
    return;
  }

  _offers.insert({ id, offer });

  if (agency) {
    _agency._offers.insert(id);
  }

  if (coordinator) {
    _coordinator._offers.insert(id);
  }

  if (dbserver) {
    _dbserver._offers.insert(id);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief removes an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::removeOffer (const OfferID& offerId) {
  lock_guard<mutex> lock(_lock);

  removeOffer(offerId.value());
}

////////////////////////////////////////////////////////////////////////////////
/// @brief status update
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::statusUpdate (uint64_t taskId,
                                      InstanceState state) {
  lock_guard<mutex> lock(_lock);

  if (state == InstanceState::RUNNING) {
    taskRunning(taskId);
  }
  else if (state == InstanceState::FINISHED) {
    taskFinished(taskId);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the current offers
////////////////////////////////////////////////////////////////////////////////

vector<Offer> ArangoManagerImpl::currentOffers () {
  vector<Offer> result;

  {
    lock_guard<mutex> lock(_lock);

    for (auto offer : _offers) {
      result.push_back(offer.second);
    }
  }

  return result;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                 private functions
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief removes an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::removeOffer (const string& id) {
  _offers.erase(id);
  _agency._offers.erase(id);
  _coordinator._offers.erase(id);
  _dbserver._offers.erase(id);

  LOG(INFO)
  << "OFFER removed: " << id << "\n";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if need to start a new agency instance
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::checkAgencyInstances () {
  size_t planned = _agency._plannedInstances;

  LOG(INFO)
  << "AGENCY "
  << _agency._runningInstances << " running instances, "
  << _agency._startedInstances << " started instances, "
  << planned << " planned instances\n";

  if (planned <= _agency._runningInstances) {
    return;
  }

  if (planned <= _agency._runningInstances + _agency._startedInstances) {
    // TODO(fc) do we need to add a timeout?
    return;
  }

  // TODO(fc) need to check that we always have a cluster of agency
  // which knows the master plan. If all instances fail, we need to restart at
  // least an instance which has access to the persistent volume!

  bool found;
  const Offer offer = findAgencyOffer(found);

  if (! found) {
    LOG(INFO) << "AGENCY cannot find a suitable resource for missing agency\n";
    return;
  }

  // TODO(fc) do we need prefered resources in addition in minimum?

  // set resources
  UsedResources taskResources;

  taskResources._cpus = _agency._minimumResources._cpus;
  taskResources._mem = _agency._minimumResources._mem;
  taskResources._disk = _agency._minimumResources._disk;
  taskResources._ports = 2;
  taskResources._usedPorts = findFreePorts(offer, taskResources._ports);

  if (taskResources._usedPorts.size() != 2) {
    LOG(WARNING) << "AGENCY found corrupt offer, this should not happen\n";
    return;
  }

  // try to start a new instance
  startAgencyInstance(offer, taskResources);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief finds a suitable offer for an agency
////////////////////////////////////////////////////////////////////////////////

Offer ArangoManagerImpl::findAgencyOffer (bool& found) {
  Offer result;

  found = false;

  if (_agency._offers.empty()) {
    return result;
  }

  // TODO(fc) need a better selection algorithm
  string offerId;

  for (const auto& id : _agency._offers) {
    auto iter = _offers.find(id);
    
    if (iter == _offers.end()) {
      continue;
    }

    const Offer& offer = iter->second;
    string slaveId = offer.slave_id().value();

    LOG(INFO)
    << "testing offer " << id << " on slave " << slaveId << "\n";

    auto jter = _agency._slaves.find(slaveId);

    if (jter != _agency._slaves.end() && ! jter->second.empty()) {
      continue;
    }

    found = true;
    result = iter->second;
    offerId = id;

    break;
  }

  if (found) {
    removeOffer(offerId);
  }

  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief starts a new agency
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::startAgencyInstance (
    const Offer& offer,
    const UsedResources& taskResources) {

  string const& id = offer.id().value();
  Resources resources = convertUsedResources(taskResources, _role);

  LOG(INFO)
  << "AGENCY about to start agency using offer " << id 
  << " with resources " << resources << "\n";

  uint64_t taskId = _scheduler->startAgencyInstance(offer, resources);

  Instance desc;

  desc._taskId = taskId;
  desc._type = InstanceType::AGENCY;
  desc._state = InstanceState::STARTED;
  desc._resources = taskResources;
  desc._slaveId = offer.slave_id().value();
  desc._started = system_clock::now();
  desc._lastUpdate = system_clock::time_point();

  _instances.insert({ taskId, desc });
  _agency._instances.insert(taskId);
  _agency._slaves[desc._slaveId].insert(taskId);

  ++(_agency._startedInstances);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief status update (runing)
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::taskRunning (uint64_t taskId) {
  auto iter = _instances.find(taskId);

  if (iter == _instances.end()) {
    return;
  }

  Instance& instance = iter->second;

  if (instance._state == InstanceState::RUNNING) {
    return;
  }

  if (instance._state != InstanceState::STARTED) {
    LOG(WARNING) << "INSTANCE is not STARTED, but got RUNNING (for "
                 << taskId << "), ignoring\n";
    return;
  }

  Aspects* aspects = aspectsByType(instance._type);

  LOG(INFO)
  << "INSTANCE changing state from " << int(instance._state)
  << " to RUNNING for " << taskId << "\n";

  instance._state = InstanceState::RUNNING;

  --(aspects->_startedInstances);
  ++(aspects->_runningInstances);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief status update (finished)
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::taskFinished (uint64_t taskId) {
  auto iter = _instances.find(taskId);

  if (iter == _instances.end()) {
    return;
  }

  Instance& instance = iter->second;
  InstanceState state = instance._state;

  if (state != InstanceState::STARTED && state != InstanceState::RUNNING) {
    return;
  }

  Aspects* aspects = aspectsByType(instance._type);

  LOG(INFO)
  << "INSTANCE changing state from " << int(instance._state)
  << " to RUNNING for " << taskId << "\n";

  if (state == InstanceState::STARTED && 0 < aspects->_startedInstances) {
    --(aspects->_startedInstances);
  }
  else if (state == InstanceState::RUNNING && 0 < aspects->_runningInstances) {
    --(aspects->_runningInstances);
  }

  instance._state = InstanceState::FINISHED;

  auto& iter = agency->_slaves.find(instance._slaveId);

  if (iter != agency->end()) {
    iter.erase(taskId);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if offer is suitable for agency
////////////////////////////////////////////////////////////////////////////////

bool ArangoManagerImpl::checkOfferAgency (const Offer& offer) {
  return checkOfferAgainstResources(
    offer, _agency._minimumResources, "agency");
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if offer is suitable for coordinator
////////////////////////////////////////////////////////////////////////////////

bool ArangoManagerImpl::checkOfferCoordinator (const Offer& offer) {
  return checkOfferAgainstResources(
    offer, _coordinator._minimumResources, "coordinator");
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if offer is suitable for DBserver
////////////////////////////////////////////////////////////////////////////////

bool ArangoManagerImpl::checkOfferDBServer (const Offer& offer) {
  return checkOfferAgainstResources(
    offer, _dbserver._minimumResources, "DBserver");
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the aspects for the type
////////////////////////////////////////////////////////////////////////////////

Aspects* ArangoManagerImpl::aspectsByType (InstanceType type) {
  Aspects* aspects = nullptr;

  switch (type) {
    case InstanceType::AGENCY: return &_agency;
    case InstanceType::COORDINATOR: return &_coordinator;
    case InstanceType::DBSERVER: return &_dbserver;
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                               class ArangoManager
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

ArangoManager::ArangoManager (const string& role, ArangoScheduler* scheduler)
  : _impl(nullptr),
    _dispatcher(nullptr) {
  _impl = new ArangoManagerImpl(role, scheduler);
  _dispatcher = new thread(&ArangoManagerImpl::dispatch, _impl);
};

////////////////////////////////////////////////////////////////////////////////
/// @brief destructor
////////////////////////////////////////////////////////////////////////////////

ArangoManager::~ArangoManager () {
  _impl->_stopDispatcher = true;
  _dispatcher->join();
  delete _impl;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief checks and adds an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::addOffer (const Offer& offer) {
  _impl->addOffer(offer);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief removes an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::removeOffer (const OfferID& offerId) {
  _impl->removeOffer(offerId);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief status update
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::statusUpdate (uint64_t taskId, InstanceState state) {
  _impl->statusUpdate(taskId, state);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns planned number of agency instances
////////////////////////////////////////////////////////////////////////////////

size_t ArangoManager::agencyInstances () {
  lock_guard<mutex> lock(_impl->_lock);
  return _impl->_agency._plannedInstances;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns planned number of coordinator instances
////////////////////////////////////////////////////////////////////////////////

size_t ArangoManager::coordinatorInstances () {
  lock_guard<mutex> lock(_impl->_lock);
  return _impl->_coordinator._plannedInstances;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns planned number of dbserver instances
////////////////////////////////////////////////////////////////////////////////

size_t ArangoManager::dbserverInstances () {
  lock_guard<mutex> lock(_impl->_lock);
  return _impl->_dbserver._plannedInstances;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns minimum resources for agency
////////////////////////////////////////////////////////////////////////////////

ArangoManager::Resources ArangoManager::agencyResources () {
  lock_guard<mutex> lock(_impl->_lock);
  return _impl->_agency._minimumResources;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns minimum resources for coordinator
////////////////////////////////////////////////////////////////////////////////

ArangoManager::Resources ArangoManager::coordinatorResources () {
  lock_guard<mutex> lock(_impl->_lock);
  return _impl->_coordinator._minimumResources;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns minimum resources for DBserver
////////////////////////////////////////////////////////////////////////////////

ArangoManager::Resources ArangoManager::dbserverResources () {
  lock_guard<mutex> lock(_impl->_lock);
  return _impl->_dbserver._minimumResources;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the current offers
////////////////////////////////////////////////////////////////////////////////

vector<Offer> ArangoManager::currentOffers () {
  return _impl->currentOffers();
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------

// Local Variables:
// mode: outline-minor
// outline-regexp: "/// @brief\\|/// {@inheritDoc}\\|/// @page\\|// --SECTION--\\|/// @\\}"
// End:
