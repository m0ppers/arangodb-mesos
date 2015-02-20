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
#include <iostream>
#include <set>
#include <unordered_map>

#include "ArangoScheduler.h"

using namespace mesos;
using namespace arangodb;

using std::chrono::system_clock;
using InstanceState = ArangoManager::InstanceState;
using InstanceType = ArangoManager::InstanceType;
using Instance = ArangoManager::Instance;

// -----------------------------------------------------------------------------
// --SECTION--                                                  helper functions
// -----------------------------------------------------------------------------

namespace {
  bool notIsPorts (const Resource& resource) {
    return resource.name() != "ports";
  }

  size_t numberPorts (const Offer& offer) {
    size_t value = 0;

    for (size_t i = 0; i < offer.resources_size(); ++i) {
      const auto& resource = offer.resources(i);

      if (resource.name() == "ports" &&
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


  Resources resourcesPorts (const vector<uint32_t>& ports) {
    Resources resources;

    Resource res;
    res.set_name("ports");
    res.set_type(Value::RANGES);

    for (uint32_t p : ports) {
      Value_Range* range = res.mutable_ranges()->add_range();

      range->set_begin(p);
      range->set_end(p);
    }

    resources += res;

    return resources;
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                                 struct StartOffer
// -----------------------------------------------------------------------------

namespace {
  struct StartOffer {
    bool _usable;
    Offer _offer;
    Resources _resources;
    vector<uint32_t> _ports;
  };
}

// -----------------------------------------------------------------------------
// --SECTION--                                                     class Aspects
// -----------------------------------------------------------------------------

class Aspects {
  public:
    Aspects (const string& name, const string& role)
      : _name(name), _role(role) {
    }

  public:
    Resources minimumResources () {
      return _minimumResources;
    }

    Resources minimumResourcesRole () {
      return _minimumResources.flatten(_role, Resource::FRAMEWORK);
    }

    bool checkMinimum (const Offer& offer) {
      Resources resources = offer.resources();

      if (numberPorts(offer) < _requiredPorts) {
        LOG(INFO) 
        << "DEBUG " << resources << " does not have " 
        << _requiredPorts << " ports";

        return false;
      }

      Resources mr = minimumResourcesRole();

      if (resources.contains(mr)) {
        LOG(INFO) << "DEBUG " << resources << " contains " << mr;
        return true;
      }

      Resources m = minimumResources();

      if (resources.contains(m)) {
        LOG(INFO) << "DEBUG " << resources << " contains " << m;
        return true;
      }

      LOG(INFO)
      << "DEBUG minimum " << m << " or " << mr << " not contained in "
      << resources;

      return false;
    }

  public:
    const string _name;

    size_t _plannedInstances;
    size_t _minimumInstances;

    size_t _startedInstances;
    size_t _runningInstances;

    Resources _minimumResources;
    Resources _additionalResources;

    size_t _requiredPorts;

    set<string> _offers;
    set<uint64_t> _instances;
    unordered_map<string, set<uint64_t>> _slaves;

  private:
    const string _role;
};

// -----------------------------------------------------------------------------
// --SECTION--                                               class AgencyAspects
// -----------------------------------------------------------------------------

class AgencyAspects : public Aspects {
  public:
    AgencyAspects (const string& role) 
      : Aspects("AGENCY", role) {
      _minimumInstances = 1;
      _plannedInstances = 3;

      _startedInstances = 0;
      _runningInstances = 0;

      _minimumResources = Resources::parse("cpus:1;mem:100;disk:100").get();
      _requiredPorts = 2;
    }
};

// -----------------------------------------------------------------------------
// --SECTION--                                          class CoordinatorAspects
// -----------------------------------------------------------------------------

class CoordinatorAspects : public Aspects {
  public:
    CoordinatorAspects (const string& role) 
      : Aspects("COORDINATOR", role) {
      _minimumInstances = 1;
      _plannedInstances = 4;

      _startedInstances = 0;
      _runningInstances = 0;

      _minimumResources = Resources::parse("cpus:4;mem:1024;disk:1024").get();
      _requiredPorts = 1;
    }
};

// -----------------------------------------------------------------------------
// --SECTION--                                            class DBServersAspects
// -----------------------------------------------------------------------------

class DBServerAspects : public Aspects {
  public:
    DBServerAspects (const string& role) 
      : Aspects("DBSERVER", role) {
      _minimumInstances = 2;
      _plannedInstances = 2;

      _startedInstances = 0;
      _runningInstances = 0;

      _minimumResources = Resources::parse("cpus:2;mem:1024;disk:2048").get();
      _requiredPorts = 1;
    }
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
    vector<Instance> currentInstances ();

  private:
    void removeOffer (const string& offerId);

    void checkInstances (Aspects&);
    StartOffer findOffer (Aspects&);
    StartOffer findReservedOffer (Aspects&);
    void findUnreservedOffer (Aspects&);

    void startInstance (Aspects&, StartOffer&);

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

    AgencyAspects _agency;
    CoordinatorAspects _coordinator;
    DBServerAspects _dbserver;

  private:
    unordered_map<string, Offer> _reservedOffers;
    unordered_map<string, Offer> _unreservedOffers;

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
    _stopDispatcher(false),
    _agency(role),
    _coordinator(role),
    _dbserver(role) {

  // TODO(fc) how to persist & change these values
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

      checkInstances(_agency);
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

  // basic check if offer is suitable
  vector<Aspects> aspects = { _agency };

  if (none_of(aspects.begin(), aspects.end(),
              [&] (Aspects& a) -> bool {return a.checkMinimum(offer);})) {

    LOG(INFO)
    << "OFFER declined, because minimum resource not satisfied";

    _scheduler->declineOffer(offer.id());

    return;
  }

  Resources reserved = filter(
    static_cast<bool (*)(const Resource&)>(Resources::isReserved),
    offer.resources());

  reserved = filter(notIsPorts, reserved);

  if (reserved.empty()) {
    _unreservedOffers.insert({ id, offer });
  }
  else {
    _reservedOffers.insert({ id, offer });
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
/// @brief returns the current offers for debugging
////////////////////////////////////////////////////////////////////////////////

vector<Offer> ArangoManagerImpl::currentOffers () {
  vector<Offer> result;

  {
    lock_guard<mutex> lock(_lock);

    for (const auto& offer : _offers) {
      result.push_back(offer.second);
    }
  }

  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the current offers for debugging
////////////////////////////////////////////////////////////////////////////////

vector<Instance> ArangoManagerImpl::currentInstances () {
  vector<Instance> result;

  {
    lock_guard<mutex> lock(_lock);

    for (const auto& instance : _instances) {
      result.push_back(instance.second);
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

void ArangoManagerImpl::checkInstances (Aspects& aspect) {
  size_t planned = aspect._plannedInstances;

  LOG(INFO)
  << aspect._name << " "
  << aspect._runningInstances << " running instances, "
  << aspect._startedInstances << " started instances, "
  << planned << " planned instances\n";

  if (planned <= aspect._runningInstances) {
    return;
  }

  if (planned <= aspect._runningInstances + aspect._startedInstances) {
    // TODO(fc) do we need to add a timeout?
    return;
  }

  // TODO(fc) need to check that we always have a cluster of agency
  // which knows the master plan. If all instances fail, we need to restart at
  // least an instance which has access to the persistent volume!

  StartOffer offer = findOffer(aspect);

  if (! offer._usable) {
    LOG(INFO) << "AGENCY cannot find a suitable resource for missing agency\n";
    return;
  }

  // try to start a new instance
  startInstance(aspect, offer);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief finds a suitable offer for an agency
////////////////////////////////////////////////////////////////////////////////

StartOffer ArangoManagerImpl::findOffer (Aspects& aspect) {

  // first check the reserved offers
  StartOffer result = findReservedOffer(aspect);

  if (result._usable) {
    return result;
  }

  // found no reserved offer, check the unreserved one and try to reserve one
  findUnreservedOffer(aspect);

  return { false };
}

////////////////////////////////////////////////////////////////////////////////
/// @brief finds a suitable reserved offer for an agency
////////////////////////////////////////////////////////////////////////////////

StartOffer ArangoManagerImpl::findReservedOffer (Aspects& aspect) {
  Resources resources = aspect.minimumResourcesRole();

  for (auto& id_offer : _reservedOffers) {
    Resources r = id_offer.second.resources();

    if (r.contains(resources)) {
      auto ports = findFreePorts(id_offer.second, aspect._requiredPorts);

      if (ports.size() == aspect._requiredPorts) {
        return { true, id_offer.second, resources, ports };
      }
    }
  }

  return { false };
}

////////////////////////////////////////////////////////////////////////////////
/// @brief finds a suitable unreserved offer for an agency
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::findUnreservedOffer (Aspects& aspect) {
  Resources resources = aspect.minimumResources();

  for (auto& id_offer : _unreservedOffers) {
    Offer& offer = id_offer.second;
    Resources r = offer.resources();

    if (r.contains(resources) && numberPorts(offer) >= aspect._requiredPorts) {
      resources = resources.flatten(_role, Resource::FRAMEWORK);

      LOG(INFO)
      << aspect._name << " "
      << "trying to reserve " << r 
      << " with " << resources;

      _scheduler->reserveDynamically(id_offer.second, resources);
      _unreservedOffers.erase(id_offer.first);
      return;
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief starts a new agency
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::startInstance (Aspects& aspects, StartOffer& offer) {
  string const& id = offer._offer.id().value();

  Resources resources = offer._resources;
  resources += resourcesPorts(offer._ports);

  LOG(INFO)
  << "AGENCY about to start agency using offer " << id 
  << " with resources " << resources << "\n";

  uint64_t taskId = _scheduler->startAgencyInstance(offer._offer, resources);

  Instance desc;

  desc._taskId = taskId;
  desc._type = InstanceType::AGENCY;
  desc._state = InstanceState::STARTED;
  desc._resources = resources;
  desc._slaveId = offer._offer.slave_id().value();
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
  const auto& iter = _instances.find(taskId);

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
  << "INSTANCE changing state from " << ArangoManager::stringInstanceState(instance._state)
  << " to RUNNING for " << taskId << "\n";

  instance._state = InstanceState::RUNNING;

  --(aspects->_startedInstances);
  ++(aspects->_runningInstances);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief status update (finished)
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::taskFinished (uint64_t taskId) {
  const auto& iter = _instances.find(taskId);

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
  << "INSTANCE changing state from " << ArangoManager::stringInstanceState(instance._state)
  << " to FINISHED for " << taskId << "\n";

  if (state == InstanceState::STARTED && 0 < aspects->_startedInstances) {
    --(aspects->_startedInstances);
  }
  else if (state == InstanceState::RUNNING && 0 < aspects->_runningInstances) {
    --(aspects->_runningInstances);
  }

  instance._state = InstanceState::FINISHED;

  const auto& jter = aspects->_slaves.find(instance._slaveId);

  if (jter != aspects->_slaves.end()) {
    aspects->_slaves.erase(jter);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if offer is suitable for agency
////////////////////////////////////////////////////////////////////////////////

bool ArangoManagerImpl::checkOfferAgency (const Offer& offer) {
  return false;
/*
  return checkOfferAgainstResources(
    offer, _agency._minimumResources, "agency");
*/
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if offer is suitable for coordinator
////////////////////////////////////////////////////////////////////////////////

bool ArangoManagerImpl::checkOfferCoordinator (const Offer& offer) {
  return false;
/*
  return checkOfferAgainstResources(
    offer, _coordinator._minimumResources, "coordinator");
*/
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if offer is suitable for DBserver
////////////////////////////////////////////////////////////////////////////////

bool ArangoManagerImpl::checkOfferDBServer (const Offer& offer) {
  return false;
/*
  return checkOfferAgainstResources(
    offer, _dbserver._minimumResources, "DBserver");
*/
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

/*
ArangoManager::BasicResources ArangoManager::agencyResources () {
  lock_guard<mutex> lock(_impl->_lock);
//  return _impl->_agency._minimumResources;
}
*/

////////////////////////////////////////////////////////////////////////////////
/// @brief returns minimum resources for coordinator
////////////////////////////////////////////////////////////////////////////////

/*
ArangoManager::BasicResources ArangoManager::coordinatorResources () {
  lock_guard<mutex> lock(_impl->_lock);
//  return _impl->_coordinator._minimumResources;
}
*/

////////////////////////////////////////////////////////////////////////////////
/// @brief returns minimum resources for DBserver
////////////////////////////////////////////////////////////////////////////////

/*
ArangoManager::BasicResources ArangoManager::dbserverResources () {
  lock_guard<mutex> lock(_impl->_lock);
//  return _impl->_dbserver._minimumResources;
}
*/

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the current offers for debugging
////////////////////////////////////////////////////////////////////////////////

vector<Offer> ArangoManager::currentOffers () {
  return _impl->currentOffers();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the current instances for debugging
////////////////////////////////////////////////////////////////////////////////

vector<Instance> ArangoManager::currentInstances () {
  return _impl->currentInstances();
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------

// Local Variables:
// mode: outline-minor
// outline-regexp: "/// @brief\\|/// {@inheritDoc}\\|/// @page\\|// --SECTION--\\|/// @\\}"
// End:
