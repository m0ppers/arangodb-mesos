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
      cout << "OFFER declined for " << name << ": not enough CPU power "
           << "(got " << cpus << ", need " << resources._cpus << ")\n";
      return false;
    }

    double mem = scalarResource(offer, "mem");

    if (mem < resources._mem) {
      cout << "OFFER declined for " << name << ": not enough memory "
           << "(got " << mem << ", need " << resources._mem << ")\n";
      return false;
    }

    double disk = scalarResource(offer, "disk");

    if (disk < resources._disk) {
      cout << "OFFER declined for " << name << ": not enough disk "
           << "(got " << disk << ", need " << resources._disk << ")\n";
      return false;
    }

    size_t ports = rangeResource(offer, "ports");

    if (ports < resources._ports) {
      cout << "OFFER declined for " << name << ": not enough ports "
           << "(got " << ports << ", need " << resources._ports << ")\n";
      return false;
    }

    cout << "OFFER suitable for " << name << ": "
         << "cpus:" << cpus << ", "
         << "mem:" << mem << ", "
         << "disk:" << disk << ", "
         << "ports:" << ports << "\n";

    return true;
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                          enum class InstanceState
// -----------------------------------------------------------------------------

enum class InstanceState {
  STARTED,
  RUNNING,
  TERMINATED,
  LOST
};

// -----------------------------------------------------------------------------
// --SECTION--                                                    class Instance
// -----------------------------------------------------------------------------

class Instance {
  public:
    InstanceState _state;
    system_clock::time_point _start;
    system_clock::time_point _stop;
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
    unordered_map<string, Instance> _instances;

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
    vector<Offer> currentOffers ();

  private:
    void removeOffer (const string& offerId);
    void checkAgencyInstances ();
    Offer findAgencyOffer (bool&);
    void startAgencyInstance (const Offer&, const Resources&);

    bool checkOfferAgency (const Offer& offer);
    bool checkOfferCoordinator (const Offer& offer);
    bool checkOfferDBServer (const Offer& offer);

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
    cout << "DISPATCHER checking state\n";

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

  cout << "OFFER received: " << id << ": " << offer.resources() << "\n";

  bool agency = checkOfferAgency(offer);
  bool coordinator = checkOfferCoordinator(offer);
  bool dbserver = checkOfferDBServer(offer);

  if (! dbserver && ! coordinator && ! agency) {
    cout << "OFFER ignored: " << id << ": " << offer.resources() << "\n";
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

  cout << "OFFER stored: " << id << ": " << offer.resources() << "\n";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief removes an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::removeOffer (const OfferID& offerId) {
  lock_guard<mutex> lock(_lock);

  removeOffer(offerId.value());
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

  cout << "OFFER removed: " << id << "\n";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if need to start a new agency instance
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::checkAgencyInstances () {
  size_t planned = _agency._plannedInstances;

  cout << "AGENCY "
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
    cout << "AGENCY cannot find a suitable resource for missing agency\n";
    return;
  }

  // TODO(fc) do we need prefered resources in addition in minimum?

  // set resources
  Resources resources;

  Resource cpu;
  cpu.set_name("cpus");
  cpu.set_role(_role);
  cpu.set_type(Value::SCALAR);
  cpu.mutable_scalar()->set_value(_agency._minimumResources._cpus);

  resources += cpu;

  Resource mem;
  mem.set_name("mem");
  mem.set_role(_role);
  mem.set_type(Value::SCALAR);
  mem.mutable_scalar()->set_value(_agency._minimumResources._mem);

  resources += mem;

  Resource disk;
  disk.set_name("disk");
  disk.set_role(_role);
  disk.set_type(Value::SCALAR);
  disk.mutable_scalar()->set_value(_agency._minimumResources._disk);

  resources += disk;

  Resource ports;
  ports.set_name("ports");
  ports.set_role(_role);
  ports.set_type(Value::RANGES);

  Value_Range* range;

  range = ports.mutable_ranges()->add_range();
  range->set_begin(31000);
  range->set_end(31000);

  range = ports.mutable_ranges()->add_range();
  range->set_begin(31010);
  range->set_end(31010);

  resources += ports;

  // try to start a new instance
  startAgencyInstance(offer, resources);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief finds a suitable offer for an agency
////////////////////////////////////////////////////////////////////////////////

Offer ArangoManagerImpl::findAgencyOffer (bool& found) {
  Offer result;

  if (_agency._offers.empty()) {
    found = false;
    return result;
  }

  // TODO(fc) need a better selection algorithm

  string id = *_agency._offers.begin();
  auto iter = _offers.find(id);

  if (iter != _offers.end()) {
    found = true;
    result = iter->second;
  }

  removeOffer(id);

  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief starts a new agency
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::startAgencyInstance (const Offer& offer,
                                             const Resources& resources) {
  string const& id = offer.id().value();

  cout << "AGENCY about to start agency using offer " << id 
       << " with resources " << resources << "\n";

  _scheduler->startAgencyInstance(offer, resources);
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
