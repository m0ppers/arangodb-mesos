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
#include <unordered_set>

#include "ArangoScheduler.h"

using namespace mesos;
using namespace arangodb;

using std::chrono::system_clock;

// -----------------------------------------------------------------------------
// --SECTION--                                                  helper functions
// -----------------------------------------------------------------------------

namespace {
  bool notIsPorts (const Resource& resource) {
    return resource.name() != "ports";
  }



  bool isDisk (const Resource& resource) {
    return resource.name() == "disk";
  }



  bool notIsDisk (const Resource& resource) {
    return resource.name() != "disk";
  }



  size_t numberPorts (const Offer& offer) {
    size_t value = 0;

    for (int i = 0; i < offer.resources_size(); ++i) {
      const auto& resource = offer.resources(i);

      if (resource.name() == "ports" &&
          resource.type() == Value::RANGES) {
        const auto& ranges = resource.ranges();

        for (int j = 0; j < ranges.range_size(); ++j) {
          const auto& range = ranges.range(j);

          value += range.end() - range.begin() + 1;
        }
      }
    }

    return value;
  }



  double diskspace (const Resource& resource) {
    if (resource.name() == "disk" && resource.type() == Value::SCALAR) {
      return resource.scalar().value();
    }

    return 0;
  }

  double diskspace (const Resources& resources) {
    double value = 0;

    for (auto resource : resources) {
      value += diskspace(resource);
    }

    return value;
  }



  vector<uint32_t> findFreePorts (const Offer& offer, size_t len) {
    static const size_t MAX_ITERATIONS = 1000;

    vector<uint32_t> result;
    vector<Value::Range> resources;

    for (int i = 0; i < offer.resources_size(); ++i) {
      const auto& resource = offer.resources(i);

      if (resource.name() == "ports" &&
          resource.type() == Value::RANGES) {
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
// --SECTION--                                                     class Aspects
// -----------------------------------------------------------------------------

class Aspects {
  public:
    Aspects (const string& name, const string& role)
      : _name(name), _role(role) {
      _startedInstances = 0;
      _runningInstances = 0;

    }

  public:
    virtual size_t id () const = 0;
    virtual OfferAnalysis analyseOffer (const Offer& offer) = 0;

  public:
    const string _name;
    const string _role;

    Resources _minimumResources;
    Resources _additionalResources;

    bool _persistentVolumeRequired;
    size_t _requiredPorts;

    size_t _plannedInstances;
    size_t _minimumInstances;

  public:
    size_t _startedInstances;
    size_t _runningInstances;

  public:
    unordered_set<string> _blockedSlaves;
    unordered_set<string> _startedSlaves;
   unordered_set<string> _preferredSlaves;
};

// -----------------------------------------------------------------------------
// --SECTION--                                               class AgencyAspects
// -----------------------------------------------------------------------------

class AgencyAspects : public Aspects {
  public:
    AgencyAspects (const string& role) 
      : Aspects("AGENCY", role) {
      _minimumResources = Resources::parse("cpus:1;mem:100;disk:100").get();
      _additionalResources = Resources();
      _persistentVolumeRequired = true;
      _requiredPorts = 2;

      _minimumInstances = 1;
      _plannedInstances = 1;
    }

  public:
    size_t id () const override {
      return static_cast<size_t>(AspectsId::ID_AGENCY);
    }

    OfferAnalysis analyseOffer (const Offer& offer) override {
      Resources resources = offer.resources();

      // first check if this slave is already known
      string slaveId = offer.slave_id().value();

      if (_blockedSlaves.find(slaveId) != _blockedSlaves.end()) {
        LOG(INFO)
        << _name << " has blocked slave " << slaveId;

        return { OfferAnalysisType::NOT_USABLE };
      }

      if (_startedSlaves.find(slaveId) != _startedSlaves.end()) {
        LOG(INFO)
        << _name << " has already an instance started on slave " << slaveId;

        return { OfferAnalysisType::NOT_USABLE };
      }

      if (_preferredSlaves.size() >= _plannedInstances) {
        if (_preferredSlaves.find(slaveId) == _preferredSlaves.end()) {
          LOG(INFO)
          << _name << slaveId << " is not a preferred slave";

          return { OfferAnalysisType::NOT_USABLE };
        }
      }

      // first check the number of ports
      if (numberPorts(offer) < _requiredPorts) {
        LOG(INFO) 
        << "DEBUG " << resources << " does not have " 
        << _requiredPorts << " ports";

        return { OfferAnalysisType::NOT_USABLE };
      }

      // next check non-disk parts
      bool reservationRequired = false;

      Resources ndisk = _minimumResources.filter(notIsDisk);
      Option<Resources> found = resources.find(ndisk.flatten(_role, Resource::FRAMEWORK));

      if (found.isNone()) {
        LOG(INFO) 
        << "DEBUG " << resources << " does not have " 
        << ndisk << " requirements";

        return { OfferAnalysisType::NOT_USABLE };
      }

      Resources reserved = found.get();
      Resources unreserved = reserved.filter(Resources::isUnreserved);

      cout << "==== ndisk " << ndisk << "\n";
      cout << "==== reserved " << reserved << "\n";
      cout << "==== unreserved " << unreserved << "\n";

      if (! unreserved.empty()) {
        reservationRequired = true;
      }

      // next check the disk part
      Resources mdisk = _minimumResources.filter(isDisk);
      size_t mds = diskspace(mdisk);
      Resources odisk = resources.filter(isDisk);

      // first check for already persistent resources
      for (auto res : odisk) {
        cout << "DEBUG checking " << res << " for persistent" << endl;

        if (res.role() != _role) {
          cout << "DEBUG skip wrong role " << _role << endl;
          continue;
        }

        if (diskspace(res) < mds) {
          cout << "DEBUG too small " << mds << endl;
          continue;
        }

        if (! res.has_disk()) {
          cout << "DEBUG skip no disk" << endl;
          continue;
        }

        if (! res.disk().has_persistence()) {
          cout << "DEBUG skip no persistence" << endl;
          continue;
        }

        string diskId = res.disk().persistence().id();

        if (diskId.find("AGENCY:") != 0) {
          cout << "DEBUG skip worng id" << endl;
          continue;
        }

        _blockedSlaves.insert(slaveId);

        cout << "could work" << endl;

        if (reservationRequired) {
          return { OfferAnalysisType::DYNAMIC_RESERVATION_REQUIRED, unreserved };
        }
        else {
          return { OfferAnalysisType::USABLE, reserved + res };
        }
      }

      // next check for reserved resources
      for (auto res : odisk) {
        cout << "DEBUG checking " << res << " for reserved" << endl;

        if (res.role() != _role) {
          cout << "DEBUG ignored, wrong role " << res.role() << endl;
          continue;
        }

        if (diskspace(res) < mds) {
          cout << "DEBUG too small " << mds << endl;
          continue;
        }

        cout << "could work" << endl;

        if (reservationRequired) {
          return { OfferAnalysisType::DYNAMIC_RESERVATION_REQUIRED, unreserved };
        }
        else {
          return { OfferAnalysisType::PERSISTENT_VOLUME_REQUIRED, Resources() + res };
        }
      }

      // at last, try to find an unreserved resource
      for (auto res : odisk) {
        cout << "DEBUG checking " << res << " for unreserved" << endl;

        if (res.role() != "*") {
          cout << "DEBUG ignored, wrong role " << res.role() << endl;
          continue;
        }

        if (diskspace(res) < mds) {
          cout << "DEBUG too small " << mds << endl;
          continue;
        }

        cout << "could work" << endl;

        return { OfferAnalysisType::DYNAMIC_RESERVATION_REQUIRED, unreserved + mdisk };
      }

      cout << "could not work" << endl;

      return { OfferAnalysisType::NOT_USABLE };
    }
};

// -----------------------------------------------------------------------------
// --SECTION--                                          class CoordinatorAspects
// -----------------------------------------------------------------------------

class CoordinatorAspects : public Aspects {
  public:
    CoordinatorAspects (const string& role) 
      : Aspects("COORDINATOR", role) {
      _minimumResources = Resources::parse("cpus:4;mem:1024;disk:1024").get();
      _additionalResources = Resources();
      _persistentVolumeRequired = false;
      _requiredPorts = 1;

      _minimumInstances = 1;
      _plannedInstances = 3;
    }

  public:
    size_t id () const override {
      return static_cast<size_t>(AspectsId::ID_COORDINATOR);
    }

    OfferAnalysis analyseOffer (const Offer& offer) override {
      return OfferAnalysis();
    }
};

// -----------------------------------------------------------------------------
// --SECTION--                                            class DBServersAspects
// -----------------------------------------------------------------------------

class DBServerAspects : public Aspects {
  public:
    DBServerAspects (const string& role) 
      : Aspects("DBSERVER", role) {
      _minimumResources = Resources::parse("cpus:2;mem:1024;disk:2048").get();
      _additionalResources = Resources();
      _persistentVolumeRequired = true;
      _requiredPorts = 1;

      _minimumInstances = 2;
      _plannedInstances = 2;
    }

  public:
    size_t id () const override {
      return static_cast<size_t>(AspectsId::ID_DBSERVER);
    }

    OfferAnalysis analyseOffer (const Offer& offer) override {
      return OfferAnalysis();
    }
};

// -----------------------------------------------------------------------------
// --SECTION--                                        class AspectInstanceStatus
// -----------------------------------------------------------------------------

enum class AspectInstanceStatus {
  DYNAMIC_RESERVATION_REQUESTED,
  PERSISTENT_VOLUME_REQUESTED,
  TASK_STARTED
};

// -----------------------------------------------------------------------------
// --SECTION--                                              class AspectInstance
// -----------------------------------------------------------------------------

class AspectInstance {
  public:
    AspectInstanceStatus _status;
    OfferID _offerId;
    chrono::system_clock::time_point _started;
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
    vector<OfferSummary> currentOffers ();
    vector<Instance> currentInstances ();

  private:
    void removeOffer (const string& offerId);

    void checkInstances (Aspects&);
    void makePersistentVolume (const string&, const Offer&, const Resources&) const;
    void makeDynamicReservation(const string&, const Offer&, const Resources&) const;
    OfferSummary findOffer (Aspects&);

    void startInstance (Aspects&, const Offer&, const OfferAnalysis&);

    void taskRunning (uint64_t);
    void taskFinished (uint64_t);

  public:
    const string _role;
    ArangoScheduler* _scheduler;
    mutex _lock;
    atomic<bool> _stopDispatcher;

    AgencyAspects _agency;
    CoordinatorAspects _coordinator;
    DBServerAspects _dbserver;

  private:
    vector<Aspects*> _aspects;

    unordered_map<string, OfferSummary> _offers;
    unordered_map<uint64_t, Instance> _instances;
};

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

ArangoManagerImpl::ArangoManagerImpl (const string& role,
                                      ArangoScheduler* scheduler)
  : _role(role),
    _scheduler(scheduler),
    _stopDispatcher(false),
    _agency(role),
    _coordinator(role),
    _dbserver(role) {

  // TODO(fc) how to persist & change these values

  _aspects = { &_agency /* , _coordinator, _dbserver */ };
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

  // check if offer is suitable for "something"
  OfferSummary summary = { false, offer };

  for (auto& aspect : _aspects) {
    OfferAnalysis oa = aspect->analyseOffer(offer);

    if (oa._status != OfferAnalysisType::NOT_USABLE) {
      summary._usable = true;
    }

    summary._analysis[aspect->id()]= oa;
  }

  if (! summary._usable) {
    // TODO(fc) declining the offer results in getting another offer with
    // the same parameters over and over again. Why?

    // _scheduler->declineOffer(offer.id());

    return;
  }

  _offers.insert({ id, summary });
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

vector<OfferSummary> ArangoManagerImpl::currentOffers () {
  vector<OfferSummary> result;

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
  auto iter = _offers.find(id);

  if (iter == _offers.end()) {
    return;
  }

  const Offer& offer = iter->second._offer;
  string slaveId = offer.slave_id().value();

  _agency._blockedSlaves.erase(slaveId);
  _coordinator._blockedSlaves.erase(slaveId);
  _dbserver._blockedSlaves.erase(slaveId);

  _offers.erase(iter);

  LOG(INFO)
  << "DEBUG removed offer " << id
  << " for slave " << slaveId;
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

  // TODO(fc) need to check that we always have a cluster of agencies
  // which knows the master plan. If all instances fail, we need to restart at
  // least an instance which has access to the persistent volume!

  OfferSummary offer = findOffer(aspect);

  if (! offer._usable) {
    LOG(INFO)
    << aspect._name << " cannot find a suitable resource";
    return;
  }

  OfferAnalysis& analysis = offer._analysis[aspect.id()];
  analysis._ports = findFreePorts(offer._offer, aspect._requiredPorts);

  // try to start a new instance
  startInstance(aspect, offer._offer, analysis);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief makes a persistent volume
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::makePersistentVolume (const string& name,
                                              const Offer& offer,
                                              const Resources& resources) const {
  const string& offerId = offer.id().value();
  const string& slaveId = offer.slave_id().value();

  if (resources.empty()) {
    LOG(WARNING)
    << name << " cannot make persistent volume from empty resource "
    << "(offered resource was " << offer.resources() << ")";

    return;
  }

  Resource disk = *resources.begin();

  Resource::DiskInfo diskInfo;

  diskInfo.mutable_persistence()->set_id(name + ":" + slaveId);

  Volume volume;

  volume.set_container_path("/tmp/arangodb/" + name);
  volume.set_mode(Volume::RW);

  diskInfo.mutable_volume()->CopyFrom(volume);
  disk.mutable_disk()->CopyFrom(diskInfo);

  Resources persistent;
  persistent += disk;

  LOG(INFO)
  << name << " "
  << "trying to make " << offerId
  << " persistent for " << persistent;

  _scheduler->makePersistent(offer, persistent);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief makes a dynamic reservation
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::makeDynamicReservation (const string& name,
                                                const Offer& offer,
                                                const Resources& resources) const {
  const string& offerId = offer.id().value();
  Resources res = resources.flatten(_role, Resource::FRAMEWORK);

  LOG(INFO)
  << name << " "
  << "trying to reserve " << offerId
  << " with " << res;

  _scheduler->reserveDynamically(offer, res);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief finds a suitable offer for an agency
////////////////////////////////////////////////////////////////////////////////

OfferSummary ArangoManagerImpl::findOffer (Aspects& aspect) {
  size_t id = aspect.id();

  // find a usable offer
  auto iter = std::find_if(_offers.begin(), _offers.end(),
    [&] (pair<const string&, const OfferSummary&> offer) -> bool {
      return offer.second._analysis[id]._status == OfferAnalysisType::USABLE;
  });

  if (iter != _offers.end()) {
    OfferSummary result = iter->second;
    removeOffer(result._offer.id().value());
    return result;
  }

  // find a persistent offer
  iter = std::find_if(_offers.begin(), _offers.end(),
    [&] (pair<const string&, const OfferSummary&> offer) -> bool {
      return offer.second._analysis[id]._status == OfferAnalysisType::PERSISTENT_VOLUME_REQUIRED;
  });

  if (iter != _offers.end()) {
    OfferSummary result = iter->second;
    makePersistentVolume(aspect._name, result._offer, result._analysis[id]._resources);
    removeOffer(result._offer.id().value());
    return { false };
  }

  // find a dynamic reservation offer
  iter = std::find_if(_offers.begin(), _offers.end(),
    [&] (pair<const string&, const OfferSummary&> offer) -> bool {
      return offer.second._analysis[id]._status == OfferAnalysisType::DYNAMIC_RESERVATION_REQUIRED;
  });

  if (iter != _offers.end()) {
    OfferSummary result = iter->second;
    makeDynamicReservation(aspect._name, result._offer, result._analysis[id]._resources);
    removeOffer(result._offer.id().value());
    return { false };
  }

  return { false };
}

////////////////////////////////////////////////////////////////////////////////
/// @brief starts a new agency
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::startInstance (Aspects& aspect,
                                       const Offer& offer,
                                       const OfferAnalysis& analysis) {
  string const& id = offer.id().value();

  Resources resources = analysis._resources;
  resources += resourcesPorts(analysis._ports);

  LOG(INFO)
  << "AGENCY about to start agency using offer " << id 
  << " with resources " << resources
  << " on host " << offer.hostname();

  uint64_t taskId = _scheduler->startAgencyInstance(offer, resources);
  string slaveId = offer.slave_id().value();

  Instance desc;

  desc._taskId = taskId;
  desc._aspectId = aspect.id();
  desc._state = InstanceState::STARTED;
  desc._resources = resources;
  desc._slaveId = slaveId;
  desc._started = system_clock::now();
  desc._lastUpdate = system_clock::time_point();

  _instances.insert({ taskId, desc });
  aspect._startedSlaves.insert(slaveId);

  // TODO(fc) need to gc old slaves

  if (aspect._preferredSlaves.size() < aspect._plannedInstances) {
    aspect._preferredSlaves.insert(slaveId);
  }

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
    LOG(WARNING)
    << "INSTANCE is not STARTED, but got RUNNING (for "
    << taskId << "), ignoring";

    return;
  }

  Aspects* aspect = _aspects[instance._aspectId];

  LOG(INFO)
  << aspect->_name << " changing state from "
  << toString(instance._state)
  << " to RUNNING for " << taskId;

  instance._state = InstanceState::RUNNING;

  --(aspect->_startedInstances);
  ++(aspect->_runningInstances);
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

  Aspects* aspect = _aspects[instance._aspectId];

  LOG(INFO)
  << aspect->_name << " changing state from "
  << toString(instance._state)
  << " to FINISHED for " << taskId << "\n";

  if (state == InstanceState::STARTED && 0 < aspect->_startedInstances) {
    --(aspect->_startedInstances);
  }
  else if (state == InstanceState::RUNNING && 0 < aspect->_runningInstances) {
    --(aspect->_runningInstances);
  }

  instance._state = InstanceState::FINISHED;

  aspect->_startedSlaves.erase(instance._slaveId);
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

  delete _dispatcher;
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

vector<OfferSummary> ArangoManager::currentOffers () {
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
