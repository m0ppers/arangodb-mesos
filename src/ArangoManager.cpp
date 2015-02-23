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

namespace {
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

    public:
      size_t _plannedInstances;
      size_t _minimumInstances;

      size_t _startedInstances;
      size_t _runningInstances;
  };
}

// -----------------------------------------------------------------------------
// --SECTION--                                               class AgencyAspects
// -----------------------------------------------------------------------------

namespace {
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
        Option<Resources> found = resources.find(ndisk);

        if (found.isNone()) {
          LOG(INFO) 
          << "DEBUG " << resources << " does not have " 
          << ndisk << " requirements";

          return { OfferAnalysisType::NOT_USABLE };
        }

        Resources reserved = found.get();
        Resources unreserved = reserved.filter(Resources::isUnreserved);

        if (! unreserved.empty()) {
          reservationRequired = true;
        }

        // next check the disk part
        Resources mdisk = _minimumResources.filter(isDisk);
        size_t mds = diskspace(mdisk);
        Resources odisk = resources.filter(isDisk);

        // first check for already persistent resources
        for (auto res : odisk) {
          cout << "DEBUG checking " << res << " for resistent" << endl;

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

          cout << "could work" << endl;

          if (reservationRequired) {
            return { OfferAnalysisType::DYNAMIC_RESERVATION_REQUIRED, unreserved };
          }
          else {
            return { OfferAnalysisType::USABLE, reserved };
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

          return { OfferAnalysisType::DYNAMIC_RESERVATION_REQUIRED, unreserved + res };
        }

        cout << "could not work" << endl;

        return { OfferAnalysisType::NOT_USABLE };
      }
  };
}

// -----------------------------------------------------------------------------
// --SECTION--                                          class CoordinatorAspects
// -----------------------------------------------------------------------------

namespace {
  class CoordinatorAspects : public Aspects {
    public:
      CoordinatorAspects (const string& role) 
        : Aspects("COORDINATOR", role) {
        _minimumResources = Resources::parse("cpus:4;mem:1024;disk:1024").get();
        _additionalResources = Resources();
        _persistentVolumeRequired = false;
        _requiredPorts = 1;

        _minimumInstances = 1;
        _plannedInstances = 4;
      }

    public:
      size_t id () const override {
        return static_cast<size_t>(AspectsId::ID_COORDINATOR);
      }

      OfferAnalysis analyseOffer (const Offer& offer) override {
        return OfferAnalysis();
      }
  };
}

// -----------------------------------------------------------------------------
// --SECTION--                                            class DBServersAspects
// -----------------------------------------------------------------------------

namespace {
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
}

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
    OfferAnalysis findReservedOffer (Aspects&);
    OfferAnalysis findUnreservedOffer (Aspects&);

    void startInstance (Aspects&, OfferAnalysis&);

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
    vector<Aspects*> _aspects;

    unordered_map<string, OfferSummary> _offers;
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
    LOG(INFO)
    << "OFFER declined, because minimum resource not satisfied";

    _scheduler->declineOffer(offer.id());

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

#if 0
    for (const auto& instance : _instances) {
      result.push_back(instance.second);
    }
#endif
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
#if 0
  _agency._offers.erase(id);
  _coordinator._offers.erase(id);
  _dbserver._offers.erase(id);
#endif

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

  // TODO(fc) need to check that we always have a cluster of agencies
  // which knows the master plan. If all instances fail, we need to restart at
  // least an instance which has access to the persistent volume!

  OfferSummary offer = findOffer(aspect);

#if 0
  if (offer._type != OfferAnalysisType::USABLE) {
    LOG(INFO) << "AGENCY cannot find a suitable resource for missing agency\n";
    return;
  }

  // try to start a new instance
  startInstance(aspect, offer);
#endif
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
    _offers.erase(iter);
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
    _offers.erase(iter);
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
    _offers.erase(iter);
    return { false };
  }

  return { false };
}

////////////////////////////////////////////////////////////////////////////////
/// @brief finds a suitable reserved offer for an agency
////////////////////////////////////////////////////////////////////////////////

OfferAnalysis ArangoManagerImpl::findReservedOffer (Aspects& aspect) {
#if 0
  Resources resources = aspect.minimumResourcesRole();

  Resources possibleVolume;
  string possibleId;

  for (auto& id_offer : _reservedOffers) {
    Resources r = id_offer.second.resources();

    if (r.contains(resources)) {
      auto ports = findFreePorts(id_offer.second, aspect._requiredPorts);

      if (ports.size() != aspect._requiredPorts) {
        continue;
      }

      if (aspect._persistentVolumeRequired) {
        Resources possible;
        bool ok = aspect.hasPersistentVolume(r, possible);

        cout << "########### " << ok << ":" << possible << endl;

        if (! ok) {
          possibleVolume = possible;
          possibleId = id_offer.first;
          continue;
        }
      }

      return { OfferAnalysisType::USABLE, id_offer.second, resources, ports };
    }
  }

  if (! possibleId.empty()) {
    Offer offer = _reservedOffers[possibleId];

    return {
      OfferAnalysisType::REQUIRE_PERSISTENT_VOLUME,
      offer,
      possibleVolume
    };
  }

  return { OfferAnalysisType::NOT_FOUND };
#endif
}

////////////////////////////////////////////////////////////////////////////////
/// @brief finds a suitable unreserved offer for an agency
////////////////////////////////////////////////////////////////////////////////

OfferAnalysis ArangoManagerImpl::findUnreservedOffer (Aspects& aspect) {
#if 0
  Resources resources = aspect.minimumResources();

  for (auto& id_offer : _unreservedOffers) {
    Offer& offer = id_offer.second;
    Resources r = offer.resources();

    if (r.contains(resources) && numberPorts(offer) >= aspect._requiredPorts) {
      return {
        OfferAnalysisType::REQUIRE_DYNAMIC_RESERVATION,
        id_offer.second,
        r
      };
    }
  }

  return { OfferAnalysisType::NOT_FOUND };
#endif
}

////////////////////////////////////////////////////////////////////////////////
/// @brief starts a new agency
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::startInstance (Aspects& aspects, OfferAnalysis& offer) {
#if 0
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
#endif
}

////////////////////////////////////////////////////////////////////////////////
/// @brief status update (runing)
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::taskRunning (uint64_t taskId) {
#if 0
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

  Aspects* aspects = aspectsByType(instance._type);

  LOG(INFO)
  << "INSTANCE changing state from "
  << ArangoManager::stringInstanceState(instance._state)
  << " to RUNNING for " << taskId;

  instance._state = InstanceState::RUNNING;

  --(aspects->_startedInstances);
  ++(aspects->_runningInstances);
#endif
}

////////////////////////////////////////////////////////////////////////////////
/// @brief status update (finished)
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::taskFinished (uint64_t taskId) {
#if 0
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
#endif
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
