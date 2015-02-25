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

#include "ArangoScheduler.h"
#include "utils.h"

using namespace mesos;
using namespace arangodb;

using std::chrono::system_clock;

// -----------------------------------------------------------------------------
// --SECTION--                                                  helper functions
// -----------------------------------------------------------------------------

namespace {

///////////////////////////////////////////////////////////////////////////////
/// @brief not-a-port filter
///////////////////////////////////////////////////////////////////////////////

  bool notIsPorts (const Resource& resource) {
    return resource.name() != "ports";
  }

///////////////////////////////////////////////////////////////////////////////
/// @brief is-a-disk filter
///////////////////////////////////////////////////////////////////////////////

  bool isDisk (const Resource& resource) {
    return resource.name() == "disk";
  }

///////////////////////////////////////////////////////////////////////////////
/// @brief is-not-a-disk filter
///////////////////////////////////////////////////////////////////////////////

  bool notIsDisk (const Resource& resource) {
    return resource.name() != "disk";
  }

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts diskspace from a resource
///////////////////////////////////////////////////////////////////////////////

  double diskspace (const Resource& resource) {
    if (resource.name() == "disk" && resource.type() == Value::SCALAR) {
      return resource.scalar().value();
    }

    return 0;
  }

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts diskspace from resources
///////////////////////////////////////////////////////////////////////////////

  double diskspace (const Resources& resources) {
    double value = 0;

    for (auto resource : resources) {
      value += diskspace(resource);
    }

    return value;
  }

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts number of avaiable ports from an offer
///////////////////////////////////////////////////////////////////////////////

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

///////////////////////////////////////////////////////////////////////////////
/// @brief finds free ports from an offer
///////////////////////////////////////////////////////////////////////////////

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

///////////////////////////////////////////////////////////////////////////////
/// @brief generates resources from a list of ports
///////////////////////////////////////////////////////////////////////////////

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

///////////////////////////////////////////////////////////////////////////////
/// @brief analyses an offer
///////////////////////////////////////////////////////////////////////////////

  bool checkSlaveStates (const Aspects& aspect, const string& slaveId) {
    const string& name = aspect._name;
    auto& blockedSlaves = aspect._blockedSlaves;

    if (blockedSlaves.find(slaveId) != blockedSlaves.end()) {
      LOG(INFO)
      << name << " has blocked slave " << slaveId;

      return false;
    }

    auto& startedSlaves = aspect._startedSlaves;

    if (startedSlaves.find(slaveId) != startedSlaves.end()) {
      LOG(INFO)
      << name << " has already an instance started on slave " << slaveId;

      return false;
    }

    auto& preferredSlaves = aspect._preferredSlaves;

    if (preferredSlaves.size() >= aspect._plannedInstances) {
      if (preferredSlaves.find(slaveId) == preferredSlaves.end()) {
        LOG(INFO)
        << name << slaveId << " is not a preferred slave";

        return false;
      }
    }

    return true;
  }


  bool checkPorts (const Aspects& aspect, const Offer& offer) {
    if (numberPorts(offer) < aspect._requiredPorts) {
      LOG(INFO) 
      << "DEBUG " << offer.resources() << " does not have " 
      << aspect._requiredPorts << " ports";

      return false;
    }

    return true;
  }



  bool checkMemCpu (const Aspects& aspect,
                    const Offer& offer,
                    Resources& reserved,
                    Resources& unreserved) {
    const string& role = aspect._role;
    const Resources& minimumResources = aspect._minimumResources;

    Resources memcpu = minimumResources.filter(notIsDisk);
    Resources resources = offer.resources();
    Option<Resources> found = resources.find(memcpu.flatten(role, Resource::FRAMEWORK));

    if (found.isNone()) {
      LOG(INFO) 
      << "DEBUG " << resources << " does not have " 
      << memcpu << " requirements";

      return false;
    }

    reserved = found.get();
    unreserved = reserved.filter(Resources::isUnreserved);

    return true;
  }



  pair<bool, Resource> checkPersistentDisk (const Aspects& aspect,
                                            const Resources& minimumDisk,
                                            const Resources& offerDisk) {
    const string& role = aspect._role;
    const string& name = aspect._name;
    size_t mds = diskspace(minimumDisk);

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

      if (diskId.find(name + ":") != 0) {
        continue;
      }

      return { true, res };
    }

    return { false, Resource() };
  }



  pair<bool, Resource> checkReservedDisk (const Aspects& aspect,
                                          const Resources& minimumDisk,
                                          const Resources& offerDisk) {
    const string& role = aspect._role;
    size_t mds = diskspace(minimumDisk);

    for (const auto& res : offerDisk) {
      if (res.role() != role) {
        continue;
      }

      if (diskspace(res) < mds) {
        continue;
      }

      return { true, res };
    }

    return { false, Resource() };
  }


  pair<bool, Resource> checkUnreservedDisk (const Aspects& aspect,
                                            const Resources& minimumDisk,
                                            const Resources& offerDisk) {
    size_t mds = diskspace(minimumDisk);

    for (auto res : offerDisk) {
      if (res.role() != "*") {
        continue;
      }

      if (diskspace(res) < mds) {
        continue;
      }

      return { true, res };
    }

    return { false, Resource() };
  }



  OfferAnalysis analyseOffer (const Aspects& aspect, const Offer& offer) {
    Resources resources = offer.resources();
    string slaveId = offer.slave_id().value();

    // first check if this slave is already known
    if (! checkSlaveStates(aspect, slaveId)) {
      return { OfferAnalysisType::NOT_USABLE };
    }

    // first check the number of ports
    if (! checkPorts(aspect, offer)) {
      return { OfferAnalysisType::NOT_USABLE };
    }

    // next check non-disk parts
    Resources reserved;
    Resources unreserved;
      
    if (! checkMemCpu(aspect, offer, reserved, unreserved)) {
      return { OfferAnalysisType::NOT_USABLE };
    }

    bool reservationRequired = ! unreserved.empty();

    // next check the disk part
    Resources mdisk = aspect._minimumResources.filter(isDisk);
    Resources odisk = resources.filter(isDisk);

    // first check for already persistent resources
    auto diskres = checkPersistentDisk(aspect, mdisk, odisk);

    if (diskres.first) {
      if (reservationRequired) {
        return {
          OfferAnalysisType::DYNAMIC_RESERVATION_REQUIRED,
          unreserved };
      }
      else {
        string containerPath = diskres.second.disk().volume().container_path();
        string hostPath;

        if (diskres.second.disk().volume().has_host_path()) {
          hostPath = diskres.second.disk().volume().host_path();
        }

        return {
          OfferAnalysisType::USABLE,
          reserved + diskres.second,
          containerPath,
          hostPath };
      }
    }

    // next check for reserved resources
    diskres = checkReservedDisk(aspect, mdisk, odisk);

    if (diskres.first) {
      if (reservationRequired) {
        return {
          OfferAnalysisType::DYNAMIC_RESERVATION_REQUIRED,
          unreserved };
      }
      else {
        return { 
          OfferAnalysisType::PERSISTENT_VOLUME_REQUIRED,
          Resources() + diskres.second };
      }
    }

    // at last, try to find an unreserved resource
    diskres = checkUnreservedDisk(aspect, mdisk, odisk);

    if (diskres.first) {
      return {
        OfferAnalysisType::DYNAMIC_RESERVATION_REQUIRED,
        unreserved + mdisk };
    }

    return { OfferAnalysisType::NOT_USABLE };
  }

///////////////////////////////////////////////////////////////////////////////
/// @brief initializes an agency
///////////////////////////////////////////////////////////////////////////////

  void initializeAgency (const Instance& instance) {
    
    // extract the hostname
    const string& hostname = instance._hostname;

    // and the client port
    uint32_t port = instance._ports[1];

    string command
      = "./bin/initAgency.sh " + hostname + " " + to_string(port);

    int res = system(command.c_str());

    LOG(INFO)
    << "COMMAND " << command << " returned " << res;
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                             class InstanceManager
// -----------------------------------------------------------------------------

class InstanceManager {
  public:
    unordered_map<uint64_t, Instance> _instances;
};

// -----------------------------------------------------------------------------
// --SECTION--                                                     class Aspects
// -----------------------------------------------------------------------------

Aspects::Aspects (const string& name, const string& role)
  : _name(name), _role(role) {
  _startedInstances = 0;
  _runningInstances = 0;
}

// -----------------------------------------------------------------------------
// --SECTION--                                               class AgencyAspects
// -----------------------------------------------------------------------------

///////////////////////////////////////////////////////////////////////////////
/// @brief AgencyAspects
///////////////////////////////////////////////////////////////////////////////

class AgencyAspects : public Aspects {
  public:
    AgencyAspects (const string& role) 
      : Aspects("AGENCY", role) {
      _minimumResources = Resources::parse("cpus:0.5;mem:100;disk:100").get();
      _additionalResources = Resources();
      _persistentVolumeRequired = true;
      _requiredPorts = 2;

      _minimumInstances = 1;
      _plannedInstances = 1;
    }

  public:
    unordered_set<string> _masters;

  public:
    size_t id () const override {
      return static_cast<size_t>(AspectsId::ID_AGENCY);
    }

    bool isUsable () const override {
      return 0 < _runningInstances;
    }

    string arguments (const Offer& offer,
                      const OfferAnalysis& analysis) const override {
      uint32_t p1 = analysis._ports[0];
      uint32_t p2 = analysis._ports[1];

      vector<string> a;

      a.push_back("/usr/lib/arangodb/etcd-arango");

      a.push_back("--data-dir");
      a.push_back(analysis._containerPath + "/data");

      a.push_back("--listen-peer-urls");
      a.push_back("http://" + offer.hostname() + ":" + to_string(p1));

      a.push_back("--initial-advertise-peer-urls");
      a.push_back("http://" + offer.hostname() + ":" + to_string(p1));

      a.push_back("--initial-cluster");
      a.push_back("default=http://" + offer.hostname() + ":" + to_string(p1));

      a.push_back("--listen-client-urls");
      a.push_back("http://" + offer.hostname() + ":" + to_string(p2));

      a.push_back("--advertise-client-urls");
      a.push_back("http://" + offer.hostname() + ":" + to_string(p2));

      return join(a, "\n");
    }

    void instanceUp (const Instance& instance) override {
      const string& slaveId = instance._slaveId;

      if (_masters.find(slaveId) == _masters.end()) {
        initializeAgency(instance);
      }

      _masters.insert(slaveId);
    }
};

///////////////////////////////////////////////////////////////////////////////
/// @brief finds an agency endpoint
///////////////////////////////////////////////////////////////////////////////

namespace {

  // TODO(fc) do not use a global variable
  AgencyAspects* globalAgency = nullptr;
  InstanceManager* instanceManager = nullptr;

  string findAgencyEndpoint () {
    default_random_engine generator;
    uniform_int_distribution<int> d1(0, globalAgency->_masters.size() - 1);
    size_t d2 = d1(generator);

    auto iter = globalAgency->_masters.begin();
    advance(iter, d2);

    const string& slaveId = *iter;
    uint64_t taskId = globalAgency->_slave2task[slaveId];

    const Instance& instance = instanceManager->_instances[taskId];

    return "tcp://" + instance._hostname + ":" + to_string(instance._ports[1]);
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                               class ArangoAspects
// -----------------------------------------------------------------------------

class ArangoAspects : public Aspects {
  public:
    ArangoAspects (const string& name, const string& type, const string& role)
      : Aspects(name, role),
        _type(type) {
    }

  public:
    string arguments (const Offer& offer,
                      const OfferAnalysis& analysis) const override {
      uint32_t p1 = analysis._ports[0];

      vector<string> a;

      a.push_back("/usr/sbin/arangod");

      a.push_back("--database.directory");
      a.push_back(analysis._containerPath + "/data");

      a.push_back("--log.file");
      a.push_back(analysis._containerPath + "/logs/" + _type + ".log");

      a.push_back("--javascript.app-path");
      a.push_back(analysis._containerPath + "/apps");

      string serverEndpoint = "tcp://" + offer.hostname() + ":" + to_string(p1);

      a.push_back("--server.endpoint");
      a.push_back(serverEndpoint);

      string agency = findAgencyEndpoint();

      a.push_back("--cluster.agency-endpoint");
      a.push_back(agency);

      a.push_back("--cluster.my-address");
      a.push_back(serverEndpoint);

      string slaveId = offer.slave_id().value();

      // create a hash from the container_path and the slaveId
      uint64_t hash = FnvHashString({ analysis._containerPath, slaveId });

      a.push_back("--cluster.my-local-info");
      a.push_back(_type + ":" + to_string(hash));

      return join(a, "\n");
    }

  public:
    const string _type;
};

// -----------------------------------------------------------------------------
// --SECTION--                                          class CoordinatorAspects
// -----------------------------------------------------------------------------

class CoordinatorAspects : public ArangoAspects {
  public:
    CoordinatorAspects (const string& role) 
      : ArangoAspects("COORDINATOR", "coordinator", role) {
      _minimumResources = Resources::parse("cpus:1;mem:1024;disk:1024").get();
      _additionalResources = Resources();
      _persistentVolumeRequired = true;
      _requiredPorts = 1;

      _minimumInstances = 1;
      _plannedInstances = 3;
    }

  public:
    size_t id () const override {
      return static_cast<size_t>(AspectsId::ID_COORDINATOR);
    }

    bool isUsable () const override {
      return 0 < _runningInstances;
    }

    void instanceUp (const Instance& instance) override {
    }
};

// -----------------------------------------------------------------------------
// --SECTION--                                            class DBServersAspects
// -----------------------------------------------------------------------------

class DBServerAspects : public ArangoAspects {
  public:
    DBServerAspects (const string& role) 
      : ArangoAspects("DBSERVER", "dbserver", role) {
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

    bool isUsable () const override {
      return 0 < _runningInstances;
    }

    void instanceUp (const Instance& instance) override {
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

class arangodb::ArangoManagerImpl : public InstanceManager {
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

  _aspects = { &_agency, &_coordinator, &_dbserver };

  globalAgency = &_agency;
  instanceManager = this;
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

      if (_agency.isUsable()) {
        checkInstances(_coordinator);
        checkInstances(_dbserver);
      }
    }

    this_thread::sleep_for(chrono::seconds(SLEEP_SEC));
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief adds an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::addOffer (const Offer& offer) {
  lock_guard<mutex> lock(_lock);

  const string& id = offer.id().value();
  const string& slaveId = offer.slave_id().value();

  LOG(INFO)
  << "OFFER received: " << id << ": " << offer.resources() << "\n";

  // check if offer is suitable for "something"
  OfferSummary summary = { false, offer };

  for (auto& aspect : _aspects) {
    OfferAnalysis oa = analyseOffer(*aspect, offer);

    if (oa._status != OfferAnalysisType::NOT_USABLE) {
      summary._usable = true;
      aspect->_blockedSlaves.insert(slaveId);
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
  const auto& iter = _offers.find(id);

  if (iter == _offers.end()) {
    return;
  }

  const Offer& offer = iter->second._offer;
  const string& slaveId = offer.slave_id().value();

  _agency._blockedSlaves.erase(slaveId);
  _coordinator._blockedSlaves.erase(slaveId);
  _dbserver._blockedSlaves.erase(slaveId);

  LOG(INFO)
  << "DEBUG removed offer " << id
  << " for slave " << slaveId;

  // must be last, because it will kill offer and slaveId
  _offers.erase(iter);
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
  string const& slaveId = offer.slave_id().value();

  Resources resources = analysis._resources;
  resources += resourcesPorts(analysis._ports);

  string arguments = aspect.arguments(offer, analysis);

  uint64_t taskId = _scheduler->startInstance(aspect._name, offer, resources, arguments);

  Instance desc;

  desc._taskId = taskId;
  desc._aspectId = aspect.id();
  desc._state = InstanceState::STARTED;
  desc._resources = resources;
  desc._slaveId = slaveId;
  desc._hostname = offer.hostname();
  desc._ports = analysis._ports;
  desc._started = system_clock::now();
  desc._lastUpdate = system_clock::time_point();

  _instances.insert({ taskId, desc });
  aspect._startedSlaves.insert(slaveId);
  aspect._slave2task.insert({ slaveId, taskId });

  // TODO(fc) need to gc old slaves

  if (aspect._preferredSlaves.size() < aspect._plannedInstances) {
    aspect._preferredSlaves.insert(slaveId);
  }

  ++(aspect._startedInstances);
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

  aspect->instanceUp(instance);
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
