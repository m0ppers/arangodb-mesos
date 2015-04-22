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

#include <stout/uuid.hpp>

#include <atomic>
#include <iostream>
#include <set>

#include "ArangoScheduler.h"
#include "Caretaker.h"
#include "Global.h"
#include "utils.h"

#include "pbjson.hpp"

using namespace mesos;
using namespace arangodb;

using std::chrono::system_clock;

// -----------------------------------------------------------------------------
// --SECTION--                                                  helper functions
// -----------------------------------------------------------------------------

namespace {

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
/// @brief initializes an agency
///////////////////////////////////////////////////////////////////////////////

  /*
  bool initializeAgency (const Instance& instance) {
    static const int SLEEP_SEC = 5;
    
    // extract the hostname
    const string& hostname = instance._hostname;

    // and the client port
    uint32_t port = instance._ports[1];

    string command
      = "sleep " + to_string(SLEEP_SEC) 
      + " && ./bin/initAgency.sh " + hostname + " " + to_string(port);

    LOG(INFO)
    << "AGENCY about to initialize using: " << command;

    int res = system(command.c_str());

    LOG(INFO)
    << "AGENCY " << command << " returned " << res;

    return res == 0;
  }
  */

///////////////////////////////////////////////////////////////////////////////
/// @brief discovers new coordinators and dbservers
///////////////////////////////////////////////////////////////////////////////

  string findAgencyAddress ();

  void discoverRoles () {
    string command
      = "./bin/discover.sh " + findAgencyAddress();

    LOG(INFO)
    << "DISCOVERY about to start: " << command;

    int res = system(command.c_str());

    LOG(INFO)
    << "DISCOVERY " << command << " returned " << res;
  }

///////////////////////////////////////////////////////////////////////////////
/// @brief bootstraps a dbserver
///////////////////////////////////////////////////////////////////////////////

  /*
  void bootstrapDbserver (const Instance& instance) {
    
    // extract the hostname
    const string& hostname = instance._hostname;

    // and the client port
    uint32_t port = instance._ports[0];

    // construct the address
    string address = "http://" + hostname + ":" + to_string(port);

    string command
      = "curl -s -X POST " + address + "/_admin/cluster/bootstrapDbServer -d '{\"isRelaunch\":false}'";

    LOG(INFO)
    << "DBSERVER bootstraping using: " << command;

    int res = system(command.c_str());

    LOG(INFO)
    << "DBSERVER " << command << " returned " << res;
  }
  */

///////////////////////////////////////////////////////////////////////////////
/// @brief bootstraps a coordinator
///////////////////////////////////////////////////////////////////////////////

  /*
  void bootstrapCoordinator (const Instance& instance) {
    
    // extract the hostname
    const string& hostname = instance._hostname;

    // and the client port
    uint32_t port = instance._ports[0];

    // construct the address
    string address = "http://" + hostname + ":" + to_string(port);

    string command
      = "curl -s -X POST " + address + "/_admin/cluster/bootstrapCoordinator -d '{\"isRelaunch\":false}'";

    LOG(INFO)
    << "COORDINATOR bootstraping using: " << command;

    int res = system(command.c_str());

    LOG(INFO)
    << "COORDINATOR " << command << " returned " << res;
  }
  */

///////////////////////////////////////////////////////////////////////////////
/// @brief upgrades the cluster database
///////////////////////////////////////////////////////////////////////////////

  /*
  void upgradeDatabase (const Instance& instance) {

    // extract the hostname
    const string& hostname = instance._hostname;

    // and the client port
    uint32_t port = instance._ports[0];

    // construct the address
    string address = "http://" + hostname + ":" + to_string(port);

    string command
      = "curl -s -X POST " + address + "/_admin/cluster/upgradeClusterDatabase -d '{\"isRelaunch\":false}'";

    LOG(INFO)
    << "COORDINATOR upgrading database using: " << command;

    int res = system(command.c_str());

    LOG(INFO)
    << "COORDINATOR " << command << " returned " << res;
  }
  */
}

// -----------------------------------------------------------------------------
// --SECTION--                                                     class Aspects
// -----------------------------------------------------------------------------

Aspects::Aspects (const string& name)
  : _name(name) {
}

// -----------------------------------------------------------------------------
// --SECTION--                                               class AgencyAspects
// -----------------------------------------------------------------------------

///////////////////////////////////////////////////////////////////////////////
/// @brief AgencyAspects
///////////////////////////////////////////////////////////////////////////////

class AgencyAspects : public Aspects {
  public:
    AgencyAspects ()
      : Aspects("AGENCY") {
    }

    string arguments (const ResourcesCurrentEntry& info,
                      const string& taskId) const override {
      uint32_t p1 = info.ports(0);
      uint32_t p2 = info.ports(1);
      string containerPath = info.container_path();
      string hostname = info.hostname();

      vector<string> a;

      a.push_back("/usr/lib/arangodb/etcd-arango");

      a.push_back("--data-dir");
      a.push_back(containerPath + "/data");

      a.push_back("--listen-peer-urls");
      a.push_back("http://" + hostname + ":" + to_string(p1));

      a.push_back("--initial-advertise-peer-urls");
      a.push_back("http://" + hostname + ":" + to_string(p1));

      a.push_back("--initial-cluster");
      a.push_back("default=http://" + hostname + ":" + to_string(p1));

      a.push_back("--listen-client-urls");
      a.push_back("http://" + hostname + ":" + to_string(p2));

      a.push_back("--advertise-client-urls");
      a.push_back("http://" + hostname + ":" + to_string(p2));

      return join(a, "\n");
    }

    /*
    bool instanceUp (const Instance& instance) override {
      const string& slaveId = instance._slaveId;

      if (_masters.find(slaveId) != _masters.end()) {
        return true;
      }

      bool ok = initializeAgency(instance);

      if (! ok) {
        return false;
      }

      _masters.insert(slaveId);
      return true;
    }
    */
};

///////////////////////////////////////////////////////////////////////////////
/// @brief finds an agency endpoint
///////////////////////////////////////////////////////////////////////////////

namespace {

  // TODO(fc) do not use a global variable
  AgencyAspects* globalAgency = nullptr;

  string findAgencyEndpoint () {
    /*
    default_random_engine generator;
    uniform_int_distribution<int> d1(0, globalAgency->_masters.size() - 1);
    size_t d2 = d1(generator);

    auto iter = globalAgency->_masters.begin();
    advance(iter, d2);

    const string& slaveId = *iter;
    const string& taskId = globalAgency->_slave2task[slaveId];

    const Instance& instance = globalAgency->_instanceManager->_instances[taskId];

    return "tcp://" + instance._hostname + ":" + to_string(instance._ports[1]);
    */
  }

  string findAgencyAddress () {
    /*
    default_random_engine generator;
    uniform_int_distribution<int> d1(0, globalAgency->_masters.size() - 1);
    size_t d2 = d1(generator);

    auto iter = globalAgency->_masters.begin();
    advance(iter, d2);

    const string& slaveId = *iter;
    const string& taskId = globalAgency->_slave2task[slaveId];

    const Instance& instance = globalAgency->_instanceManager->_instances[taskId];

    return instance._hostname + ":" + to_string(instance._ports[1]);
    */
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                               class ArangoAspects
// -----------------------------------------------------------------------------

class ArangoAspects : public Aspects {
  public:
    ArangoAspects (const string& name)
      : Aspects(name) {
    }

  public:
    string arguments (const ResourcesCurrentEntry& info,
                      const string& taskId) const override {
/*
      uint32_t p1 = analysis._ports[0];

      vector<string> a;

      a.push_back("/usr/sbin/arangod");

      a.push_back("--database.directory");
      a.push_back(analysis._containerPath + "/data");

      a.push_back("--log.file");
      a.push_back(analysis._containerPath + "/logs/" + _type + ".log");
      // a.push_back("-");

      a.push_back("--log.level");
      // a.push_back("trace");
      // a.push_back("debug");
      a.push_back("info");

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
      a.push_back("--cluster.my-local-info");
      a.push_back(_type + ":" + taskId);

      return join(a, "\n");
*/
    }

  public:
    const string _type;
};

// -----------------------------------------------------------------------------
// --SECTION--                                          class CoordinatorAspects
// -----------------------------------------------------------------------------

class CoordinatorAspects : public ArangoAspects {
  public:
    CoordinatorAspects ()
      : ArangoAspects("coordinator") {
    }
};

// -----------------------------------------------------------------------------
// --SECTION--                                            class DBServersAspects
// -----------------------------------------------------------------------------

class DBServerAspects : public Aspects {
  public:
    DBServerAspects ()
      : Aspects("dbserver") {
    }

  public:
    string arguments (const ResourcesCurrentEntry& info,
                      const string& taskId) const override {
      vector<string> a;

      a.push_back("/bin/sleep");
      a.push_back("300");

      return join(a, "\n");
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
    AspectInstanceStatus _state;
    OfferID _offerId;
    chrono::system_clock::time_point _started;
};

// -----------------------------------------------------------------------------
// --SECTION--                                           class ArangoManagerImpl
// -----------------------------------------------------------------------------

class arangodb::ArangoManagerImpl {
  public:
    ArangoManagerImpl (const string& role, const string& principal);

  public:
    void dispatch ();
    void addOffer (const Offer& offer);
    void removeOffer (const OfferID& offerId);
    void statusUpdate (const string&);
    void slaveInfoUpdate (const mesos::SlaveInfo& info);
    // vector<OfferSummary> currentOffers ();
    // vector<Instance> currentInstances ();
    // ClusterInfo clusterInfo (const string& name) const;
    // ClusterInfo adjustPlanned (const string& name, const ClusterInfo&);
    // vector<arangodb::SlaveInfo> slaveInfo (const string& name) const;

  private:
    void removeOffer (const string& offerId);

    void checkInstances (Aspects&);
    bool makePersistentVolume (const string& name, const Offer&, const Resources&);
    bool makeDynamicReservation(const Offer&, const Resources&);

    void startInstance (Aspects&, const ResourcesCurrentEntry&);

    void taskRunning (const string&);
    void taskFinished (const string&);

  public:
    const string _role;
    const string _principal;
    mutex _lock;
    atomic<bool> _stopDispatcher;

    AgencyAspects _agency;
    CoordinatorAspects _coordinator;
    DBServerAspects _dbserver;

  private:
    vector<Aspects*> _aspects;

    // unordered_map<string, mesos::SlaveInfo> _slaveInfo;
    // unordered_map<string, OfferSummary> _offers;

    unordered_map<string, mesos::Offer> _storedOffers;
};

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

ArangoManagerImpl::ArangoManagerImpl (const string& role,
                                      const string& principal)
  : _role(role),
    _principal(principal),
    _stopDispatcher(false),
    _agency(),
    _coordinator(),
    _dbserver() {

  // TODO(fc) how to persist & change these values

  _aspects = { &_agency, &_coordinator, &_dbserver };

  globalAgency = &_agency;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief main dispatcher
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::dispatch () {
  static const int SLEEP_SEC = 10;

  bool init = false;
  unordered_set<string> bootstrapped;

  while (! _stopDispatcher) {
    LOG(INFO) << "DISPATCHER checking state\n";

    // .............................................................................
    // check all outstanding offers
    // .............................................................................

    unordered_map<string, mesos::Offer> next;
    vector<pair<mesos::Offer, mesos::Resources>> dynamic;
    vector<pair<mesos::Offer, OfferAction>> persistent;

    {
      lock_guard<mutex> lock(_lock);

      Caretaker& caretaker = Global::caretaker();
      caretaker.updatePlan();

      for (auto&& id_offer : _storedOffers) {
        OfferAction action = caretaker.checkOffer(id_offer.second);

        switch (action._state) {
          case OfferActionState::IGNORE:
            Global::scheduler().declineOffer(id_offer.second.id());
            break;

          case OfferActionState::USABLE:
            break;

          case OfferActionState::STORE_FOR_LATER:
            next[id_offer.first] = id_offer.second;
            break;

          case OfferActionState::MAKE_DYNAMIC_RESERVATION:
            dynamic.push_back(make_pair(id_offer.second, action._resources));
            break;

          case OfferActionState::MAKE_PERSISTENT_VOLUME:
            persistent.push_back(make_pair(id_offer.second, action));
            break;
        }
      }

      _storedOffers.swap(next);
    }

    // .............................................................................
    // try to make the dynamic reservations and persistent volumes
    // .............................................................................

    bool sleep = true;

    for (auto&& offer_res : dynamic) {
      bool res = makeDynamicReservation(offer_res.first, offer_res.second);

      if (res) {
        sleep = false;
      }
    }

    for (auto&& offer_res : persistent) {
      bool res = makePersistentVolume(offer_res.second._name,
                                      offer_res.first,
                                      offer_res.second._resources);

      if (res) {
        sleep = false;
      }
    }

    // .............................................................................
    // check if we can start new instances
    // .............................................................................

    vector<InstanceAction> start;

    {
      lock_guard<mutex> lock(_lock);

      Caretaker& caretaker = Global::caretaker();
      InstanceAction action;


      do {
        action = caretaker.checkInstance();

        switch (action._state) {
          case::InstanceActionState::DONE:
            break;

          case::InstanceActionState::START_AGENCY:
          case::InstanceActionState::START_COORDINATOR:
          case::InstanceActionState::START_PRIMARY_DBSERVER:
            start.push_back(action);
            break;
        }
      }
      while (action._state != InstanceActionState::DONE);
    }

    for (auto&& action : start) {
      switch (action._state) {
        case::InstanceActionState::START_AGENCY:
          startInstance(_agency, action._info);
          break;

        case::InstanceActionState::START_COORDINATOR:
          startInstance(_coordinator, action._info);
          break;

        case::InstanceActionState::START_PRIMARY_DBSERVER:
          startInstance(_dbserver, action._info);
          break;

        default:
          break;
      }
    }
    
    // .............................................................................
    // wait for a little while
    // .............................................................................

    if (sleep) {
      this_thread::sleep_for(chrono::seconds(SLEEP_SEC));
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief adds an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::addOffer (const Offer& offer) {
  lock_guard<mutex> lock(_lock);

  {
    string json;
    pbjson::pb2json(&offer, json);
    LOG(INFO)
    << "OFFER received: " << json;
  }

  _storedOffers[offer.id().value()] = offer;
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

/*
void ArangoManagerImpl::statusUpdate (const string& taskId,
                                      InstanceState state) {
  lock_guard<mutex> lock(_lock);

  if (state == InstanceState::RUNNING) {
    taskRunning(taskId);
  }
  else if (state == InstanceState::FINISHED) {
    taskFinished(taskId);
  }
}

*/

////////////////////////////////////////////////////////////////////////////////
/// @brief slave update
////////////////////////////////////////////////////////////////////////////////

/*
void ArangoManagerImpl::slaveInfoUpdate (const mesos::SlaveInfo& info) {
  LOG(INFO)
  << "DEBUG received slave info for " << info.id().value();

  _slaveInfo[info.id().value()] = info;
}
*/

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the current offers for debugging
////////////////////////////////////////////////////////////////////////////////

/*
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
*/

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the current instances for debugging
////////////////////////////////////////////////////////////////////////////////

/*
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
*/

////////////////////////////////////////////////////////////////////////////////
/// @brief clusterInfo
////////////////////////////////////////////////////////////////////////////////

/*
ClusterInfo ArangoManagerImpl::clusterInfo (const string& name) const {
  ClusterInfo info;

  info._name = name;

  info._planned._agencies = _agency._plannedInstances;
  info._planned._coordinators = _coordinator._plannedInstances;
  info._planned._dbservers = _dbserver._plannedInstances;

  info._running._agencies = _agency._runningInstances;
  info._running._coordinators = _coordinator._runningInstances;
  info._running._dbservers = _dbserver._runningInstances;

  for (auto aspect : _aspects) {
    info._planned._servers += aspect->_plannedInstances;
    info._running._servers += aspect->_runningInstances;

    double c = cpus(aspect->_minimumResources);
    info._planned._cpus += aspect->_plannedInstances * c;
    info._running._cpus += aspect->_runningInstances * c;

    double m = memory(aspect->_minimumResources);
    info._planned._memory += aspect->_plannedInstances * m * 1024 * 1024;
    info._running._memory += aspect->_runningInstances * m * 1024 * 1024;

    double d = diskspace(aspect->_minimumResources);
    info._planned._disk += aspect->_plannedInstances * d * 1024 * 1024;
    info._running._disk += aspect->_runningInstances * d * 1024 * 1024;
  }

  info._planned._servers = round(info._planned._servers * 1000.0) / 1000.0;
  info._planned._cpus = round(info._planned._cpus * 1000.0) / 1000.0;
  info._planned._memory = round(info._planned._memory * 1000.0) / 1000.0;
  info._planned._disk = round(info._planned._disk * 1000.0) / 1000.0;

  info._running._servers = round(info._running._servers * 1000.0) / 1000.0;
  info._running._cpus = round(info._running._cpus * 1000.0) / 1000.0;
  info._running._memory = round(info._running._memory * 1000.0) / 1000.0;
  info._running._disk = round(info._running._disk * 1000.0) / 1000.0;

  return info;
}
*/

////////////////////////////////////////////////////////////////////////////////
/// @brief adjustPlanned
////////////////////////////////////////////////////////////////////////////////

/*
ClusterInfo ArangoManagerImpl::adjustPlanned (const string& name,
                                              const ClusterInfo& info) {
  _agency._plannedInstances = info._planned._agencies;
  _coordinator._plannedInstances = info._planned._coordinators;
  _dbserver._plannedInstances = info._planned._dbservers;

  return clusterInfo(info._name);

  // TODO(fc) kill old instances
}
*/

////////////////////////////////////////////////////////////////////////////////
/// @brief slaveInfo
////////////////////////////////////////////////////////////////////////////////

/*
vector<arangodb::SlaveInfo> ArangoManagerImpl::slaveInfo (const string& name) const {
  map<string, SlaveInfo> infos;

  for (auto& i : _instances) {
    auto& instance = i.second;

    if (instance._state != InstanceState::STARTED && instance._state != InstanceState::RUNNING) {
      continue;
    }

    auto& info = infos[instance._slaveId];

    info._used._cpus += cpus(instance._resources);
    info._used._memory += memory(instance._resources) * 1024 * 1024;
    info._used._disk += diskspace(instance._resources) * 1024 * 1024;
  }

  for (auto& i : infos) {
    auto& info = i.second;

    info._name = i.first;

    info._available._cpus = info._used._cpus * 2;
    info._available._memory = info._used._memory * 2;
    info._available._disk = info._used._disk * 2;
  }

  for (auto& i : _slaveInfo) {
    auto& slave = i.second;
    auto& info = infos[i.first];
    Resources resources = slave.resources();

    info._available._cpus = cpus(resources);
    info._available._memory = memory(resources) * 1024 * 1024;
    info._available._disk = diskspace(resources) * 1024 * 1024;
  }

  vector<SlaveInfo> result;

  for (auto& i : infos) {
    result.push_back(i.second);
  }

  return result;
}
*/

// -----------------------------------------------------------------------------
// --SECTION--                                                 private functions
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief removes an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::removeOffer (const string& id) {
/*
  const auto& iter = _offers.find(id);

  if (iter == _offers.end()) {
    return;
  }

  const Offer& offer = iter->second._offer;
  const string& slaveId = offer.slave_id().value();

  LOG(INFO)
  << "DEBUG removed offer " << id
  << " for slave " << slaveId;

  // must be last, because it will kill offer and slaveId
  _offers.erase(iter);
*/
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if need to start a new agency instance
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::checkInstances (Aspects& aspect) {
}

////////////////////////////////////////////////////////////////////////////////
/// @brief makes a persistent volume
////////////////////////////////////////////////////////////////////////////////

bool ArangoManagerImpl::makePersistentVolume (const string& name,
                                              const Offer& offer,
                                              const Resources& resources) {
  const string& offerId = offer.id().value();
  const string& slaveId = offer.slave_id().value();

  if (resources.empty()) {
    LOG(WARNING)
    << "cannot make persistent volume from empty resource "
    << "(offered resource was " << offer.resources() << ")";

    return false;
  }

  Resource disk = *resources.begin();

  Resource::DiskInfo diskInfo;

  diskInfo.mutable_persistence()->set_id(name + "_" + slaveId);

  Volume volume;

  volume.set_container_path(name);
  volume.set_mode(Volume::RW);

  diskInfo.mutable_volume()->CopyFrom(volume);
  disk.mutable_disk()->CopyFrom(diskInfo);

  Resources persistent;
  persistent += disk;

  LOG(INFO)
  << "DEBUG makePersistentVolume(" << name << "): "
  << "trying to make " << offerId
  << " persistent for " << persistent;

  mesos::Resources offered = offer.resources();

  if (offered.contains(persistent)) {
    addOffer(offer);
    return true;
  }
  else {
    Global::scheduler().makePersistent(offer, persistent);
    return false;
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief makes a dynamic reservation
////////////////////////////////////////////////////////////////////////////////

bool ArangoManagerImpl::makeDynamicReservation (const Offer& offer,
                                                const Resources& resources) {
  const string& offerId = offer.id().value();

#if MESOS_RESERVE_PORTS
  Resources res = resources;
#else
  Resources res = filterNotIsPorts(resources);
#endif

#if MESOS_PRINCIPAL
  res = res.flatten(Global::role(), Global::principal());
#else
  res = res.flatten(Global::role());
#endif

  LOG(INFO)
  << "DEBUG makeDynamicReservation: "
  << "trying to reserve " << offerId
  << " with " << res;

  Resources offered = offer.resources();
  Resources diff = res - offered;

  if (diff.empty()) {
    addOffer(offer);
    return true;
  }
  else {
    Global::scheduler().reserveDynamically(offer, diff);
    return false;
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief starts a new agency
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::startInstance (Aspects& aspect,
                                       const ResourcesCurrentEntry& info) {
  string taskId = UUID::random().toString();
  string arguments = aspect.arguments(info, taskId);

  if (info.ports_size() != 1) {
    LOG(WARNING)
    << "expected one port, got " << info.ports_size();
    return;
  }

  mesos::ContainerInfo::DockerInfo docker;
  docker.set_image("arangodb/arangodb");
  docker.set_network(mesos::ContainerInfo::DockerInfo::BRIDGE);

  mesos::ContainerInfo::DockerInfo::PortMapping* mapping = docker.add_port_mappings();
  mapping->set_host_port(info.ports(0));
  mapping->set_container_port(8529);
  mapping->set_protocol("tcp");

  cout << "##################### " << info.ports(0) << " to 8529\n";

  Global::scheduler().startInstance(
    taskId,
    "arangodb:" + aspect._name + ":" + taskId,
    info.slave_id(),
    info.offer_id(),
    info.resources(),
    docker);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief status update (runing)
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::taskRunning (const string& taskId) {
/*
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

  bool ok = aspect->instanceUp(instance);

  if (! ok) {
    _scheduler->killInstance(aspect->_name, instance._taskId);

    // TODO(fc) keep a list of killed task and try again, if now 
    // update is received within a certain time.
  }
*/
}

////////////////////////////////////////////////////////////////////////////////
/// @brief status update (finished)
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::taskFinished (const string& taskId) {
/*
  const auto& iter = _instances.find(taskId);

  if (iter == _instances.end()) {
    return;
  }

  Instance& instance = iter->second;
  InstanceState state = instance._state;

  if (state != InstanceState::STARTED && state != InstanceState::RUNNING) {
    return;
  }

  size_t aspectId = instance._aspectId;
  Aspects* aspect = _aspects[aspectId];

  LOG(INFO)
  << aspect->_name << " changing state from "
  << toString(instance._state)
  << " to FINISHED for " << taskId << "\n";

  // update statistics
  if (state == InstanceState::STARTED && 0 < aspect->_startedInstances) {
    --(aspect->_startedInstances);
  }
  else if (state == InstanceState::RUNNING && 0 < aspect->_runningInstances) {
    --(aspect->_runningInstances);
  }

  instance._state = InstanceState::FINISHED;

  // change waiting offers
  const string& slaveId = instance._slaveId;

  for (auto& offer : _offers) {
    auto& analysis = offer.second._analysis[aspectId];

    if (analysis._state == OfferAnalysisStatus::WAIT) {
      analysis._state = analysis._initialState;
    }
  }

  // remove instances completely
  aspect->_startedSlaves.erase(slaveId);
*/
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

ArangoManager::ArangoManager (const string& role, const string& principal)
  : _impl(nullptr),
    _dispatcher(nullptr) {
  _impl = new ArangoManagerImpl(role, principal);
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
/// @brief updates status
////////////////////////////////////////////////////////////////////////////////

/*
void ArangoManager::statusUpdate (const string& taskId, InstanceState state) {
  _impl->statusUpdate(taskId, state);
}
*/

////////////////////////////////////////////////////////////////////////////////
/// @brief updates slave
////////////////////////////////////////////////////////////////////////////////

/*
void ArangoManager::slaveInfoUpdate (const mesos::SlaveInfo& info) {
  _impl->slaveInfoUpdate(info);
}
*/

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the configured clusters
////////////////////////////////////////////////////////////////////////////////

/*
vector<ClusterInfo> ArangoManager::clusters () const {
  return { _impl->clusterInfo("arangodb") };
}
*/

////////////////////////////////////////////////////////////////////////////////
/// @brief returns information for one cluster
////////////////////////////////////////////////////////////////////////////////

/*
ClusterInfo ArangoManager::cluster (const string& name) const {
  return _impl->clusterInfo(name);
}
*/

////////////////////////////////////////////////////////////////////////////////
/// @brief adjusts the total number of servers
////////////////////////////////////////////////////////////////////////////////

namespace {
  // TODO(fc) needs sensible minimum

  void adjustSize (size_t& current, int value) {
    if (0 <= value) {
      current += value;
      return;
    }

    if (current <= (size_t) -value) {
      current = 1;
      return;
    }

    current += value;
  }
}

/*
ClusterInfo ArangoManager::adjustServers (const string& name, int value) {
  ClusterInfo info = _impl->clusterInfo(name);
  adjustSize(info._planned._agencies, value);
  adjustSize(info._planned._coordinators, value);
  adjustSize(info._planned._dbservers, value);

  return _impl->adjustPlanned(name, info);
}
*/

////////////////////////////////////////////////////////////////////////////////
/// @brief adjusts the total number of agencies
////////////////////////////////////////////////////////////////////////////////

/*
ClusterInfo ArangoManager::adjustAgencies (const string& name, int value) {
  ClusterInfo info = _impl->clusterInfo(name);
  adjustSize(info._planned._agencies, value);

  return _impl->adjustPlanned(name, info);
}
*/

////////////////////////////////////////////////////////////////////////////////
/// @brief adjusts the total number of coordinators
////////////////////////////////////////////////////////////////////////////////

/*
ClusterInfo ArangoManager::adjustCoordinators (const string& name, int value) {
  ClusterInfo info = _impl->clusterInfo(name);
  adjustSize(info._planned._coordinators, value);

  return _impl->adjustPlanned(name, info);
}
*/

////////////////////////////////////////////////////////////////////////////////
/// @brief adjusts the total number of dbservers
////////////////////////////////////////////////////////////////////////////////

/*
ClusterInfo ArangoManager::adjustDbservers (const string& name, int value) {
  ClusterInfo info = _impl->clusterInfo(name);
  adjustSize(info._planned._dbservers, value);

  return _impl->adjustPlanned(name, info);
}
*/

////////////////////////////////////////////////////////////////////////////////
/// @brief returns information about the slaves
////////////////////////////////////////////////////////////////////////////////

/*
vector<arangodb::SlaveInfo> ArangoManager::slaveInfo (const string& name) {
  return _impl->slaveInfo(name);
}
*/

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the current offers for debugging
////////////////////////////////////////////////////////////////////////////////

/*
vector<OfferSummary> ArangoManager::currentOffers () {
  return _impl->currentOffers();
}
*/

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the current instances for debugging
////////////////////////////////////////////////////////////////////////////////

/*
vector<Instance> ArangoManager::currentInstances () {
  return _impl->currentInstances();
}
*/

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
