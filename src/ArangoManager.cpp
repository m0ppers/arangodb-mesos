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

#include "ArangoScheduler.h"
#include "ArangoState.h"
#include "Caretaker.h"
#include "Global.h"
#include "utils.h"

#include "pbjson.hpp"

#include <stout/uuid.hpp>

#include <iostream>
#include <set>
#include <unordered_set>
#include <unordered_map>

using namespace arangodb;
using namespace std;

using std::chrono::system_clock;

// -----------------------------------------------------------------------------
// --SECTION--                                                 private functions
// -----------------------------------------------------------------------------

///////////////////////////////////////////////////////////////////////////////
/// @brief finds free ports from an offer
///////////////////////////////////////////////////////////////////////////////

vector<uint32_t> findFreePorts (const mesos::Offer& offer, size_t len) {
  static const size_t MAX_ITERATIONS = 1000;

  vector<uint32_t> result;
  vector<mesos::Value::Range> resources;

  for (int i = 0; i < offer.resources_size(); ++i) {
    const auto& resource = offer.resources(i);

    if (resource.name() == "ports" &&
        resource.type() == mesos::Value::RANGES) {
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

// -----------------------------------------------------------------------------
// --SECTION--                                           class ArangoManagerImpl
// -----------------------------------------------------------------------------

class ArangoManagerImpl : public ArangoManager {
  public:
    ArangoManagerImpl ();
    ~ArangoManagerImpl ();

  public:
    void addOffer (const mesos::Offer& offer) override;
    void removeOffer (const mesos::OfferID& offerId) override;
    void taskStatusUpdate (const mesos::TaskStatus& status) override;
    void destroy () override;
    vector<string> readEndpoints () override;
    vector<string> writeEndpoints () override;

  private:
    void dispatch ();
    void applyStatusUpdates ();
    bool checkOutstandOffers ();
    void startNewInstances ();
    bool makePersistentVolume (const string& name, const mesos::Offer&, const mesos::Resources&);
    bool makeDynamicReservation (const mesos::Offer&, const mesos::Resources&);
    void startInstance (InstanceActionState, const ResourcesCurrentEntry&, const AspectPosition&);
    void fillKnownInstances (AspectType, const InstancesCurrent&);
    void killAllInstances ();

  private:
    mutex _lock;
    thread* _dispatcher;

    unordered_map<string, AspectPosition> _task2position;
    unordered_map<string, mesos::Offer> _storedOffers;
    vector<mesos::TaskStatus> _taskStatusUpdates;
};

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

ArangoManagerImpl::ArangoManagerImpl ()
  : _lock(),
    _dispatcher(nullptr),
    _task2position(),
    _storedOffers(),
    _taskStatusUpdates() {

  _dispatcher = new thread(&ArangoManagerImpl::dispatch, this);

  Current current = Global::state().current();

  fillKnownInstances(AspectType::AGENCY, current.agencies());
  fillKnownInstances(AspectType::COORDINATOR, current.coordinators());
  fillKnownInstances(AspectType::PRIMARY_DBSERVER, current.primary_dbservers());
  fillKnownInstances(AspectType::SECONDARY_DBSERVER, current.secondary_dbservers());
}

////////////////////////////////////////////////////////////////////////////////
/// @brief destructor
////////////////////////////////////////////////////////////////////////////////

ArangoManagerImpl::~ArangoManagerImpl () {
  _stopDispatcher = true;
  _dispatcher->join();

  delete _dispatcher;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief adds an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::addOffer (const mesos::Offer& offer) {
  lock_guard<mutex> lock(_lock);

  /*
  {
    string json;
    pbjson::pb2json(&offer, json);
    LOG(INFO)
    << "OFFER received: " << json;
  }
  */

  _storedOffers[offer.id().value()] = offer;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief removes an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::removeOffer (const mesos::OfferID& offerId) {
  lock_guard<mutex> lock(_lock);

  string id = offerId.value();

  LOG(INFO)
  << "OFFER removed: " << id;
  
  _storedOffers.erase(id);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief status update
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::taskStatusUpdate (const mesos::TaskStatus& status) {
  lock_guard<mutex> lock(_lock);

  _taskStatusUpdates.push_back(status);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief destroys the cluster and shuts down the scheduler
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::destroy () {
  killAllInstances();

  Global::state().destroy();

  Global::scheduler().stop();

  string body = "frameworkId=" + Global::state().frameworkId();
  Global::scheduler().postRequest("master/shutdown", body);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief endpoints for reading
////////////////////////////////////////////////////////////////////////////////

vector<string> ArangoManagerImpl::readEndpoints () {
  Current current = Global::state().current();
  auto const& dbservers = current.primary_dbservers();

  vector<string> endpoints;

  for (int i = 0;  i < dbservers.entries_size();  ++i) {
    auto const& dbserver = dbservers.entries(i);
    string endpoint = "http://" + dbserver.hostname() + ":" + to_string(dbserver.ports(0));

    endpoints.push_back(endpoint);
  }

  return endpoints;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief endpoints for writing
////////////////////////////////////////////////////////////////////////////////

vector<string> ArangoManagerImpl::writeEndpoints () {
  Current current = Global::state().current();
  auto const& dbservers = current.primary_dbservers();

  vector<string> endpoints;

  for (int i = 0;  i < dbservers.entries_size();  ++i) {
    auto const& dbserver = dbservers.entries(i);
    string endpoint = "http://" + dbserver.hostname() + ":" + to_string(dbserver.ports(0));

    endpoints.push_back(endpoint);
  }

  return endpoints;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                   private methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief main dispatcher
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::dispatch () {
  static const int SLEEP_SEC = 10;

  unordered_set<string> bootstrapped;

  while (! _stopDispatcher) {
    bool found;
    Global::state().frameworkId(found);

    if (! found) {
      this_thread::sleep_for(chrono::seconds(SLEEP_SEC));
      continue;
    }

    // apply received status updates
    applyStatusUpdates();

    // check all outstanding offers
    bool sleep = checkOutstandOffers();

    // check if we can start new instances
    startNewInstances();

    // wait for a little while
    if (sleep) {
      this_thread::sleep_for(chrono::seconds(SLEEP_SEC));
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief applies status updates
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::applyStatusUpdates () {
  Caretaker& caretaker = Global::caretaker();

  lock_guard<mutex> lock(_lock);

  for (auto&& status : _taskStatusUpdates) {
    mesos::TaskID taskId = status.task_id();
    const AspectPosition& pos = _task2position[taskId.value()];

    caretaker.setTaskStatus(pos, status);

    switch (status.state()) {
      case mesos::TASK_STAGING:
        break;

      case mesos::TASK_RUNNING:
        caretaker.setInstanceState(pos, INSTANCE_STATE_RUNNING);
        break;

      case mesos::TASK_STARTING:
        // do nothing
        break;

      case mesos::TASK_FINISHED: // TERMINAL. The task finished successfully.
      case mesos::TASK_FAILED:   // TERMINAL. The task failed to finish successfully.
      case mesos::TASK_KILLED:   // TERMINAL. The task was killed by the executor.
      case mesos::TASK_LOST:     // TERMINAL. The task failed but can be rescheduled.
      case mesos::TASK_ERROR:    // TERMINAL. The task failed but can be rescheduled.
        caretaker.setInstanceState(pos, INSTANCE_STATE_STOPPED);
        caretaker.freeResourceForInstance(pos);
        break;
    }
  }

  _taskStatusUpdates.clear();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks available offers
////////////////////////////////////////////////////////////////////////////////

bool ArangoManagerImpl::checkOutstandOffers () {
  Caretaker& caretaker = Global::caretaker();

  unordered_map<string, mesos::Offer> next;
  vector<pair<mesos::Offer, mesos::Resources>> dynamic;
  vector<pair<mesos::Offer, OfferAction>> persistent;
  vector<mesos::Offer> declined;

  {
    lock_guard<mutex> lock(_lock);

    caretaker.updatePlan();

    for (auto&& id_offer : _storedOffers) {
      OfferAction action = caretaker.checkOffer(id_offer.second);

      switch (action._state) {
        case OfferActionState::IGNORE:
          declined.push_back(id_offer.second);
          break;

        case OfferActionState::USABLE:
          break;

        case OfferActionState::STORE_FOR_LATER:
          declined.push_back(id_offer.second);

          // TODO(fc) do we need to keep the offer for a while?
          // next[id_offer.first] = id_offer.second;

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
  // decline unusable offers
  // .............................................................................

  for (auto&& offer : declined) {
    Global::scheduler().declineOffer(offer.id());
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

  return sleep;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief starts new instances
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::startNewInstances () {
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
        case::InstanceActionState::START_SECONDARY_DBSERVER:
          start.push_back(action);
          break;
      }
    }
    while (action._state != InstanceActionState::DONE);
  }

  for (auto&& action : start) {
    startInstance(action._state, action._info, action._pos);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief makes a persistent volume
////////////////////////////////////////////////////////////////////////////////

bool ArangoManagerImpl::makePersistentVolume (const string& name,
                                              const mesos::Offer& offer,
                                              const mesos::Resources& resources) {
  const string& offerId = offer.id().value();
  const string& slaveId = offer.slave_id().value();

  if (resources.empty()) {
    LOG(WARNING)
    << "cannot make persistent volume from empty resource "
    << "(offered resource was " << offer.resources() << ")";

    return false;
  }

  mesos::Resource disk = *resources.begin();
  mesos::Resource::DiskInfo diskInfo;

  diskInfo.mutable_persistence()->set_id(name + "_" + slaveId);

  mesos::Volume volume;

  volume.set_container_path(name);
  volume.set_mode(mesos::Volume::RW);

  diskInfo.mutable_volume()->CopyFrom(volume);
  disk.mutable_disk()->CopyFrom(diskInfo);

  mesos::Resources persistent;
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

bool ArangoManagerImpl::makeDynamicReservation (const mesos::Offer& offer,
                                                const mesos::Resources& resources) {
  const string& offerId = offer.id().value();

#if MESOS_RESERVE_PORTS
  mesos::Resources res = resources;
#else
  mesos::Resources res = filterNotIsPorts(resources);
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

  mesos::Resources offered = offer.resources();
  mesos::Resources diff = res - offered;

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
/// @brief starts a new standalone arangodb
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::startInstance (InstanceActionState aspect,
                                       const ResourcesCurrentEntry& info,
                                       const AspectPosition& pos) {
  lock_guard<mutex> lock(_lock);

  string taskId = UUID::random().toString();

  if (info.ports_size() != 1) {
    LOG(WARNING)
    << "expected one port, got " << info.ports_size();
    return;
  }

  // use docker to run the task
  mesos::ContainerInfo container;
  container.set_type(mesos::ContainerInfo::DOCKER);

  // command to execute
  mesos::CommandInfo command;
  command.set_value("standalone");
  command.set_shell(false);

  // docker info
  mesos::ContainerInfo::DockerInfo* docker = container.mutable_docker();
  docker->set_image("arangodb/arangodb-mesos");
  docker->set_network(mesos::ContainerInfo::DockerInfo::BRIDGE);

  // port mapping
  mesos::ContainerInfo::DockerInfo::PortMapping* mapping = docker->add_port_mappings();
  mapping->set_host_port(info.ports(0));
  mapping->set_container_port(8529);
  mapping->set_protocol("tcp");

  // volume
  string path = "arangodb_" + Global::frameworkName() + "_standalone";

  mesos::Volume* volume = container.add_volumes();
  volume->set_container_path("/data");
  volume->set_host_path(Global::volumePath() + "/" + path);
  volume->set_mode(mesos::Volume::RW);

  // and start
  mesos::TaskInfo taskInfo = Global::scheduler().startInstance(
    taskId,
    "standalone",
    info,
    container,
    command);

  _task2position[taskId] = pos;

  Caretaker& caretaker = Global::caretaker();
  caretaker.setTaskInfo(pos, taskInfo);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief recover task mapping
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::fillKnownInstances (AspectType type,
                                            const InstancesCurrent& instances) {
  for (int i = 0;  i < instances.entries_size();  ++i) {
    const InstancesCurrentEntry& entry = instances.entries(i);

    if (entry.has_task_info()) {
      _task2position[entry.task_info().task_id().value()] = { type, (size_t) i };
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief kills all running tasks
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::killAllInstances () {
  for (const auto& task : _task2position) {
    const auto& id = task.first;

    Global::scheduler().killInstance(id);
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

ArangoManager::ArangoManager ()
  : _stopDispatcher(false) {
}

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

ArangoManager* ArangoManager::New () {
  return new ArangoManagerImpl();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief destructor
////////////////////////////////////////////////////////////////////////////////

ArangoManager::~ArangoManager () {
  _stopDispatcher = true;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
