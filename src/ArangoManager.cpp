//////////////////////////////////////////////////////////////////////////////
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
#include <random>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>

using namespace arangodb;
using namespace std;

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
/// @brief bootstraps a dbserver
///////////////////////////////////////////////////////////////////////////////

static bool bootstrapDBservers () {
  string hostname 
    = Global::state().current().coordinators().entries(0).hostname();
  uint32_t port
    = Global::state().current().coordinators().entries(0).ports(0);
  string url = "http://" + hostname + ":" + to_string(port) +
                    "/_admin/cluster/bootstrapDbServers";
  string body = "{\"isRelaunch\":false}";
  string result;
  LOG(INFO) << "doing HTTP POST to " << url;
  int res = doHTTPPost(url, body, result);
  if (res != 0) {
    LOG(WARNING)
    << "bootstrapDBservers did not work, curl error: " << res << ", result:\n"
    << result;
    return false;
  }
  LOG(INFO) << "bootstrapDBservers answered:" << result;
  return true;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief upgrades the cluster database
///////////////////////////////////////////////////////////////////////////////

static bool upgradeClusterDatabase () {
  string hostname 
    = Global::state().current().coordinators().entries(0).hostname();
  uint32_t port
    = Global::state().current().coordinators().entries(0).ports(0);
  string url = "http://" + hostname + ":" + to_string(port) +
                    "/_admin/cluster/upgradeClusterDatabase";
  string body = "{\"isRelaunch\":false}";
  string result;
  LOG(INFO) << "doing HTTP POST to " << url;
  int res = doHTTPPost(url, body, result);
  if (res != 0) {
    LOG(WARNING)
    << "upgradeClusterDatabase did not work, curl error: " << res 
    << ", result:\n" << result;
    return false;
  }
  LOG(INFO) << "upgradeClusterDatabase answered:" << result;
  return true;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief bootstraps coordinators
///////////////////////////////////////////////////////////////////////////////

static bool bootstrapCoordinators () {
  int number
    = Global::state().current().coordinators().entries_size();
  bool error = false;
  for (int i = 0; i < number; i++) { 
    string hostname 
      = Global::state().current().coordinators().entries(i).hostname();
    uint32_t port
      = Global::state().current().coordinators().entries(i).ports(0);
    string url = "http://" + hostname + ":" + to_string(port) +
                      "/_admin/cluster/bootstrapCoordinator";
    string body = "{\"isRelaunch\":false}";
    string result;
    LOG(INFO) << "doing HTTP POST to " << url;
    int res = doHTTPPost(url, body, result);
    if (res != 0) {
      LOG(WARNING)
      << "bootstrapCoordinator did not work for " << i 
      << ", curl error: " << res << ", result:\n"
      << result;
      error = true;
    }
    else {
      LOG(INFO) << "bootstrapCoordinator answered:" << result;
    }
  }
  return ! error;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief initialize the cluster
///////////////////////////////////////////////////////////////////////////////

static void initializeCluster() {
  auto cur = Global::state().current();
  if (! cur.cluster_bootstrappeddbservers()) {
    if (! bootstrapDBservers()) {
      return;
    }
    cur.set_cluster_bootstrappeddbservers(true);
    Global::state().setCurrent(cur);
  }
  if (! cur.cluster_upgradeddb()) {
    if (! upgradeClusterDatabase()) {
      return;
    }
    cur.set_cluster_upgradeddb(true);
    Global::state().setCurrent(cur);
  }
  if (! cur.cluster_bootstrappedcoordinators()) {
    if (bootstrapCoordinators()) {
      cur.set_cluster_bootstrappedcoordinators(true);
      cur.set_cluster_initialized(true);
      Global::state().setCurrent(cur);
      LOG(INFO) << "cluster is ready";
    }
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                              class ReconcileTasks
// -----------------------------------------------------------------------------

class ReconcileTasks {
  public:
    mesos::TaskStatus _status;
    chrono::steady_clock::time_point _nextReconcile;
    chrono::steady_clock::duration _backoff;
};

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
    vector<string> coordinatorEndpoints () override;
    vector<string> dbserverEndpoints () override;

  private:
    void dispatch ();
    void prepareReconciliation ();
    void reconcileTasks ();
    void checkTimeouts ();
    void applyStatusUpdates ();
    bool checkOutstandOffers ();
    void startNewInstances ();
    bool makePersistentVolume (const string& name, const mesos::Offer&, const mesos::Resources&);
    bool makeDynamicReservation (const mesos::Offer&, const mesos::Resources&);
    void startInstance (InstanceActionState, const ResourceCurrent&, const AspectPosition&);
    void fillKnownInstances (AspectType, const InstancesCurrent&);
    void killAllInstances ();

  private:
    mutex _lock;
    thread* _dispatcher;

    chrono::steady_clock::time_point _nextImplicitReconciliation;
    chrono::steady_clock::duration _implicitReconciliationIntervall;
    chrono::steady_clock::duration _maxReconcileIntervall;

    unordered_map<string, ReconcileTasks> _reconcilationTasks;

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

  fillKnownInstances(AspectType::AGENT, current.agents());
  fillKnownInstances(AspectType::COORDINATOR, current.coordinators());
  fillKnownInstances(AspectType::PRIMARY_DBSERVER, current.primary_dbservers());
  fillKnownInstances(AspectType::SECONDARY_DBSERVER, current.secondary_dbservers());

  _nextImplicitReconciliation = chrono::steady_clock::now();
  _implicitReconciliationIntervall = chrono::minutes(5);
  _maxReconcileIntervall = chrono::minutes(5);
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
  LOG(INFO) << "destroy() called, killing off everything...";
  killAllInstances();

  Global::state().destroy();

  Global::scheduler().stop();

  string body = "frameworkId=" + Global::state().frameworkId();
  Global::scheduler().postRequest("master/shutdown", body);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief endpoints of the coordinators
////////////////////////////////////////////////////////////////////////////////

vector<string> ArangoManagerImpl::coordinatorEndpoints () {
  Current current = Global::state().current();
  auto const& coordinators = current.coordinators();

  vector<string> endpoints;

  for (int i = 0;  i < coordinators.entries_size();  ++i) {
    auto const& coordinator = coordinators.entries(i);
    if (coordinator.has_hostname() && coordinator.ports_size() > 0) {
      string endpoint = "http://" + coordinator.hostname() + ":" + to_string(coordinator.ports(0));
      endpoints.push_back(endpoint);
    }
  }

  return endpoints;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief endpoints of the DBservers
////////////////////////////////////////////////////////////////////////////////

vector<string> ArangoManagerImpl::dbserverEndpoints () {
  Current current = Global::state().current();
  auto const& dbservers = current.primary_dbservers();

  vector<string> endpoints;

  for (int i = 0;  i < dbservers.entries_size();  ++i) {
    auto const& dbserver = dbservers.entries(i);
    if (dbserver.has_hostname() && dbserver.ports_size() > 0) {
      string endpoint = "http://" + dbserver.hostname() + ":" + to_string(dbserver.ports(0));
      endpoints.push_back(endpoint);
    }
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

  prepareReconciliation();

  while (! _stopDispatcher) {
    bool found;
    Global::state().frameworkId(found);

    if (! found) {
      this_thread::sleep_for(chrono::seconds(SLEEP_SEC));
      continue;
    }

    // start reconciliation
    reconcileTasks();

    // apply received status updates
    applyStatusUpdates();

    // check all outstanding offers
    bool sleep = checkOutstandOffers();

    // apply any timeouts
    checkTimeouts();

    // check if we can start new instances
    startNewInstances();

    // initialise cluster when it is up:
    auto cur = Global::state().current();

    if (  cur.cluster_complete() &&
        ! cur.cluster_initialized()) {
      LOG(INFO)
      << "calling initializeCluster()...";
      initializeCluster();
    }

    // wait for a little while
    if (sleep) {
      this_thread::sleep_for(chrono::seconds(SLEEP_SEC));
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief prepares the reconciliation of tasks
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::prepareReconciliation () {
  vector<mesos::TaskStatus> status = Global::state().knownTaskStatus();
  auto now = chrono::steady_clock::now();

  for (auto&& taskStatus : status) {
    const string& taskId = taskStatus.task_id().value();

    auto nextReconcile = now;
    auto backoff = chrono::seconds(1);

    ReconcileTasks reconcile = {
      taskStatus,
      nextReconcile,
      backoff
    };

    _reconcilationTasks[taskId] = reconcile;
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief tries to recover tasks
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::reconcileTasks () {

  // see http://mesos.apache.org/documentation/latest/reconciliation/
  // for details about reconciliation

  auto now = chrono::steady_clock::now();

  // first, we as implicit reconciliation periodically
  if (_nextImplicitReconciliation >= now) {
    LOG(INFO)
    << "DEBUG implicit reconciliation";

    Global::scheduler().reconcileTasks();
    _nextImplicitReconciliation = now + _implicitReconciliationIntervall;
  }

  // check for unknown tasks
  for (auto&& task : _reconcilationTasks) {
    if (task.second._nextReconcile > now) {
      LOG(INFO)
      << "DEBUG explicit reconciliation for "
      << task.first;

      Global::scheduler().reconcileTask(task.second._status);

      task.second._backoff *= 2;

      if (task.second._backoff >= _maxReconcileIntervall) {
        task.second._backoff = _maxReconcileIntervall;
      }

      task.second._nextReconcile = now + task.second._backoff;
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks for timeout
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::checkTimeouts () {
  // TODO
}

////////////////////////////////////////////////////////////////////////////////
/// @brief applies status updates
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::applyStatusUpdates () {
  Caretaker& caretaker = Global::caretaker();

  lock_guard<mutex> lock(_lock);

  for (auto&& status : _taskStatusUpdates) {
    mesos::TaskID taskId = status.task_id();
    string taskIdStr = taskId.value();

    _reconcilationTasks.erase(taskIdStr);

    const AspectPosition& pos = _task2position[taskIdStr];

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
  // try to make the dynamic reservations
  // .............................................................................

  bool sleep = true;

  for (auto&& offer_res : dynamic) {
    bool res = makeDynamicReservation(offer_res.first, offer_res.second);

    if (res) {
      sleep = false;
    }
  }

  // .............................................................................
  // try to make a persistent volumes
  // .............................................................................

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

        case::InstanceActionState::START_AGENT:
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
  mesos::Resources res = resources.flatten(Global::role(), Global::principal());

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
/// @brief do IP address lookup
////////////////////////////////////////////////////////////////////////////////

static string getIPAddress (string hostname) {
  struct addrinfo hints;
  struct addrinfo* ai;
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = 0;
  hints.ai_flags = AI_ADDRCONFIG;
  int res = getaddrinfo(hostname.c_str(), nullptr, &hints, &ai);
  if (res != 0) {
    LOG(WARNING) << "Alarm: res=" << res;
    return hostname;
  }
  struct addrinfo* b = ai;
  std::string result = hostname;
  while (b != nullptr) {
    auto q = reinterpret_cast<struct sockaddr_in*>(ai->ai_addr);
    char buffer[INET_ADDRSTRLEN+5];
    char const* p = inet_ntop(AF_INET, &q->sin_addr, buffer, sizeof(buffer));
    if (p != nullptr) {
      if (p[0] != '1' || p[1] != '2' || p[2] != '7') {
        result = p;
      }
    }
    else {
      LOG(WARNING) << "error in inet_ntop";
    }
    b = b->ai_next;
  }
  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief starts a new standalone arangodb
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::startInstance (InstanceActionState aspect,
                                       const ResourceCurrent& info,
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

  // our own name:
  string type;
  switch (aspect) {
    case InstanceActionState::START_AGENT:
      type = "Agent";
      break;
    case InstanceActionState::START_PRIMARY_DBSERVER:
      type = "DBServer";
      break;
    case InstanceActionState::START_COORDINATOR:
      type = "Coordinator";
      break;
    case InstanceActionState::START_SECONDARY_DBSERVER:
      type = "Secondary";
      break;
    case InstanceActionState::DONE:
      assert(false);
      break;
  }
  string myName = type + to_string(pos._pos + 1);

  // command to execute
  mesos::CommandInfo command;
  mesos::Environment environment;
  switch (aspect) {
    case InstanceActionState::START_AGENT: {
      command.set_value("agency");
      auto p = environment.add_variables();
      p->set_name("numberOfDBServers");
      p->set_value(to_string(Global::state().targets().dbservers().instances()));
      p = environment.add_variables();
      p->set_name("numberOfCoordinators");
      p->set_value(to_string(Global::state().targets().coordinators().instances()));
      p = environment.add_variables();
      p->set_name("asyncReplication");
      p->set_value(Global::asyncReplication() ? string("true")
                                              : string("false"));
      break;
    }
    case InstanceActionState::START_PRIMARY_DBSERVER:
    case InstanceActionState::START_COORDINATOR:
    case InstanceActionState::START_SECONDARY_DBSERVER: {
      if (Global::mode() == OperationMode::STANDALONE) {
        command.set_value("standalone");
      }
      else {
        command.set_value("cluster");
        string hostname = Global::state().current().agents().entries(0).hostname();
        uint32_t port = Global::state().current().agents().entries(0).ports(0);
        command.add_arguments(
            "tcp://" + getIPAddress(hostname) + ":" + to_string(port));
        command.add_arguments(myName);
      }
      break;
    }
    case InstanceActionState::DONE: {
      assert(false);
      break;
    }
  }
  command.set_shell(false);
  // Find out the IP address:

  auto p = environment.add_variables();
  p->set_name("HOST");
  p->set_value(getIPAddress(info.hostname()));
  p = environment.add_variables();
  p->set_name("PORT0");
  p->set_value(std::to_string(info.ports(0)));
  command.mutable_environment()->CopyFrom(environment);

  // docker info
  mesos::ContainerInfo::DockerInfo* docker = container.mutable_docker();
  docker->set_image("arangodb/arangodb-mesos:latest");
  docker->set_network(mesos::ContainerInfo::DockerInfo::BRIDGE);

  // port mapping
  mesos::ContainerInfo::DockerInfo::PortMapping* mapping = docker->add_port_mappings();
  mapping->set_host_port(info.ports(0));
  switch (aspect) {
    case InstanceActionState::START_AGENT:
      mapping->set_container_port(4001);
      break;
    case InstanceActionState::START_PRIMARY_DBSERVER:
      mapping->set_container_port(8529);
      break;
    case InstanceActionState::START_COORDINATOR:
      mapping->set_container_port(8529);
      break;
    case InstanceActionState::START_SECONDARY_DBSERVER:
      mapping->set_container_port(8529);
      break;
    case InstanceActionState::DONE:
      assert(false);
      break;
  }
  mapping->set_protocol("tcp");

  // volume
  string path = "arangodb_" + Global::frameworkName() + "_" 
                + Global::state().frameworkId() + "_" + myName;
  mesos::Volume* volume = container.add_volumes();
  volume->set_container_path("/data");
  volume->set_host_path(Global::volumePath() + "/" + path);
  volume->set_mode(mesos::Volume::RW);

  // sets the task_id (in case we crash) before we start
  Caretaker& caretaker = Global::caretaker();

  mesos::TaskID tid;
  tid.set_value(taskId);

  caretaker.setTaskId(pos, tid);

  // and start
  mesos::TaskInfo taskInfo = Global::scheduler().startInstance(
    taskId,
    myName,
    info,
    container,
    command);

  _task2position[taskId] = pos;

  caretaker.setTaskInfo(pos, taskInfo);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief recover task mapping
////////////////////////////////////////////////////////////////////////////////

void ArangoManagerImpl::fillKnownInstances (AspectType type,
                                            const InstancesCurrent& instances) {
  LOG(INFO)
  << "recovering instance type " << (int) type;

  for (int i = 0;  i < instances.entries_size();  ++i) {
    const InstanceCurrent& entry = instances.entries(i);

    if (entry.has_task_info()) {
      string id = entry.task_info().task_id().value();

      LOG(INFO)
      << "for task id " << id << ": " << i;

      _task2position[id] = { type, (size_t) i };
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
