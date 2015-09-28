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
#include "Global.h"
#include "utils.h"

#include "pbjson.hpp"

#include <stout/uuid.hpp>

#include <iostream>
#include <set>
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
// --SECTION--                                               class ArangoManager
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

ArangoManager::ArangoManager ()
  : _stopDispatcher(false),
    _dispatcher(nullptr),
    _nextImplicitReconciliation(chrono::steady_clock::now()),
    _implicitReconciliationIntervall(chrono::minutes(5)),
    _maxReconcileIntervall(chrono::minutes(5)),
    _task2position(),
    _lock(),
    _storedOffers(),
    _taskStatusUpdates() {

  _dispatcher = new thread(&ArangoManager::dispatch, this);

  Current current = Global::state().current();

  fillKnownInstances(TaskType::AGENT, current.agents());
  fillKnownInstances(TaskType::COORDINATOR, current.coordinators());
  fillKnownInstances(TaskType::PRIMARY_DBSERVER, current.dbservers());
  fillKnownInstances(TaskType::SECONDARY_DBSERVER, current.secondaries());
}

////////////////////////////////////////////////////////////////////////////////
/// @brief destructor
////////////////////////////////////////////////////////////////////////////////

ArangoManager::~ArangoManager () {
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

void ArangoManager::addOffer (const mesos::Offer& offer) {
  lock_guard<mutex> lock(_lock);

#if 0
  // This is already logged in the scheduler in more concise format.
  {
    string json;
    pbjson::pb2json(&offer, json);
    LOG(INFO)
    << "OFFER received: " << json;
  }
#endif

  _storedOffers[offer.id().value()] = offer;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief removes an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::removeOffer (const mesos::OfferID& offerId) {
  lock_guard<mutex> lock(_lock);

  string id = offerId.value();

  LOG(INFO)
  << "OFFER removed: " << id;
  
  _storedOffers.erase(id);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief status update
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::taskStatusUpdate (const mesos::TaskStatus& status) {
  lock_guard<mutex> lock(_lock);

  _taskStatusUpdates.push_back(status);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief destroys the cluster and shuts down the scheduler
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::destroy () {
  LOG(INFO) << "destroy() called, killing off everything...";
  killAllInstances();

  sleep(60);

  Global::state().destroy();

  Global::scheduler().stop();

  string body = "frameworkId=" + Global::state().frameworkId();
  Global::scheduler().postRequest("master/shutdown", body);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief endpoints of the coordinators
////////////////////////////////////////////////////////////////////////////////

vector<string> ArangoManager::coordinatorEndpoints () {
  Current current = Global::state().current();
  //auto const& coordinators = current.coordinators();
  auto const& coordinators = current.coordinators();

  vector<string> endpoints;

  for (int i = 0;  i < coordinators.entries_size();  ++i) {
    //auto const& coordinator = coordinators.entries(i);
    auto const& coord_res = coordinators.entries(i);
    if (coord_res.has_hostname() && coord_res.ports_size() > 0) {
      string endpoint = "http://" + coord_res.hostname() + ":" 
                        + to_string(coord_res.ports(0));
      endpoints.push_back(endpoint);
    }
  }

  return endpoints;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief endpoints of the DBservers
////////////////////////////////////////////////////////////////////////////////

vector<string> ArangoManager::dbserverEndpoints () {
  Current current = Global::state().current();
  auto const& dbservers = current.dbservers();

  vector<string> endpoints;

  for (int i = 0; i < dbservers.entries_size();  ++i) {
    auto const& dbs_res = dbservers.entries(i);
    if (dbs_res.has_hostname() && dbs_res.ports_size() > 0) {
      string endpoint = "http://" + dbs_res.hostname() + ":" 
                        + to_string(dbs_res.ports(0));
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

void ArangoManager::dispatch () {
  //static const int SLEEP_SEC = 10;
  static const int SLEEP_SEC = 1;

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
    checkOutstandOffers();

    // apply any timeouts
    bool sleep = checkTimeouts();

    // initialise cluster when it is up:
    auto cur = Global::state().current();

    if (  cur.cluster_complete() &&
        ! cur.cluster_initialized()) {
      LOG(INFO)
      << "calling initializeCluster()...";
      initializeCluster();
    }

    // wait for a little while, if we are idle
    if (sleep) {
      this_thread::sleep_for(chrono::seconds(SLEEP_SEC));
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief prepares the reconciliation of tasks
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::prepareReconciliation () {
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

void ArangoManager::reconcileTasks () {

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

static double const TryingToReserveTimeout = 30;  // seconds
static double const TryingToPersistTimeout = 30;
static double const TryingToStartTimeout   = 120; // docker pull might take
static double const TryingToRestartTimeout = 120;
static double const TryingToResurrectTimeout = 900;  // Patience before we
                                                     // give up on a persistent
                                                     // task.
// Note that the failover timeout can be configured by the user via a
// command line option.

bool ArangoManager::checkTimeouts () {
  std::vector<TaskType> types 
    = { TaskType::AGENT, TaskType::PRIMARY_DBSERVER,
        TaskType::SECONDARY_DBSERVER, TaskType::COORDINATOR };

  auto plan = Global::state().plan();
  auto current = Global::state().current();

  for (auto taskType : types) {
    TasksPlan* tasksPlan;
    TasksCurrent* tasksCurr;
    switch (taskType) {
      case TaskType::AGENT:
        tasksPlan = plan.mutable_agents();
        tasksCurr = current.mutable_agents();
        break;
      case TaskType::PRIMARY_DBSERVER:
        tasksPlan = plan.mutable_dbservers();
        tasksCurr = current.mutable_dbservers();
        break;
      case TaskType::SECONDARY_DBSERVER:
        tasksPlan = plan.mutable_secondaries();
        tasksCurr = current.mutable_secondaries();
        break;
      case TaskType::COORDINATOR:
        tasksPlan = plan.mutable_coordinators();
        tasksCurr = current.mutable_coordinators();
        break;
      default:  // never happens
        tasksPlan = nullptr;
        tasksCurr = nullptr;
        break;
    }
    double now = chrono::duration_cast<chrono::seconds>(
      chrono::steady_clock::now().time_since_epoch()).count();
    double timeStamp;
    for (int i = 0; i < tasksPlan->entries_size(); i++) {
      TaskPlan* tp = tasksPlan->mutable_entries(i);
      TaskCurrent* ic = tasksCurr->mutable_entries(i);
      switch (tp->state()) {
        case TASK_STATE_NEW:
          // Wait forever here, no timeout.
          break;
        case TASK_STATE_TRYING_TO_RESERVE:
          // After a timeout, go back to state TASK_STATE_NEW, because
          // there was no satisfactory answer to our reservation request:
          timeStamp = tp->started();
          if (timeStamp - now > TryingToReserveTimeout) {
            LOG(INFO) << "Timeout " << TryingToReserveTimeout << "s reached "
                      << " for task " << ic->task_info().name();
            LOG(INFO) << "Calling UNRESERVE for offer ";
            // FIXME: call UNRESERVE, unfortunately, we do no longer know
            // what we reserved. :-(
            tp->set_state(TASK_STATE_NEW);
          }
          break;
        case TASK_STATE_TRYING_TO_PERSIST:
          // After a timeout, go back to state TASK_STATE_NEW, because
          // there was no satisfactory answer to our persistence request:
          timeStamp = tp->started();
          if (timeStamp - now > TryingToPersistTimeout) {
            LOG(INFO) << "Timeout " << TryingToPersistTimeout << "s reached "
                      << " for task " << ic->task_info().name();
            LOG(INFO) << "Calling UNRESERVE for offer ";
            // FIXME: call UNRESERVE, unfortunately, we do no longer know
            // what we did reserved. :-(
            tp->set_state(TASK_STATE_NEW);
          }
          // ...
          break;
        case TASK_STATE_TRYING_TO_START:
          // After a timeout, go back to state TASK_STATE_NEW, because
          // there was no satisfactory answer to our start request:
          // ...
          break;
        case TASK_STATE_RUNNING:
          // Run forever here, no timeout.
          break;
        case TASK_STATE_KILLED:
          // After some time being killed, we have to take action and
          // engage in some automatic failover procedure:
          // ...
          break;
        case TASK_STATE_TRYING_TO_RESTART:
          // We got the offer for a restart, but the restart is not happening.
          // We need to go back to state TASK_STATE_KILLED to wait for another
          // offer.
          // ...
          break;
        case TASK_STATE_FAILED_OVER:
          // This task has been replaced by its failover partner, now we
          // finally lose patience to wait for a restart and give up on the
          // task. We free all resources and go back to TASK_STATE_NEW
          // ...
          break;
        case TASK_STATE_DEAD:
          // This task is no longer used. Do nothing.
          break;
      }
    }
  }
  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief applies status updates
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::applyStatusUpdates () {
  Caretaker& caretaker = Global::caretaker();

  lock_guard<mutex> lock(_lock);

  for (auto&& status : _taskStatusUpdates) {
    mesos::TaskID taskId = status.task_id();
    string taskIdStr = taskId.value();

    _reconcilationTasks.erase(taskIdStr);

    std::pair<TaskType, int>& pos = _task2position[taskIdStr];

    caretaker.setTaskStatus(pos.first, pos.second, status);

    switch (status.state()) {
      case mesos::TASK_STAGING:
        break;

      case mesos::TASK_RUNNING:
        caretaker.setTaskPlanState(pos.first, pos.second, TASK_STATE_RUNNING);
        caretaker.setInstanceState(pos.first, pos.second, INSTANCE_STATE_RUNNING);
        break;

      case mesos::TASK_STARTING:
        // do nothing
        break;

      case mesos::TASK_FINISHED: // TERMINAL. The task finished successfully.
      case mesos::TASK_FAILED:   // TERMINAL. The task failed to finish successfully.
      case mesos::TASK_KILLED:   // TERMINAL. The task was killed by the executor.
      case mesos::TASK_LOST:     // TERMINAL. The task failed but can be rescheduled.
      case mesos::TASK_ERROR:    // TERMINAL. The task failed but can be rescheduled.
        caretaker.setTaskPlanState(pos.first, pos.second, TASK_STATE_KILLED);
        caretaker.setInstanceState(pos.first, pos.second, INSTANCE_STATE_STOPPED);
        break;
    }
  }

  _taskStatusUpdates.clear();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks available offers
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::checkOutstandOffers () {
  Caretaker& caretaker = Global::caretaker();

  // ...........................................................................
  // first of all, update our plan
  // ...........................................................................

  caretaker.updatePlan();

  // ...........................................................................
  // check all stored offers
  // ...........................................................................

  {
    lock_guard<mutex> lock(_lock);

    for (auto&& id_offer : _storedOffers) {
      caretaker.checkOffer(id_offer.second);
    }

    _storedOffers.clear();
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief recover task mapping
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::fillKnownInstances (TaskType type,
                                        TasksCurrent const& currents) {
  LOG(INFO)
  << "recovering instance type " << (int) type;

  for (int i = 0;  i < currents.entries_size();  ++i) {
    TaskCurrent const& entry = currents.entries(i);

    if (entry.has_task_info()) {
      string id = entry.task_info().task_id().value();

      LOG(INFO)
      << "for task id " << id << ": " << i;

      _task2position[id] = std::make_pair(type, i);
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief kills all running tasks
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::killAllInstances () {
  for (auto const& task : _task2position) {
    auto const& id = task.first;

    Global::scheduler().killInstance(id);
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
