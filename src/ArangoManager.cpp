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
#include <chrono>
#include <thread>

using namespace arangodb;
using namespace std;

// -----------------------------------------------------------------------------
// --SECTION--                                                 private functions
// -----------------------------------------------------------------------------

///////////////////////////////////////////////////////////////////////////////
/// @brief bootstraps a dbserver
///////////////////////////////////////////////////////////////////////////////

static bool bootstrapDBservers (ArangoState::Lease& lease) {
  string hostname 
    = lease.state().current().coordinators().entries(0).hostname();
  uint32_t port
    = lease.state().current().coordinators().entries(0).ports(0);
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

static bool upgradeClusterDatabase (ArangoState::Lease& lease) {
  string hostname 
    = lease.state().current().coordinators().entries(0).hostname();
  uint32_t port
    = lease.state().current().coordinators().entries(0).ports(0);
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

static bool bootstrapCoordinators (ArangoState::Lease& lease) {
  int number
    = lease.state().current().coordinators().entries_size();
  bool error = false;
  for (int i = 0; i < number; i++) { 
    if (lease.state().plan().coordinators().entries(i).state() 
        == TASK_STATE_RUNNING) {
      string hostname 
        = lease.state().current().coordinators().entries(i).hostname();
      uint32_t port
        = lease.state().current().coordinators().entries(i).ports(0);
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
  }
  return ! error;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief initialize the cluster
///////////////////////////////////////////////////////////////////////////////

static void initializeCluster(ArangoState::Lease& l) {
  auto cur = l.state().mutable_current();
  if (! cur->cluster_bootstrappeddbservers()) {
    if (! bootstrapDBservers(l)) {
      return;
    }
    cur->set_cluster_bootstrappeddbservers(true);
    l.changed();
  }
  if (! cur->cluster_upgradeddb()) {
    if (! upgradeClusterDatabase(l)) {
      return;
    }
    cur->set_cluster_upgradeddb(true);
    l.changed();
  }
  if (! cur->cluster_bootstrappedcoordinators()) {
    if (bootstrapCoordinators(l)) {
      cur->set_cluster_bootstrappedcoordinators(true);
      cur->set_cluster_initialized(true);
      l.changed();
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

  auto lease = Global::state().lease(false);
  Current current = lease.state().current();

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
    LOG(INFO) << "OFFER received: " << arangodb::toJson(offer);
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

  LOG(INFO) << "OFFER removed: " << id;
  
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

  {
    // First set the target state to 0 instances:
    auto l = Global::state().lease(true);
    Targets* target = l.state().mutable_targets();
    target->mutable_agents()->set_instances(0);
    target->mutable_coordinators()->set_instances(0);
    target->mutable_dbservers()->set_instances(0);
    target->mutable_secondaries()->set_instances(0);

    LOG(INFO) << "The old state with DEAD tasks:CURRENT:\n"
              << arangodb::toJson(l.state().current());

    // Now set the state of all instances to TASK_STATE_DEAD:
    std::vector<std::string> ids;
    Plan* plan = l.state().mutable_plan();
    Current const& current = l.state().current();

    auto markAllDead = [&] (TasksPlan* entries, TasksCurrent const& currs) 
                       -> void {
      for (int i = 0; i < entries->entries_size(); i++) {
        TaskPlan* entry = entries->mutable_entries(i);
        if (entry->state() != TASK_STATE_DEAD) {
          LOG(INFO) << "Planning to kill instance with id '"
                    << currs.entries(i).task_info().task_id().value()
                    << "'";
          ids.push_back(currs.entries(i).task_info().task_id().value());
          entry->set_state(TASK_STATE_DEAD);
        }
      }
    };

    markAllDead(plan->mutable_agents(), current.agents());
    markAllDead(plan->mutable_dbservers(), current.dbservers());
    markAllDead(plan->mutable_secondaries(), current.secondaries());
    markAllDead(plan->mutable_coordinators(), current.coordinators());

    LOG(INFO) << "The new state with DEAD tasks:\nPLAN:"
              << arangodb::toJson(l.state().plan());

    killAllInstances(ids);
  }

  // During the following time we will get KILL messages, this will keep
  // the status and as a consequences we will destroy all persistent volumes,
  // unreserve all reserved resources and decline the offers:
  this_thread::sleep_for(chrono::seconds(60));

  string body;
  {
    auto l = Global::state().lease();
    body = "frameworkId=" + l.state().framework_id().value();
  }

  // Now everything should be down, so terminate for good:
  Global::state().destroy();

  Global::scheduler().stop();

  Global::scheduler().postRequest("master/shutdown", body);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief endpoints of the coordinators
////////////////////////////////////////////////////////////////////////////////

vector<string> ArangoManager::coordinatorEndpoints () {
  auto l = Global::state().lease();
  Current current = l.state().current();
  auto const& coordinators = current.coordinators();

  vector<string> endpoints;

  for (int i = 0;  i < coordinators.entries_size();  ++i) {
    auto const& coordinator = coordinators.entries(i);
    if (coordinator.has_hostname() && coordinator.ports_size() > 0) {
      string endpoint = "http://" + coordinator.hostname() + ":" 
                        + to_string(coordinator.ports(0));
      endpoints.push_back(endpoint);
    }
  }

  return endpoints;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief endpoints of the DBservers
////////////////////////////////////////////////////////////////////////////////

vector<string> ArangoManager::dbserverEndpoints () {
  auto l = Global::state().lease();
  Current current = l.state().current();
  auto const& dbservers = current.dbservers();

  vector<string> endpoints;

  for (int i = 0; i < dbservers.entries_size();  ++i) {
    auto const& dbserver = dbservers.entries(i);
    if (dbserver.has_hostname() && dbserver.ports_size() > 0) {
      string endpoint = "http://" + dbserver.hostname() + ":" 
                        + to_string(dbserver.ports(0));
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
    {
      auto l = Global::state().lease();
      found = l.state().has_framework_id();
    }

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

    {
      auto l = Global::state().lease();
      // initialise cluster when it is up:
      auto cur = l.state().current();

      if (  cur.cluster_complete() &&
          ! cur.cluster_initialized()) {
        LOG(INFO) << "calling initializeCluster()..."; initializeCluster(l);
      }
    }

    // wait for a little while, if we are idle
    if (sleep) {
      this_thread::sleep_for(chrono::seconds(SLEEP_SEC));
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief fill in TaskStatus
////////////////////////////////////////////////////////////////////////////////

static void fillTaskStatus (vector<pair<string,string>>& result,
                            TasksPlan const& plans,
                            TasksCurrent const& currents) {

  // we have to check the TaskInfo (not TaskStatus!)
  for (int i = 0;  i < currents.entries_size();  ++i) {
    TaskPlan const& planEntry = plans.entries(i);
    TaskCurrent const& entry = currents.entries(i);

    switch (planEntry.state()) {
      case TASK_STATE_NEW:
      case TASK_STATE_TRYING_TO_RESERVE:
      case TASK_STATE_TRYING_TO_PERSIST:
      case TASK_STATE_TRYING_TO_START:
      case TASK_STATE_TRYING_TO_RESTART:
      case TASK_STATE_RUNNING:
      case TASK_STATE_KILLED:
      case TASK_STATE_FAILED_OVER:
        // At this stage we do not distinguish the state, is this sensible?
        if (entry.has_task_info()) {
          auto const& info = entry.task_info();
          string taskId = info.task_id().value();
          string slaveId = info.slave_id().value();
          result.push_back(make_pair(taskId, slaveId));
        }

        break;
      case TASK_STATE_DEAD:
        break;
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief prepares the reconciliation of tasks
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::prepareReconciliation () {
  auto l = Global::state().lease();
  vector<pair<string,string>> taskSlaveIds;

  fillTaskStatus(taskSlaveIds, l.state().plan().agents(),
                               l.state().current().agents());
  fillTaskStatus(taskSlaveIds, l.state().plan().coordinators(), 
                               l.state().current().coordinators());
  fillTaskStatus(taskSlaveIds, l.state().plan().dbservers(),
                               l.state().current().dbservers());
  fillTaskStatus(taskSlaveIds, l.state().plan().secondaries(),
                               l.state().current().secondaries());

  auto now = chrono::steady_clock::now();

  for (auto const& taskSlaveId : taskSlaveIds) {
    auto nextReconcile = now;
    auto backoff = chrono::seconds(1);

    ReconcileTasks reconcile = {
      taskSlaveId.first,   // TaskId
      taskSlaveId.second,  // SlaveId
      nextReconcile,
      backoff
    };

    _reconciliationTasks[taskSlaveId.first] = reconcile;
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief tries to recover tasks
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::reconcileTasks () {

  // see http://mesos.apache.org/documentation/latest/reconciliation/
  // for details about reconciliation

  auto now = chrono::steady_clock::now();

  // first, we ask for implicit reconciliation periodically
  if (_nextImplicitReconciliation >= now) {
    LOG(INFO) << "DEBUG implicit reconciliation";

    Global::scheduler().reconcileTasks();
    _nextImplicitReconciliation = now + _implicitReconciliationIntervall;
  }

  // check for unknown tasks
  for (auto& task : _reconciliationTasks) {
    if (task.second._nextReconcile > now) {
      LOG(INFO) << "DEBUG explicit reconciliation for " << task.first;

      Global::scheduler().reconcileTask(task.second._taskId,
                                        task.second._slaveId);

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
static double const TryingToStartTimeout   = 300; // docker pull might take
static double const TryingToRestartTimeout = 300;
static double const FailoverTimeout        = 60;
static double const TryingToResurrectTimeout = 900;  // Patience before we
                                                     // give up on a persistent
                                                     // task.
// Note that the failover timeout can be configured by the user via a
// command line option.

bool ArangoManager::checkTimeouts () {
  auto l = Global::state().lease();

  std::vector<TaskType> types 
    = { TaskType::AGENT, TaskType::PRIMARY_DBSERVER,
        TaskType::SECONDARY_DBSERVER, TaskType::COORDINATOR };

  auto* plan = l.state().mutable_plan();
  auto* current = l.state().mutable_current();

  for (auto taskType : types) {
    TasksPlan* tasksPlan;
    TasksCurrent* tasksCurr;
    switch (taskType) {
      case TaskType::AGENT:
        tasksPlan = plan->mutable_agents();
        tasksCurr = current->mutable_agents();
        break;
      case TaskType::PRIMARY_DBSERVER:
        tasksPlan = plan->mutable_dbservers();
        tasksCurr = current->mutable_dbservers();
        break;
      case TaskType::SECONDARY_DBSERVER:
        tasksPlan = plan->mutable_secondaries();
        tasksCurr = current->mutable_secondaries();
        break;
      case TaskType::COORDINATOR:
        tasksPlan = plan->mutable_coordinators();
        tasksCurr = current->mutable_coordinators();
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
          timeStamp = tp->timestamp();
          if (now - timeStamp > TryingToReserveTimeout) {
            LOG(INFO) << "Timeout " << TryingToReserveTimeout << "s reached "
                      << " for task " << ic->task_info().name()
                      << " in state TASK_STATE_TRYING_TO_RESERVE.";
            LOG(INFO) << "Going back to state TASK_STATE_NEW.";
            tp->set_state(TASK_STATE_NEW);
            tp->clear_persistence_id();
            tp->set_timestamp(now);
            l.changed();
          }
          break;
        case TASK_STATE_TRYING_TO_PERSIST:
          // After a timeout, go back to state TASK_STATE_NEW, because
          // there was no satisfactory answer to our persistence request:
          timeStamp = tp->timestamp();
          if (now - timeStamp > TryingToPersistTimeout) {
            LOG(INFO) << "Timeout " << TryingToPersistTimeout << "s reached "
                      << " for task " << ic->task_info().name()
                      << " in state TASK_STATE_TRYING_TO_PERSIST.";
            LOG(INFO) << "Going back to state TASK_STATE_NEW.";
            tp->set_state(TASK_STATE_NEW);
            tp->clear_persistence_id();
            tp->set_timestamp(now);
            l.changed();
          }
          break;
        case TASK_STATE_TRYING_TO_START:
          // After a timeout, go back to state TASK_STATE_NEW, because
          // there was no satisfactory answer to our start request:
          timeStamp = tp->timestamp();
          if (now - timeStamp > TryingToStartTimeout) {
            LOG(INFO) << "Timeout " << TryingToPersistTimeout << "s reached "
                      << " for task " << ic->task_info().name()
                      << " in state TASK_STATE_TRYING_TO_START.";
            LOG(INFO) << "Going back to state TASK_STATE_NEW.";
            tp->set_state(TASK_STATE_NEW);
            tp->clear_persistence_id();
            tp->set_timestamp(now);
            l.changed();
          }
          break;
        case TASK_STATE_RUNNING:
          // Run forever here, no timeout.
          break;
        case TASK_STATE_KILLED:
          // After some time being killed, we have to take action and
          // engage in some automatic failover procedure:
          // if failover timeout reached:
          //   if coordinator:
          //     go back to TASK_STATE_NEW to start another one
          //   else if agent:
          //     ignore timeout, keep trying, otherwise we are lost
          //   else if secondary:
          //     find corresponding primary (partner)
          //     make new secondary, change primary's secondary entry in
          //     our state and in the registry, declare old secondary dead
          //   else if primary:
          //     find corresponding secondary
          //     if not running, keep trying
          //     else:
          //       interchange plan and current infos, update task2position
          //       map, promote secondary to primary in state and agency,
          //       make old primary the secondary of the old secondary,
          //       set state of old primary to TASK_STATE_FAILED_OVER
          timeStamp = tp->timestamp();
          if (now - timeStamp > FailoverTimeout) {
            LOG(INFO) << "Timeout " << FailoverTimeout << "s reached "
                      << " for task " << ic->task_info().name()
                      << " in state TASK_STATE_KILLED.";
            if (taskType == TaskType::AGENT) {
              LOG(INFO) << "Task is an agent, simply reset the timestamp and "
                        << "wait forever.";
              tp->set_timestamp(now);
              l.changed();
            }
            else if (taskType == TaskType::COORDINATOR) {
              LOG(INFO) << "Going back to state TASK_STATE_NEW.";
              tp->set_state(TASK_STATE_NEW);
              tp->clear_persistence_id();
              tp->set_timestamp(now);
              l.changed();
            }
            else if (taskType == TaskType::SECONDARY_DBSERVER) {
              std::string primaryName = tp->sync_partner();
              // ...
            }
          }
          break;
        case TASK_STATE_TRYING_TO_RESTART:
          // We got the offer for a restart, but the restart is not happening.
          // We need to go back to state TASK_STATE_KILLED to wait for another
          // offer.
          timeStamp = tp->timestamp();
          if (now - timeStamp > TryingToStartTimeout) {
            LOG(INFO) << "Timeout " << TryingToPersistTimeout << "s reached "
                      << " for task " << ic->task_info().name()
                      << " in state TASK_STATE_TRYING_TO_RESTART.";
            LOG(INFO) << "Going back to state TASK_STATE_KILL.";
            tp->set_state(TASK_STATE_KILLED);
            l.changed();
            // Do not change the time stamp here, because we want to
            // notice alternating between KILLED and TRYING_TO_RESTART!
          }
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

    _reconciliationTasks.erase(taskIdStr);

    std::pair<TaskType, int>& pos = _task2position[taskIdStr];

    switch (status.state()) {
      case mesos::TASK_STAGING:
        break;

      case mesos::TASK_RUNNING: {
        auto lease = Global::state().lease(true);
        caretaker.setTaskPlanState(lease, pos.first, pos.second,
                                   TASK_STATE_RUNNING);
        break;
      }
      case mesos::TASK_STARTING:
        // do nothing
        break;

      case mesos::TASK_FINISHED: // TERMINAL. The task finished successfully.
      case mesos::TASK_FAILED:   // TERMINAL. The task failed to finish successfully.
      case mesos::TASK_KILLED:   // TERMINAL. The task was killed by the executor.
      case mesos::TASK_LOST:     // TERMINAL. The task failed but can be rescheduled.
      case mesos::TASK_ERROR: {  // TERMINAL. The task failed but can be rescheduled.
        auto lease = Global::state().lease(true);
        caretaker.setTaskPlanState(lease, pos.first, pos.second,
                                   TASK_STATE_KILLED);
        break;
      }
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

void ArangoManager::killAllInstances (std::vector<std::string>& ids) {
  for (auto const& id : ids) {
    Global::scheduler().killInstance(id);
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
