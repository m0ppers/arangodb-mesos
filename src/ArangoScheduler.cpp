////////////////////////////////////////////////////////////////////////////////
/// @brief scheduler for the ArangoDB framework
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

#include "ArangoScheduler.h"

#include <boost/lexical_cast.hpp>

#include <atomic>
#include <iostream>
#include <string>

#include <mesos/resources.hpp>

#include "common/type_utils.hpp"

#include "ArangoManager.h"

using namespace std;
using namespace boost;
using namespace mesos;
using namespace arangodb;

// -----------------------------------------------------------------------------
// --SECTION--                                                   local variables
// -----------------------------------------------------------------------------

namespace {
  atomic<uint64_t> NEXT_TASK_ID(1);
}

// -----------------------------------------------------------------------------
// --SECTION--                                             class ArangoScheduler
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

ArangoScheduler::ArangoScheduler (const string& role,
                                  const ExecutorInfo& executor)
  : _role(role),
    _driver(nullptr),
    _executor(executor),
    _manager(nullptr) {
  _manager = new ArangoManager(_role, this);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief destructor
////////////////////////////////////////////////////////////////////////////////

ArangoScheduler::~ArangoScheduler () {
  delete _manager;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the manager
////////////////////////////////////////////////////////////////////////////////

ArangoManager* ArangoScheduler::manager () {
  return _manager;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the driver
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::setDriver (SchedulerDriver* driver) {
  _driver = driver;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief starts an agency with a given offer
////////////////////////////////////////////////////////////////////////////////

uint64_t ArangoScheduler::startAgencyInstance (const Offer& offer,
                                          const Resources& resources) {
  uint64_t taskId = NEXT_TASK_ID.fetch_add(1);
  const string offerId = offer.id().value();

  cout << "AGENCY launching task " << taskId << " using offer " << offerId << "\n";

  TaskInfo task;

  task.set_name("Agency " + lexical_cast<string>(taskId));
  task.mutable_task_id()->set_value(lexical_cast<string>(taskId));
  task.mutable_slave_id()->MergeFrom(offer.slave_id());
  task.mutable_executor()->MergeFrom(_executor);
  task.mutable_resources()->MergeFrom(resources);

  vector<TaskInfo> tasks;
  tasks.push_back(task);

  _driver->launchTasks(offer.id(), tasks);

  return taskId;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                 Scheduler methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when scheduler has been register
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::registered (SchedulerDriver*,
                                  const FrameworkID&,
                                  const MasterInfo&) {
  // TODO(fc) what to do?
  cout << "Registered!" << endl;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when scheduler has been re-register
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::reregistered (SchedulerDriver*,
                                    const MasterInfo& masterInfo) {
  // TODO(fc) what to do?
  cout << "Re-Registered!" << endl;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when scheduler has been disconnected
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::disconnected (SchedulerDriver* driver) {
  // TODO(fc) what to do?
  cout << "Disconnected!" << endl;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when new resources are available
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::resourceOffers (SchedulerDriver* driver,
                                      const vector<Offer>& offers) {
  static int tasksLaunched = 1;

  cout << "Resource Offers!" << endl;

  for (auto& offer : offers) {
    _manager->addOffer(offer);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when new resources becomes unavailable
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::offerRescinded (SchedulerDriver* driver,
                                      const OfferID& offerId) {
  _manager->removeOffer(offerId);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when task changes
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::statusUpdate (SchedulerDriver* driver,
                                    const TaskStatus& status) {
  uint64_t taskId = lexical_cast<uint64_t>(status.task_id().value());
  auto state = status.state();

  cout << "TASK '" << status.task_id() << "' is in state " << state << endl;

  if (state == TASK_RUNNING) {
    _manager->statusUpdate(taskId, ArangoManager::InstanceState::RUNNING);
  }
  else if (state == TASK_STARTING) {
    // do nothing
  }
  else if (state == TASK_FINISHED) {
    _manager->statusUpdate(taskId, ArangoManager::InstanceState::FINISHED);
  }

  // TODO(fc) handle other cases

  cout << "Task info: " << status.task_id()
       << " is in unexpected state " << status.state()
       << " with reason " << status.reason()
       << " from source " << status.source()
       << " with message '" << status.message() << "'"
       << endl;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback for messages from executor
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::frameworkMessage (SchedulerDriver* driver,
                                        const ExecutorID& executorId,
                                        const SlaveID& slaveId,
                                        const string& data) {
  // TODO(fc) what to do?
  cout << "Framework Message!" << endl;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback for slave is down
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::slaveLost (SchedulerDriver* driver,
                                 const SlaveID& sid) {
  // TODO(fc) what to do?
  cout << "Slave Lost!" << endl;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback for executor goes down
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::executorLost (SchedulerDriver* driver,
                                    const ExecutorID& executorID,
                                    const SlaveID& slaveID,
                                    int status) {
  // TODO(fc) what to do?
  cout << "Executor Lost!" << endl;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief error handling
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::error (SchedulerDriver* driver,
                             const string& message) {
  cerr << "ERROR " << message << endl;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------

// Local Variables:
// mode: outline-minor
// outline-regexp: "/// @brief\\|/// {@inheritDoc}\\|/// @page\\|// --SECTION--\\|/// @\\}"
// End:
