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
/// @brief makes a dynamic reservation
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::reserveDynamically (const Offer& offer,
                                          const Resources& resources) {
  Offer::Operation reserve;
  reserve.set_type(Offer::Operation::RESERVE);
  reserve.mutable_reserve()->mutable_resources()->CopyFrom(resources);

  _driver->acceptOffers({offer.id()}, {reserve});
}

////////////////////////////////////////////////////////////////////////////////
/// @brief declines an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::declineOffer (const OfferID& offerId) {
  _driver->declineOffer(offerId);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief starts an agency with a given offer
////////////////////////////////////////////////////////////////////////////////

uint64_t ArangoScheduler::startAgencyInstance (const Offer& offer,
                                               const Resources& resources) {
  uint64_t taskId = NEXT_TASK_ID.fetch_add(1);
  const string offerId = offer.id().value();

  cout << "AGENCY launching task " << taskId 
       << " using offer " << offerId 
       << ": " << offer.resources()
       << "\n";

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

/*
  Resources reserved = filter(
    static_cast<bool (*)(const Resource&)>(Resources::isReserved),
    offer.resources());

  cout << "################ " << reserved << "\n";

  Offer::Operation reserve;
  reserve.set_type(Offer::Operation::RESERVE);
  reserve.mutable_reserve()->mutable_resources()->CopyFrom(reserved);

  _driver->acceptOffers({offer.id()}, {reserve});
*/

  return 0;
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

  cout << "TASK '" << status.task_id().value() << "' is in state " << state << endl;

  switch (state) {
    case TASK_RUNNING:
      _manager->statusUpdate(taskId, ArangoManager::InstanceState::RUNNING);
      break;

    case TASK_STARTING:
      // do nothing
      break;

    case TASK_FINISHED: // TERMINAL. The task finished successfully.
    case TASK_FAILED:   // TERMINAL. The task failed to finish successfully.
    case TASK_KILLED:   // TERMINAL. The task was killed by the executor.
    case TASK_LOST:     // TERMINAL. The task failed but can be rescheduled.
    case TASK_ERROR:    // TERMINAL. The task failed but can be rescheduled.
      _manager->statusUpdate(taskId, ArangoManager::InstanceState::FINISHED);
      break;
  }

  cout << "Task info: " << status.task_id().value()
       << " is in state " << status.state()
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
