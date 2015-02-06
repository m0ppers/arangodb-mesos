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

#include <iostream>
#include <string>

#include <mesos/resources.hpp>
#include <mesos/scheduler.hpp>

#include "common/type_utils.hpp"

using namespace std;
using namespace boost;
using namespace mesos;
using namespace arangodb;

// -----------------------------------------------------------------------------
// --SECTION--                                             class ArangoScheduler
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

ArangoScheduler::ArangoScheduler (const ExecutorInfo& executor,
                                  const string& role)
  : _executor(executor),
    _role(role) {
}

////////////////////////////////////////////////////////////////////////////////
/// @brief destructor
////////////////////////////////////////////////////////////////////////////////

ArangoScheduler::~ArangoScheduler () {
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
  cout << "Registered!" << endl;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when scheduler has been re-register
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::reregistered (SchedulerDriver*,
                                    const MasterInfo& masterInfo) {
  cout << "Re-Registered!" << endl;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when scheduler has been disconnected
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::disconnected (SchedulerDriver* driver) {
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
    cout << "Received offer " << offer.id() << " with " << offer.resources() << "\n";

    static const Resources TASK_RESOURCES = Resources::parse(
      "cpus:" + stringify(1) +
      ";mem:" + stringify(1)).get();

    Resources remaining = offer.resources();

    // Launch tasks.
    vector<TaskInfo> tasks;

    while (remaining.flatten().contains(TASK_RESOURCES)) {
      int taskId = tasksLaunched++;

      cout << "Launching task " << taskId << " using offer "
           << offer.id() << endl;

      TaskInfo task;
      task.set_name("Task " + lexical_cast<string>(taskId));
      task.mutable_task_id()->set_value(lexical_cast<string>(taskId));
      task.mutable_slave_id()->MergeFrom(offer.slave_id());
      task.mutable_executor()->MergeFrom(_executor);

      Option<Resources> resources =
      remaining.find(TASK_RESOURCES.flatten(_role));

      CHECK_SOME(resources);
      task.mutable_resources()->MergeFrom(resources.get());
      remaining -= resources.get();

      tasks.push_back(task);

      break;
    }

    driver->launchTasks(offer.id(), tasks);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when new resources become unavailable
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::offerRescinded (SchedulerDriver* driver,
                                      const OfferID& offerId) {
  cout << "Offer Rescinded! " << offerId << "\n";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when task changes
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::statusUpdate (SchedulerDriver* driver,
                                    const TaskStatus& status) {
  cout << "Status Update!" << endl;

  // int taskId = lexical_cast<int>(status.task_id().value());
  cout << "Task '" << status.task_id() << "' is in state " << status.state() << endl;

  if (status.state() == TASK_LOST ||
      status.state() == TASK_KILLED ||
      status.state() == TASK_FAILED) {
    cout << "Task diead: " << status.task_id()
         << " is in unexpected state " << status.state()
         << " with reason " << status.reason()
         << " from source " << status.source()
         << " with message '" << status.message() << "'"
         << endl;
    //driver->abort();
  }

#if 0
    if (tasksFinished == totalTasks)
      driver->stop();
#endif
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback for messages from executor
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::frameworkMessage (SchedulerDriver* driver,
                                        const ExecutorID& executorId,
                                        const SlaveID& slaveId,
                                        const string& data) {
  cout << "Framework Message!" << endl;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback for slave is down
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::slaveLost (SchedulerDriver* driver,
                                 const SlaveID& sid) {
  cout << "Slave Lost!" << endl;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback for executor goes down
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::executorLost (SchedulerDriver* driver,
                                    const ExecutorID& executorID,
                                    const SlaveID& slaveID,
                                    int status) {
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
