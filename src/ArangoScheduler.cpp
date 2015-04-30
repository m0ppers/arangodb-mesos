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

#include "ArangoManager.h"
#include "ArangoState.h"
#include "Global.h"
#include "utils.h"

#include <atomic>
#include <iostream>
#include <string>

#include <boost/lexical_cast.hpp>

#include <mesos/resources.hpp>

using namespace std;
using namespace boost;
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

ArangoScheduler::ArangoScheduler (const string& role,
                                  const string& principal)
  : _role(role),
    _principal(principal),
    _driver(nullptr) {
}

////////////////////////////////////////////////////////////////////////////////
/// @brief destructor
////////////////////////////////////////////////////////////////////////////////

ArangoScheduler::~ArangoScheduler () {
}

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the driver
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::setDriver (mesos::SchedulerDriver* driver) {
  _driver = driver;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief makes a dynamic reservation
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::reserveDynamically (const mesos::Offer& offer,
                                          const mesos::Resources& resources) const {
  mesos::Offer::Operation reserve;
  reserve.set_type(mesos::Offer::Operation::RESERVE);
  reserve.mutable_reserve()->mutable_resources()->CopyFrom(resources);

  _driver->acceptOffers({offer.id()}, {reserve});
}

////////////////////////////////////////////////////////////////////////////////
/// @brief creates a persistent disk
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::makePersistent (const mesos::Offer& offer,
                                      const mesos::Resources& resources) const {
  mesos::Offer::Operation reserve;
  reserve.set_type(mesos::Offer::Operation::CREATE);
  reserve.mutable_create()->mutable_volumes()->CopyFrom(resources);

  _driver->acceptOffers({offer.id()}, {reserve});
}

////////////////////////////////////////////////////////////////////////////////
/// @brief declines an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::declineOffer (const mesos::OfferID& offerId) const {
  LOG(INFO)
  << "DEBUG declining offer " << offerId.value();

  _driver->declineOffer(offerId);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief starts an instances with a given offer and resources
////////////////////////////////////////////////////////////////////////////////

mesos::TaskInfo ArangoScheduler::startInstance (
    const string& taskId,
    const string& name,
    const ResourcesCurrentEntry& info,
    const mesos::ContainerInfo& container,
    const mesos::CommandInfo& command) const {
  const mesos::SlaveID& slaveId = info.slave_id();
  const mesos::OfferID& offerId = info.offer_id();
  const mesos::Resources& resources = info.resources();
  const string& offerStr = offerId.value();

  LOG(INFO)
  << "DEBUG startInstance: "
  << "launching task " << name 
  << " using offer " << offerStr
  << " and resources " << resources;

  mesos::TaskInfo task;

  task.set_name(name);
  task.mutable_task_id()->set_value(taskId);
  task.mutable_slave_id()->CopyFrom(slaveId);
  task.mutable_resources()->CopyFrom(resources);
  task.mutable_container()->CopyFrom(container);
  task.mutable_command()->CopyFrom(command);

  // launch the tasks
  vector<mesos::TaskInfo> tasks;
  tasks.push_back(task);

  _driver->launchTasks(offerId, tasks);

  return task;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief kills an instances
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::killInstance (const string& name,
                                    const string& taskId) const {
  LOG(INFO)
  << "INSTANCE kill instance " << taskId;

  mesos::TaskID ti;
  ti.set_value(taskId);

  _driver->killTask(ti);
}

// -----------------------------------------------------------------------------
// --SECTION--                                                 Scheduler methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when scheduler has been register
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::registered (mesos::SchedulerDriver* driver,
                                  const mesos::FrameworkID& frameworkId,
                                  const mesos::MasterInfo& master) {
  LOG(INFO)
  << "registered with framework-id " << frameworkId.value()
  << " at master " << master.id();

  Global::state().setFrameworkId(frameworkId);

  vector<mesos::TaskStatus> status;
  driver->reconcileTasks(status);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when scheduler has been re-register
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::reregistered (mesos::SchedulerDriver* driver,
                                    const mesos::MasterInfo& master) {
  LOG(INFO)
  << "re-registered at new master: " << master.id();

  vector<mesos::TaskStatus> status;
  driver->reconcileTasks(status);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when scheduler has been disconnected
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::disconnected (mesos::SchedulerDriver* driver) {
  // TODO(fc) what to do?
  LOG(INFO) << "DEBUG Disconnected!";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when new resources are available
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::resourceOffers (mesos::SchedulerDriver* driver,
                                      const vector<mesos::Offer>& offers) {
  for (auto& offer : offers) {
    LOG(INFO)
    << "DEBUG offer received " << offer.id().value();

    Global::manager().addOffer(offer);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when new resources becomes unavailable
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::offerRescinded (mesos::SchedulerDriver* driver,
                                      const mesos::OfferID& offerId) {
  LOG(INFO)
  << "DEBUG offer rescinded " << offerId.value();

  Global::manager().removeOffer(offerId);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when task changes
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::statusUpdate (mesos::SchedulerDriver* driver,
                                    const mesos::TaskStatus& status) {
  const string& taskId = status.task_id().value();
  auto state = status.state();
  auto& manager = Global::manager();

  LOG(INFO)
  << "TASK '" << taskId
  << "' is in state " << state
  << " with reason " << status.reason()
  << " from source " << status.source()
  << " with message '" << status.message() << "'";

  manager.taskStatusUpdate(status);

  switch (state) {
    case mesos::TASK_STAGING:
      break;

    case mesos::TASK_RUNNING:
      // manager->statusUpdate(taskId, TASK_STATE_RUNNING);
      break;

    case mesos::TASK_STARTING:
      // do nothing
      break;

    case mesos::TASK_FINISHED: // TERMINAL. The task finished successfully.
    case mesos::TASK_FAILED:   // TERMINAL. The task failed to finish successfully.
    case mesos::TASK_KILLED:   // TERMINAL. The task was killed by the executor.
    case mesos::TASK_LOST:     // TERMINAL. The task failed but can be rescheduled.
    case mesos::TASK_ERROR:    // TERMINAL. The task failed but can be rescheduled.
      // manager->statusUpdate(taskId, InstanceState::FINISHED);
      break;
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback for messages from executor
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::frameworkMessage (mesos::SchedulerDriver* driver,
                                        const mesos::ExecutorID& executorId,
                                        const mesos::SlaveID& slaveId,
                                        const string& data) {
  mesos::SlaveInfo slaveInfo;
  slaveInfo.ParseFromString(data);

  // XXXXXX _manager->slaveInfoUpdate(slaveInfo);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback for slave is down
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::slaveLost (mesos::SchedulerDriver* driver,
                                 const mesos::SlaveID& sid) {
  // TODO(fc) what to do?
  LOG(INFO) << "Slave Lost!";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback for executor goes down
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::executorLost (mesos::SchedulerDriver* driver,
                                    const mesos::ExecutorID& executorID,
                                    const mesos::SlaveID& slaveID,
                                    int status) {
  // TODO(fc) what to do?
  LOG(INFO) << "Executor Lost!";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief error handling
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::error (mesos::SchedulerDriver* driver,
                             const string& message) {
  LOG(ERROR) << "ERROR " << message;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
