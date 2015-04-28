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

ArangoScheduler::ArangoScheduler (const string& role,
                                  const string& principal,
                                  const ExecutorInfo& executor)
  : _role(role),
    _principal(principal),
    _driver(nullptr),
    _executor(executor) {
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

void ArangoScheduler::setDriver (SchedulerDriver* driver) {
  _driver = driver;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief makes a dynamic reservation
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::reserveDynamically (const Offer& offer,
                                          const Resources& resources) const {
  Offer::Operation reserve;
  reserve.set_type(Offer::Operation::RESERVE);
  reserve.mutable_reserve()->mutable_resources()->CopyFrom(resources);

  _driver->acceptOffers({offer.id()}, {reserve});
}

////////////////////////////////////////////////////////////////////////////////
/// @brief creates a persistent disk
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::makePersistent (const Offer& offer,
                                      const Resources& resources) const {
  Offer::Operation reserve;
  reserve.set_type(Offer::Operation::CREATE);
  reserve.mutable_create()->mutable_volumes()->CopyFrom(resources);

  _driver->acceptOffers({offer.id()}, {reserve});
}

////////////////////////////////////////////////////////////////////////////////
/// @brief declines an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::declineOffer (const OfferID& offerId) const {
  LOG(INFO)
  << "DEBUG declining offer " << offerId.value();

  _driver->declineOffer(offerId);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief starts an instances with a given offer and resources
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::startInstance (const string& taskId,
                                     const string& name,
                                     const mesos::SlaveID& slaveId,
                                     const mesos::OfferID& offerId,
                                     const mesos::Resources& resources,
                                     const mesos::ContainerInfo::DockerInfo& docker,
                                     const string& startCommand) const {
  const string& offerStr = offerId.value();

  LOG(INFO)
  << "DEBUG startInstance: "
  << "launching task " << name 
  << " using offer " << offerStr
  << " and resources " << resources;

  TaskInfo task;

  task.set_name(name);
  task.mutable_task_id()->set_value(taskId);
  task.mutable_slave_id()->CopyFrom(slaveId);
  task.mutable_resources()->CopyFrom(resources);

  // command to execute
  CommandInfo* command = task.mutable_command();
  command->set_value(startCommand);
  command->set_shell(false);

  // use docker to run the task
  mesos::ContainerInfo* container = task.mutable_container();
  container->set_type(ContainerInfo::DOCKER);

  // copy the docker info
  container->mutable_docker()->CopyFrom(docker);

  // volume
  mesos::Volume* volume = container->add_volumes();
  volume->set_container_path("/data");
  volume->set_host_path("/tmp/XYZPATH");
  volume->set_mode(Volume::RW);

  // launch the tasks
  vector<TaskInfo> tasks;
  tasks.push_back(task);

  _driver->launchTasks(offerId, tasks);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief kills an instances
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::killInstance (const string& name,
                                    const string& taskId) const {
  LOG(INFO)
  << "INSTANCE kill instance " << taskId;

  TaskID ti;
  ti.set_value(taskId);

  _driver->killTask(ti);
}

// -----------------------------------------------------------------------------
// --SECTION--                                                 Scheduler methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when scheduler has been register
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::registered (SchedulerDriver*,
                                  const FrameworkID& frameworkId,
                                  const MasterInfo&) {
  LOG(INFO) << "registered with framework id " << frameworkId.value();

  Global::state().setFrameworkId(frameworkId);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when scheduler has been re-register
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::reregistered (SchedulerDriver*,
                                    const MasterInfo& masterInfo) {
  // TODO(fc) what to do?
  LOG(INFO) << "DEBUG Re-Registered!";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when scheduler has been disconnected
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::disconnected (SchedulerDriver* driver) {
  // TODO(fc) what to do?
  LOG(INFO) << "DEBUG Disconnected!";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when new resources are available
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::resourceOffers (SchedulerDriver* driver,
                                      const vector<Offer>& offers) {
  for (auto& offer : offers) {
    LOG(INFO)
    << "DEBUG offer received " << offer.id().value();

    Global::manager().addOffer(offer);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when new resources becomes unavailable
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::offerRescinded (SchedulerDriver* driver,
                                      const OfferID& offerId) {
  LOG(INFO)
  << "DEBUG offer rescinded " << offerId.value();

  Global::manager().removeOffer(offerId);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when task changes
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::statusUpdate (SchedulerDriver* driver,
                                    const TaskStatus& status) {
  const string& taskId = status.task_id().value();
  auto state = status.state();

  LOG(INFO)
  << "TASK '" << taskId
  << "' is in state " << state
  << " with reason " << status.reason()
  << " from source " << status.source()
  << " with message '" << status.message() << "'";

  switch (state) {
    case TASK_STAGING:
      break;

    case TASK_RUNNING:
      // XXXXXX _manager->statusUpdate(taskId, InstanceState::RUNNING);
      break;

    case TASK_STARTING:
      // do nothing
      break;

    case TASK_FINISHED: // TERMINAL. The task finished successfully.
    case TASK_FAILED:   // TERMINAL. The task failed to finish successfully.
    case TASK_KILLED:   // TERMINAL. The task was killed by the executor.
    case TASK_LOST:     // TERMINAL. The task failed but can be rescheduled.
    case TASK_ERROR:    // TERMINAL. The task failed but can be rescheduled.
      // XXXXXX _manager->statusUpdate(taskId, InstanceState::FINISHED);
      break;
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback for messages from executor
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::frameworkMessage (SchedulerDriver* driver,
                                        const ExecutorID& executorId,
                                        const SlaveID& slaveId,
                                        const string& data) {
  mesos::SlaveInfo slaveInfo;
  slaveInfo.ParseFromString(data);

  // XXXXXX _manager->slaveInfoUpdate(slaveInfo);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback for slave is down
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::slaveLost (SchedulerDriver* driver,
                                 const SlaveID& sid) {
  // TODO(fc) what to do?
  LOG(INFO) << "Slave Lost!";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback for executor goes down
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::executorLost (SchedulerDriver* driver,
                                    const ExecutorID& executorID,
                                    const SlaveID& slaveID,
                                    int status) {
  // TODO(fc) what to do?
  LOG(INFO) << "Executor Lost!";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief error handling
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::error (SchedulerDriver* driver,
                             const string& message) {
  // TODO(fc) what to do?
  LOG(ERROR) << "ERROR " << message;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------

// Local Variables:
// mode: outline-minor
// outline-regexp: "/// @brief\\|/// {@inheritDoc}\\|/// @page\\|// --SECTION--\\|/// @\\}"
// End:
