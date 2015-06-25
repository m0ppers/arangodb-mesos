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

#include <boost/algorithm/string.hpp>
#include <curl/curl.h>

#include <mesos/resources.hpp>

using namespace std;
using namespace boost;
using namespace arangodb;

// -----------------------------------------------------------------------------
// --SECTION--                                                 private functions
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief checks the master version
////////////////////////////////////////////////////////////////////////////////

static size_t WriteMemoryCallback(void* contents, size_t size, size_t nmemb, void *userp) {
  size_t realsize = size * nmemb;
  string* mem = (string*) userp;
 
  mem->append((char*) contents, realsize);

  return realsize;
}

static void checkVersion (string hostname, int port) {
  CURL *curl;
  CURLcode res;
 
  curl = curl_easy_init();

  if (curl) {
    string url = "http://" + hostname + ":" + to_string(port) + "/state.json";
    string body;

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*) &body);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "libcurl-agent/1.0");

    res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
      LOG(WARNING)
      << "cannot connect to " << url;
    }
    else {
      picojson::value s;
      std::string err = picojson::parse(s, body);

      if (err.empty()) {
        if (s.is<picojson::object>()) {
          auto& o = s.get<picojson::object>();
          auto& v = o["version"];

          if (v.is<string>()) {
            string version = v.get<string>();

            if (! version.empty()) {
              vector<string> vv;
              boost::split(vv, version, boost::is_any_of("."));

              int major = 0;
              int minor = 0;

              if (vv.size() >= 2) {
                major = stoi(vv[0]);
                minor = stoi(vv[1]);

                if (major == 0 && minor < 22) {
                  err = "version '" + version + "' is not suitable";
                }
                else {
                  LOG(INFO)
                  << "version '" << version << "' is suitable";
                }
              }
              else {
                err = "version '" + version + "' is corrupt";
              }
            }
            else {
              err = "version field is empty";
            }
          }
          else {
            err = "version field is not a string";
          }
        }
        else {
          err = "state is not a json object";
        }
      }

      if (! err.empty()) {
        LOG(WARNING)
        << "malformed state object from master: " << err;
      }
    }
 
    curl_easy_cleanup(curl);
  }
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

ArangoScheduler::ArangoScheduler ()
  : _driver(nullptr) {
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

  mesos::DiscoveryInfo di;
  di.set_visibility(mesos::DiscoveryInfo::FRAMEWORK);
  di.set_name(name);
  mesos::Ports po;
  auto p = po.add_ports();
  p->set_number(info.ports(0));
  p->set_name("ArangoDB");
  p->set_protocol("tcp");
  di.mutable_ports()->CopyFrom(po);
  task.mutable_discovery()->CopyFrom(di);

  // launch the tasks
  vector<mesos::TaskInfo> tasks;
  tasks.push_back(task);

  _driver->launchTasks(offerId, tasks);

  return task;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief kills an instances
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::killInstance (const string& taskId) const {
  LOG(INFO)
  << "INSTANCE kill instance " << taskId;

  mesos::TaskID ti;
  ti.set_value(taskId);

  _driver->killTask(ti);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief posts an request to the master
////////////////////////////////////////////////////////////////////////////////

string ArangoScheduler::postRequest (const string& command,
                                     const string& body) const {
  string url = Global::masterUrl() + command;

  string result;

  CURL *curl;
  CURLcode res;
 
  curl = curl_easy_init();

  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*) &result);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "libcurl-agent/1.0");
    curl_easy_setopt(curl, CURLOPT_POST, 1);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());

    res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
      LOG(WARNING)
      << "cannot connect to " << url;
    }

    curl_easy_cleanup(curl);
  }

  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief stops the driver
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::stop () {
  _driver->stop();
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

  status = Global::state().knownTaskStatus();

  if (! status.empty()) {
    driver->reconcileTasks(status);
  }

  checkVersion(master.hostname(), master.port());

  Global::setMasterUrl("http://" + master.hostname() + ":" + to_string(master.port()) + "/");
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
  LOG(INFO) << "DEBUG Disconnected! Waiting for reconnect.";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when new resources are available
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::resourceOffers (mesos::SchedulerDriver* driver,
                                      const vector<mesos::Offer>& offers) {
  for (auto& offer : offers) {
    /*
    LOG(INFO)
    << "DEBUG offer received " << offer.id().value();
    */

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
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback for messages from executor
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::frameworkMessage (mesos::SchedulerDriver* driver,
                                        const mesos::ExecutorID& executorId,
                                        const mesos::SlaveID& slaveId,
                                        const string& data) {
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
