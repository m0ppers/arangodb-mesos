///////////////////////////////////////////////////////////////////////////////
/// @brief state of the ArangoDB framework
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

#include "ArangoState.h"

#include "Global.h"

#include "pbjson.hpp"

#include <state/leveldb.hpp>
#include <state/zookeeper.hpp>

#include <boost/regex.hpp>

using namespace arangodb;
using namespace mesos::internal::state;
using namespace std;

// -----------------------------------------------------------------------------
// --SECTION--                                                 private functions
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief fill in TaskStatus
////////////////////////////////////////////////////////////////////////////////

void fillTaskStatus (vector<mesos::TaskStatus>& result,
                     const InstancesCurrent& instances) {

  // we have to check the TaskInfo (not TaskStatus!)
  for (int i = 0;  i < instances.entries_size();  ++i) {
    const InstancesCurrentEntry& entry = instances.entries(i);

    switch (entry.state()) {
      case INSTANCE_STATE_UNUSED:
        break;

      case INSTANCE_STATE_STARTING:
      case INSTANCE_STATE_RUNNING:
      case INSTANCE_STATE_STOPPED:
        if (entry.has_task_info()) {
          auto const& info = entry.task_info();

          mesos::TaskStatus status;
          status.mutable_task_id()->CopyFrom(info.task_id());

          if (info.has_slave_id()) {
            status.mutable_slave_id()->CopyFrom(info.slave_id());
          }

          status.set_state(mesos::TaskState::TASK_RUNNING);

          result.push_back(status);
        }

        break;
    }
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                                 class ArangoState
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

ArangoState::ArangoState (const string& name, const string& zk)
  : _name(name),
    _zk(zk),
    _storage(nullptr),
    _stateStore(nullptr) {
}

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief initialize storage and state
////////////////////////////////////////////////////////////////////////////////

void ArangoState::init () {
  if (_zk.empty()) {
    string path = "./STATE_" + _name;

    LOG(INFO) << "using leveldb at " << path;

    _storage = new LevelDBStorage(path);
  }
  else {
    string userAndPass  = "(([^/@:]+):([^/@]*)@)";
    string hostAndPort  = "[A-z0-9\\.-]+(:[0-9]+)?";
    string hostAndPorts = "(" + hostAndPort + "(," + hostAndPort + ")*)";
    string zkNode       = "[^/]+(/[^/]+)*";
    string REGEX        = "zk://(" + userAndPass +"?" + hostAndPorts + ")(/" + zkNode + ")";

    boost::regex re(REGEX);
    boost::smatch m;
    bool ok = boost::regex_match(_zk, m, re);

    if (! ok) {
      LOG(ERROR) << "FATAL cannot parse zookeeper '" << _zk << "'";
      exit(EXIT_FAILURE);
    }

    _storage = new ZooKeeperStorage(m[1], Seconds(120), m[9]);
  }

  _stateStore = new mesos::internal::state::State(_storage);

  _state.mutable_target();
  _state.mutable_target()->set_mode(Global::modeLC());
  TargetEntry* te;
  te = _state.mutable_target()->mutable_agents();
  te->set_instances(Global::nrAgents());
  te->set_number_ports(1);
  te = _state.mutable_target()->mutable_coordinators();
  te->set_instances(Global::nrCoordinators());
  te->set_number_ports(1);
  te = _state.mutable_target()->mutable_dbservers();
  te->set_instances(Global::nrDBServers());
  te->set_number_ports(1);

  _state.mutable_plan();
  _state.mutable_plan()->mutable_agents();
  _state.mutable_plan()->mutable_coordinators();
  _state.mutable_plan()->mutable_dbservers();

  _state.mutable_current();
  _state.mutable_current()->mutable_agents();
  _state.mutable_current()->mutable_coordinators();
  _state.mutable_current()->mutable_primary_dbservers();
  _state.mutable_current()->mutable_secondary_dbservers();
  _state.mutable_current()->mutable_agency_resources();
  _state.mutable_current()->mutable_coordinator_resources();
  _state.mutable_current()->mutable_primary_dbserver_resources();
  _state.mutable_current()->mutable_secondary_dbserver_resources();

  _state.mutable_current()->set_cluster_complete(false);
  _state.mutable_current()->set_cluster_bootstrappeddbservers(false);
  _state.mutable_current()->set_cluster_upgradeddb(false);
  _state.mutable_current()->set_cluster_bootstrappedcoordinators(false);
  _state.mutable_current()->set_cluster_initialized(false);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief loads the state
////////////////////////////////////////////////////////////////////////////////

void ArangoState::load () {
  lock_guard<mutex> lock(_lock);

  Variable variable = _stateStore->fetch("state").get();
  string value = variable.value();

  if (! value.empty()) {
    _state.ParseFromString(value);

    if (_state.target().mode() != Global::modeLC()) {
      LOG(ERROR)
      << "FATAL stored state is for mode '"
      << _state.target().mode() << "', "
      << "requested mode is '" << Global::modeLC() << "'";

      exit(EXIT_FAILURE);
    }
  }

  string json;
  pbjson::pb2json(&_state, json);
  
  LOG(INFO) << "current state: " << json;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief removes the state from store
////////////////////////////////////////////////////////////////////////////////

void ArangoState::destroy () {
  lock_guard<mutex> lock(_lock);

  Variable variable = _stateStore->fetch("state").get();
  auto r = _stateStore->expunge(variable);
  r.await();  // Wait until state is actually expunged
}

////////////////////////////////////////////////////////////////////////////////
/// @brief framework id
////////////////////////////////////////////////////////////////////////////////

string ArangoState::frameworkId () {
  bool found;
  mesos::FrameworkID id = frameworkId(found);

  if (found) {
    return id.value();
  }

  return "";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief framework id
////////////////////////////////////////////////////////////////////////////////

mesos::FrameworkID ArangoState::frameworkId (bool& found) {
  lock_guard<mutex> lock(_lock);

  found = _state.has_framework_id();

  if (found) {
    return _state.framework_id();
  }
  else {
    return mesos::FrameworkID();
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the framework id
////////////////////////////////////////////////////////////////////////////////

void ArangoState::setFrameworkId (const mesos::FrameworkID& id) {
  lock_guard<mutex> lock(_lock);

  _state.mutable_framework_id()->CopyFrom(id);
  save();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the target
////////////////////////////////////////////////////////////////////////////////

Target ArangoState::target () {
  lock_guard<mutex> lock(_lock);

  return _state.target();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief updates the target
////////////////////////////////////////////////////////////////////////////////

void ArangoState::setTarget (const Target& target) {
  lock_guard<mutex> lock(_lock);

  _state.mutable_target()->CopyFrom(target);
  save();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief target as json string
////////////////////////////////////////////////////////////////////////////////

string ArangoState::jsonTarget () {
  lock_guard<mutex> lock(_lock);

  string result;
  pbjson::pb2json(&_state.target(), result);
  
  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the plan
////////////////////////////////////////////////////////////////////////////////

Plan ArangoState::plan () {
  lock_guard<mutex> lock(_lock);

  return _state.plan();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief updates the plan
////////////////////////////////////////////////////////////////////////////////

void ArangoState::setPlan (const Plan& plan) {
  lock_guard<mutex> lock(_lock);

  _state.mutable_plan()->CopyFrom(plan);
  save();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief plan as json string
////////////////////////////////////////////////////////////////////////////////

string ArangoState::jsonPlan () {
  lock_guard<mutex> lock(_lock);

  string result;
  pbjson::pb2json(&_state.plan(), result);
  
  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the current
////////////////////////////////////////////////////////////////////////////////

Current ArangoState::current () {
  lock_guard<mutex> lock(_lock);
  return _state.current();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief updates the current
////////////////////////////////////////////////////////////////////////////////

void ArangoState::setCurrent (const Current& current) {
  lock_guard<mutex> lock(_lock);

  _state.mutable_current()->CopyFrom(current);
  save();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief current as json string
////////////////////////////////////////////////////////////////////////////////

string ArangoState::jsonCurrent () {
  lock_guard<mutex> lock(_lock);

  string result;
  pbjson::pb2json(&_state.current(), result);
  
  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns all known TaskStatus
////////////////////////////////////////////////////////////////////////////////

vector<mesos::TaskStatus> ArangoState::knownTaskStatus () {
  lock_guard<mutex> lock(_lock);

  vector<mesos::TaskStatus> result;

  fillTaskStatus(result, _state.current().agents());
  fillTaskStatus(result, _state.current().coordinators());
  fillTaskStatus(result, _state.current().primary_dbservers());
  fillTaskStatus(result, _state.current().secondary_dbservers());

  return result;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                   private methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief saves the state
////////////////////////////////////////////////////////////////////////////////

void ArangoState::save () {
  string value;
  _state.SerializeToString(&value);

  Variable variable = _stateStore->fetch("state").get();
  variable = variable.mutate(value);
  _stateStore->store(variable);
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
