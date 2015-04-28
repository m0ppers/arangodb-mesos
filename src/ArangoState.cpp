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

#include "pbjson.hpp"

#include <state/leveldb.hpp>
#include <state/zookeeper.hpp>

#include <boost/regex.hpp>

using namespace arangodb;
using namespace mesos::internal::state;
using namespace std;

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

    LOG(INFO) << "using level at " << path;

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
      LOG(ERROR) << "cannot parse zookeeper '" << _zk << "'";
      exit(EXIT_FAILURE);
    }

    _storage = new ZooKeeperStorage(m[1], Seconds(120), m[9]);
  }

  _stateStore = new mesos::internal::state::State(_storage);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief loads the state
////////////////////////////////////////////////////////////////////////////////

void ArangoState::load () {
  Variable variable = _stateStore->fetch("state").get();
  string value = variable.value();

  if (! value.empty()) {
    _state.ParseFromString(value);
  }

  string json;
  pbjson::pb2json(&_state, json);
  
  LOG(INFO) << "current state: " << json;
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

// Local Variables:
// mode: outline-minor
// outline-regexp: "/// @brief\\|/// {@inheritDoc}\\|/// @page\\|// --SECTION--\\|/// @\\}"
// End:
