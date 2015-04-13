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

#include <state/leveldb.hpp>

using namespace arangodb;
using namespace mesos::state;

// -----------------------------------------------------------------------------
// --SECTION--                                                 private functions
// -----------------------------------------------------------------------------

namespace {
  picojson::value parseJson (const string& value) {
    picojson::value json;
    std::string err = picojson::parse(json, value);

    if (! err.empty() || ! json.is<picojson::object>()) {
      if (! err.empty()) {
        LOG(FATAL)
        << "cannot parse the stored configuration '" << value << "': " << err;
      }
      else {
        LOG(FATAL)
        << "cannot parse the stored configuration '" << value << "'";
      }

      exit(EXIT_FAILURE);
    }

    return json;
  }



  StateAgencies stateAgencies (picojson::value json) {
    StateAgencies agencies;

    if (json.is<picojson::object>()) {
      picojson::object obj = json.get<picojson::object>();

      if (obj["initialized"].is<bool>()) {
        agencies._initialized = obj["initialized"].get<bool>();
      }

      if (obj["instances"].is<bool>()) {
        const auto& list = obj["instances"].get<picojson::array>();
        
        for (const auto& l : list) {
          agencies._instances.push_back(l.get<string>());
        }
      }
    }

    return agencies;
  }



  StateAgencies stateAgencies (const string& json) {
    return stateAgencies(parseJson(json));
  }



  picojson::value asJson (const StateAgencies& state) {
    picojson::object o;

    o["initialized"] = picojson::value(state._initialized);

    picojson::array a;

    for (const auto& l : state._instances) {
      a.push_back(picojson::value(l));
    }

    o["instances"] = picojson::value(a);

    return picojson::value(o);
  }



  string asString (const StateAgencies& state) {
    return asJson(state).serialize();
  }

    

  void saveState (State* state, Variable& variable, const string& value) {
    variable = variable.mutate(value);
    state->store(variable);
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                               class StateAgencies
// -----------------------------------------------------------------------------

void StateAgencies::dump () const {
  cout << "StateAgencies (" << this << ")\n"
       << "  _initialized: " << (_initialized ? "true" : "false") << "\n"
       << "  _instances: " << _instances.size() << "\n";

  for (const auto& i : _instances) {
    cout << "    " << i << "\n";
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                                 class ArangoState
// -----------------------------------------------------------------------------

ArangoState::ArangoState (const string& cluster)
  : _cluster(cluster),
    _storage(nullptr),
    _state(nullptr) {
}

void ArangoState::init () {
  _storage = new LevelDBStorage("./STATE");
  _state = new State(_storage);
}

void ArangoState::load () {
  string id = _cluster + ":agencies";
  Variable variable = _state->fetch(id).get();

  string value = variable.value();

  if (value.empty()) {
    _agencies = StateAgencies();
    saveState(_state, variable, asString(_agencies));
  }
  else {
    _agencies = stateAgencies(value);
  }

  _agencies.dump();
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------

// Local Variables:
// mode: outline-minor
// outline-regexp: "/// @brief\\|/// {@inheritDoc}\\|/// @page\\|// --SECTION--\\|/// @\\}"
// End:
