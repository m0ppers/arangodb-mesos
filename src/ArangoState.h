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

#ifndef ARANGO_STATE_H
#define ARANGO_STATE_H 1

#include <state/protobuf.hpp>

namespace arangodb {
  using namespace std;

// -----------------------------------------------------------------------------
// --SECTION--                                                     StateAgencies
// -----------------------------------------------------------------------------

  class StateAgencies {
    public:
      bool _initialized;
      vector<string> _instances;

    public:
      void dump () const;
  };

// -----------------------------------------------------------------------------
// --SECTION--                                                       ArangoState
// -----------------------------------------------------------------------------

  class ArangoState {
    public:
      ArangoState (const string& cluster);

    public:
      void init ();
      void load ();
      void save ();

      bool isInitializedAgency ();
      bool isKnownAgencySlave (const string& slave);
      bool addAgencySlave (const string& slave);

    public:
      StateAgencies _agencies;

    private:
      const string _cluster;
      mesos::state::Storage* _storage;
      mesos::state::State* _state;
  };
}

#endif

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------

// Local Variables:
// mode: outline-minor
// outline-regexp: "/// @brief\\|/// {@inheritDoc}\\|/// @page\\|// --SECTION--\\|/// @\\}"
// End:
