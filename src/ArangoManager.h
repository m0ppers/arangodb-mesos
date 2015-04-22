////////////////////////////////////////////////////////////////////////////////
/// @brief manager for the ArangoDB framework
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

#ifndef ARANGO_MANAGER_H
#define ARANGO_MANAGER_H 1

#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <mesos/resources.hpp>
#include <mesos/scheduler.hpp>

namespace arangodb {
  using namespace std;

  class ArangoManagerImpl;
  class ArangoState;
  class OfferAnalysis;
  class ResourcesCurrentEntry;

// -----------------------------------------------------------------------------
// --SECTION--                                                     class Aspects
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief Aspects
////////////////////////////////////////////////////////////////////////////////

  class Aspects {
    public:
      Aspects (const string& name);

    public:
      virtual string arguments (const ResourcesCurrentEntry&, const string& taskId) const = 0;

    public:
      const string _name;
  };

// -----------------------------------------------------------------------------
// --SECTION--                                               class ArangoManager
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief manager class
////////////////////////////////////////////////////////////////////////////////

  class ArangoManager {

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

    public:

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

      ArangoManager (const string& role, const string& principal);

////////////////////////////////////////////////////////////////////////////////
/// @brief destructor
////////////////////////////////////////////////////////////////////////////////

      ~ArangoManager ();

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

    public:

////////////////////////////////////////////////////////////////////////////////
/// @brief checks and adds an offer
////////////////////////////////////////////////////////////////////////////////

      void addOffer (const mesos::Offer&);

////////////////////////////////////////////////////////////////////////////////
/// @brief removes an offer
////////////////////////////////////////////////////////////////////////////////

      void removeOffer (const mesos::OfferID& offerId);

////////////////////////////////////////////////////////////////////////////////
/// @brief status update
////////////////////////////////////////////////////////////////////////////////

      void statusUpdate (const string&);

////////////////////////////////////////////////////////////////////////////////
/// @brief slave update
////////////////////////////////////////////////////////////////////////////////

      void slaveInfoUpdate (const mesos::SlaveInfo&);

////////////////////////////////////////////////////////////////////////////////
/// @brief returns information for one cluster
////////////////////////////////////////////////////////////////////////////////

      //      ClusterInfo cluster (const string& name) const;

////////////////////////////////////////////////////////////////////////////////
/// @brief adjusts the total number of servers
////////////////////////////////////////////////////////////////////////////////

      //      ClusterInfo adjustServers (const string& name, int);

////////////////////////////////////////////////////////////////////////////////
/// @brief adjusts the total number of agencies
////////////////////////////////////////////////////////////////////////////////

      //      ClusterInfo adjustAgencies (const string& name, int);

////////////////////////////////////////////////////////////////////////////////
/// @brief adjusts the total number of coordinators
////////////////////////////////////////////////////////////////////////////////

      //ClusterInfo adjustCoordinators (const string& name, int);

////////////////////////////////////////////////////////////////////////////////
/// @brief adjusts the total number of dbservers
////////////////////////////////////////////////////////////////////////////////

      //ClusterInfo adjustDbservers (const string& name, int);

// -----------------------------------------------------------------------------
// --SECTION--                                                 private variables
// -----------------------------------------------------------------------------

    private:

////////////////////////////////////////////////////////////////////////////////
/// @brief implementation
////////////////////////////////////////////////////////////////////////////////

      ArangoManagerImpl* _impl;

////////////////////////////////////////////////////////////////////////////////
/// @brief dispatcher thread
////////////////////////////////////////////////////////////////////////////////

      thread* _dispatcher;
  };
}

#endif

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
