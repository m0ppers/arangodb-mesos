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
#include <vector>

#include <mesos/resources.hpp>
#include <mesos/scheduler.hpp>

// -----------------------------------------------------------------------------
// --SECTION--                                               class ArangoManager
// -----------------------------------------------------------------------------

namespace arangodb {
  using namespace std;

  class ArangoManagerImpl;
  class ArangoScheduler;

////////////////////////////////////////////////////////////////////////////////
/// @brief manager class
////////////////////////////////////////////////////////////////////////////////

  class ArangoManager {

// -----------------------------------------------------------------------------
// --SECTION--                                                  embedded classes
// -----------------------------------------------------------------------------

    public:

////////////////////////////////////////////////////////////////////////////////
/// @brief instance types
////////////////////////////////////////////////////////////////////////////////

      enum class InstanceType {
        AGENCY,
        COORDINATOR,
        DBSERVER
      };

      static const string& stringInstanceType (const InstanceType& type) {
        static string agency = "AGENCY";
        static string coordinator = "COORDINATOR";
        static string dbserver = "DBSERVER";

        switch (type) {
          case InstanceType::AGENCY: return agency; break;
          case InstanceType::COORDINATOR: return coordinator; break;
          case InstanceType::DBSERVER: return dbserver; break;
        }
      }

////////////////////////////////////////////////////////////////////////////////
/// @brief instance states
////////////////////////////////////////////////////////////////////////////////

      enum class InstanceState {
        STARTED,
        RUNNING,
        FINISHED,
        FAILED
      };

      static const string& stringInstanceState (const InstanceState& state) {
        static string started = "STARTED";
        static string running = "RUNNING";
        static string finished = "FINISHED";
        static string failed = "FAILED";

        switch (state) {
          case InstanceState::STARTED: return started; break;
          case InstanceState::RUNNING: return running; break;
          case InstanceState::FINISHED: return finished; break;
          case InstanceState::FAILED: return failed; break;
        }
      }

////////////////////////////////////////////////////////////////////////////////
/// @brief instance
////////////////////////////////////////////////////////////////////////////////

      struct Instance {
        public:
          uint64_t _taskId;
          InstanceState _state;
          InstanceType _type;
          mesos::Resources _resources;
          string _slaveId;
          chrono::system_clock::time_point _started;
          chrono::system_clock::time_point _lastUpdate;
      };

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

    public:

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

      ArangoManager (const string& role, ArangoScheduler*);

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

      void statusUpdate (uint64_t, InstanceState);

////////////////////////////////////////////////////////////////////////////////
/// @brief returns planned number of agency instances
////////////////////////////////////////////////////////////////////////////////

      size_t agencyInstances ();

////////////////////////////////////////////////////////////////////////////////
/// @brief returns planned number of coordinator instances
////////////////////////////////////////////////////////////////////////////////

      size_t coordinatorInstances ();

////////////////////////////////////////////////////////////////////////////////
/// @brief returns planned number of dbserver instances
////////////////////////////////////////////////////////////////////////////////

      size_t dbserverInstances ();

////////////////////////////////////////////////////////////////////////////////
/// @brief returns minimum resources for agency
////////////////////////////////////////////////////////////////////////////////

      //      BasicResources agencyResources ();

////////////////////////////////////////////////////////////////////////////////
/// @brief returns minimum resources for coordinator
////////////////////////////////////////////////////////////////////////////////

      //      BasicResources coordinatorResources ();

////////////////////////////////////////////////////////////////////////////////
/// @brief returns minimum resources for DBserver
////////////////////////////////////////////////////////////////////////////////

      //  BasicResources dbserverResources ();

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the current offers for debugging
////////////////////////////////////////////////////////////////////////////////

      vector<mesos::Offer> currentOffers ();

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the current instances for debugging
////////////////////////////////////////////////////////////////////////////////

      vector<Instance> currentInstances ();

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

// Local Variables:
// mode: outline-minor
// outline-regexp: "/// @brief\\|/// {@inheritDoc}\\|/// @page\\|// --SECTION--\\|/// @\\}"
// End:
