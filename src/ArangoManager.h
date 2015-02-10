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

#include <string>
#include <mutex>
#include <vector>

#include <mesos/scheduler.hpp>

// -----------------------------------------------------------------------------
// --SECTION--                                               class ArangoManager
// -----------------------------------------------------------------------------

namespace arangodb {
  using namespace std;

  class ArangoManagerImpl;

////////////////////////////////////////////////////////////////////////////////
/// @brief manager class
////////////////////////////////////////////////////////////////////////////////

  class ArangoManager {

// -----------------------------------------------------------------------------
// --SECTION--                                                  embedded classes
// -----------------------------------------------------------------------------

  public:

////////////////////////////////////////////////////////////////////////////////
/// @brief roles
////////////////////////////////////////////////////////////////////////////////

    enum class RoleType {
      NONE,
      AGENCY,
      COORDINATOR,
      DB_SERVER
    };

////////////////////////////////////////////////////////////////////////////////
/// @brief task states
////////////////////////////////////////////////////////////////////////////////

    enum class TaskState {
      RUNNING,
      FAILING,
      FAILED
    };

////////////////////////////////////////////////////////////////////////////////
/// @brief roles summary
////////////////////////////////////////////////////////////////////////////////

    class RolesSummary {
    };


////////////////////////////////////////////////////////////////////////////////
/// @brief role summary
////////////////////////////////////////////////////////////////////////////////

    class RoleSummary {
    };


////////////////////////////////////////////////////////////////////////////////
/// @brief slaves summary
////////////////////////////////////////////////////////////////////////////////

    class SlavesSummary {
    };

////////////////////////////////////////////////////////////////////////////////
/// @brief task
////////////////////////////////////////////////////////////////////////////////

    class Task {
    };

////////////////////////////////////////////////////////////////////////////////
/// @brief resource
////////////////////////////////////////////////////////////////////////////////

    struct Resources {
      Resources (double cpus, size_t mem, size_t disk)
        : _cpus(cpus), _mem(mem), _disk(disk) {
      }

      double _cpus;
      size_t _mem;      // in MByte
      size_t _disk;     // in MBbyte
    };

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

    public:

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

      ArangoManager ();

////////////////////////////////////////////////////////////////////////////////
/// @brief destructor
////////////////////////////////////////////////////////////////////////////////

      ~ArangoManager ();

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

    public:

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

      Resources agencyResources ();

////////////////////////////////////////////////////////////////////////////////
/// @brief returns minimum resources for coordinator
////////////////////////////////////////////////////////////////////////////////

      Resources coordinatorResources ();

////////////////////////////////////////////////////////////////////////////////
/// @brief returns minimum resources for DBserver
////////////////////////////////////////////////////////////////////////////////

      Resources dbserverResources ();

////////////////////////////////////////////////////////////////////////////////
/// @brief checks and adds an offer
////////////////////////////////////////////////////////////////////////////////

      void addOffer (const mesos::Offer&);

////////////////////////////////////////////////////////////////////////////////
/// @brief removes an offer
////////////////////////////////////////////////////////////////////////////////

      void removeOffer (const mesos::OfferID& offerId);

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the current offers
////////////////////////////////////////////////////////////////////////////////

      vector<mesos::Offer> currentOffers ();

////////////////////////////////////////////////////////////////////////////////
/// @brief adds a task
////////////////////////////////////////////////////////////////////////////////

      void addTask (const Task&);

////////////////////////////////////////////////////////////////////////////////
/// @brief updates a task
////////////////////////////////////////////////////////////////////////////////

      void updateTask (const string& id, TaskState);

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the current roles
////////////////////////////////////////////////////////////////////////////////

      const RolesSummary currentRoles ();

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the goals
////////////////////////////////////////////////////////////////////////////////

      const RolesSummary goalRoles ();

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the agencies
////////////////////////////////////////////////////////////////////////////////

      const RoleSummary agencies ();

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the coordinators
////////////////////////////////////////////////////////////////////////////////

      const RoleSummary coordinators ();

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the DB servers
////////////////////////////////////////////////////////////////////////////////

      const RoleSummary dbServers ();

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the slaves
////////////////////////////////////////////////////////////////////////////////

      const SlavesSummary slaves ();

// -----------------------------------------------------------------------------
// --SECTION--                                                 private variables
// -----------------------------------------------------------------------------

    private:

////////////////////////////////////////////////////////////////////////////////
/// @brief syncronization lock
////////////////////////////////////////////////////////////////////////////////

    std::mutex _lock;

////////////////////////////////////////////////////////////////////////////////
/// @brief implementation
////////////////////////////////////////////////////////////////////////////////

      ArangoManagerImpl* _impl;
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