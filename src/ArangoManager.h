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

#include <string>

// -----------------------------------------------------------------------------
// --SECTION--                                               class ArangoManager
// -----------------------------------------------------------------------------

namespace arangodb {
  using namespace std;

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

    enum class RoleTypes {
      NONE,
      AGENCY,
      COORDINATOR,
      DB_SERVER
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


// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

  public:

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

    public:

////////////////////////////////////////////////////////////////////////////////
/// @brief checks an offers and returns a possible role
////////////////////////////////////////////////////////////////////////////////

      const RoleType checkOffer (size_t cpu, size_t mem, size_t disk);

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

      const Slaves slaves ();

// -----------------------------------------------------------------------------
// --SECTION--                                                 private variables
// -----------------------------------------------------------------------------

    private:

////////////////////////////////////////////////////////////////////////////////
/// @brief the current state
////////////////////////////////////////////////////////////////////////////////

      ArangoState* _state;
  };
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------

// Local Variables:
// mode: outline-minor
// outline-regexp: "/// @brief\\|/// {@inheritDoc}\\|/// @page\\|// --SECTION--\\|/// @\\}"
// End:
