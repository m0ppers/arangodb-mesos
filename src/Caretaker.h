///////////////////////////////////////////////////////////////////////////////
/// @brief caretaker for resources and instances
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

#ifndef CARETAKER_H
#define CARETAKER_H 1

#include "arangodb.pb.h"
#include "ArangoState.h"

#include <mesos/resources.hpp>

namespace arangodb {

// -----------------------------------------------------------------------------
// --SECTION--                                               enum class TaskType
// -----------------------------------------------------------------------------

  enum class TaskType {
    UNKNOWN,
    AGENT,
    COORDINATOR,
    PRIMARY_DBSERVER,
    SECONDARY_DBSERVER
  };

// -----------------------------------------------------------------------------
// --SECTION--                                                   class Caretaker
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief caretaker class
////////////////////////////////////////////////////////////////////////////////

  class Caretaker {

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

    public:

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

      Caretaker ();

////////////////////////////////////////////////////////////////////////////////
/// @brief copy constructor
////////////////////////////////////////////////////////////////////////////////

      Caretaker (const Caretaker&) = delete;

////////////////////////////////////////////////////////////////////////////////
/// @brief assignment constructor
////////////////////////////////////////////////////////////////////////////////

      Caretaker& operator= (const Caretaker&) = delete;

////////////////////////////////////////////////////////////////////////////////
/// @brief destructor
////////////////////////////////////////////////////////////////////////////////

      virtual ~Caretaker ();

// -----------------------------------------------------------------------------
// --SECTION--                                            virtual public methods
// -----------------------------------------------------------------------------

    public:

////////////////////////////////////////////////////////////////////////////////
/// @brief tries to update the plan
////////////////////////////////////////////////////////////////////////////////

      virtual void updatePlan () = 0;

////////////////////////////////////////////////////////////////////////////////
/// @brief starts a new arangodb task
////////////////////////////////////////////////////////////////////////////////

      void startArangoDBTask (TaskType taskType, int pos,
                              TaskCurrent const& info);

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

    public:

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if we can use a resource offer, 
////////////////////////////////////////////////////////////////////////////////

      virtual void checkOffer (mesos::Offer const&);

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the task id, clears the task info and status
////////////////////////////////////////////////////////////////////////////////

      void setTaskId (ArangoState::Lease&, TaskType, int, mesos::TaskID const&);

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the task info
////////////////////////////////////////////////////////////////////////////////

      void setTaskInfo (ArangoState::Lease&, TaskType, int,
                        mesos::TaskInfo const&);

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the task plan state
////////////////////////////////////////////////////////////////////////////////

      void setTaskPlanState (ArangoState::Lease&, TaskType, int,
                             TaskPlanState const);

// -----------------------------------------------------------------------------
// --SECTION--                                          static protected methods
// -----------------------------------------------------------------------------

    protected:

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if a resource offer can be used for a particular task
/// type, if doDecline is true, then the offer is immediately declined
/// if it is not useful for this task type. Returns true if the offer
/// was put to some use (or declined) and false, if somebody else can
/// have a go. Note that this method has to return true if it changed
/// the global state (or call lease.changed() explicitly).
////////////////////////////////////////////////////////////////////////////////

      bool checkOfferOneType (ArangoState::Lease& lease,
                              std::string const& name,
                              bool persistent,
                              Target const& target,
                              TasksPlan* plan,
                              TasksCurrent* current,
                              mesos::Offer const& offer,
                              bool doDecline,
                              TaskType taskType);

////////////////////////////////////////////////////////////////////////////////
/// @brief set a default minimum resource set for a Targetentry
////////////////////////////////////////////////////////////////////////////////

      static void setStandardMinimum (Target* te, int size = 1);

  };
}

#endif

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
