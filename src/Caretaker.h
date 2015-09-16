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

#include <mesos/resources.hpp>

namespace arangodb {

// -----------------------------------------------------------------------------
// --SECTION--                                             enum class AspectType
// -----------------------------------------------------------------------------

  enum class AspectType {
    UNKNOWN,
    AGENT,
    COORDINATOR,
    PRIMARY_DBSERVER,
    SECONDARY_DBSERVER
  };

// -----------------------------------------------------------------------------
// --SECTION--                                              class AspectPosition
// -----------------------------------------------------------------------------

  class AspectPosition {
    public:
      AspectPosition () : _type(AspectType::UNKNOWN), _pos(0) {}
      AspectPosition (AspectType type, size_t pos) : _type(type), _pos(pos) {}

    public:
      AspectType _type;
      size_t _pos;
  };

// -----------------------------------------------------------------------------
// --SECTION--                                       enum class OfferActionState
// -----------------------------------------------------------------------------

  enum class OfferActionState {
    IGNORE,
    USABLE,
    MAKE_DYNAMIC_RESERVATION,
    MAKE_PERSISTENT_VOLUME
  };

// -----------------------------------------------------------------------------
// --SECTION--                                                 class OfferAction
// -----------------------------------------------------------------------------

  struct OfferAction {
    OfferActionState _state;
    mesos::Resources _resources;
    std::string _name;
    std::string _persistentId;
  };

// -----------------------------------------------------------------------------
// --SECTION--                                    enum class InstanceActionState
// -----------------------------------------------------------------------------

  enum class InstanceActionState {
    DONE,
    START_AGENT,
    START_COORDINATOR,
    START_PRIMARY_DBSERVER,
    START_SECONDARY_DBSERVER
  };

// -----------------------------------------------------------------------------
// --SECTION--                                              class InstanceAction
// -----------------------------------------------------------------------------

  class InstanceAction {
    public:
      InstanceActionState _state;
      TaskCurrent _info;
      AspectPosition _pos;
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
/// @brief check if we can start an instance
////////////////////////////////////////////////////////////////////////////////

      virtual InstanceAction checkInstance () = 0;

////////////////////////////////////////////////////////////////////////////////
/// @brief tries to update the plan
////////////////////////////////////////////////////////////////////////////////

      virtual void updatePlan () = 0;

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

    public:

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if we can use a resource offer
////////////////////////////////////////////////////////////////////////////////

      virtual OfferAction checkOffer (const mesos::Offer&);

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the task id, clears the task info and status
////////////////////////////////////////////////////////////////////////////////

      void setTaskId (const AspectPosition&,
                      const mesos::TaskID&);

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the task info
////////////////////////////////////////////////////////////////////////////////

      void setTaskInfo (const AspectPosition&,
                        const mesos::TaskInfo&);

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the task status
////////////////////////////////////////////////////////////////////////////////

      void setTaskStatus (const AspectPosition&,
                          const mesos::TaskStatus&);

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the instance state
////////////////////////////////////////////////////////////////////////////////

      void setInstanceState (const AspectPosition&, TaskCurrentState);

// -----------------------------------------------------------------------------
// --SECTION--                                          static protected methods
// -----------------------------------------------------------------------------

    protected:

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if a resource offer can be used
////////////////////////////////////////////////////////////////////////////////

OfferAction checkResourceOffer (std::string const& name,
                                bool persistent,
                                Target const& target,
                                TasksPlan* plan,
                                TasksCurrent* current,
                                mesos::Offer const& offer);

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if we can/should start a new instance
////////////////////////////////////////////////////////////////////////////////

      static InstanceAction checkStartInstance (AspectType,
                                                InstanceActionState,
                                                TasksPlan*,
                                                TasksCurrent*);

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
