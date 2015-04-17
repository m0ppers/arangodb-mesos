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
// --SECTION--                                       enum class OfferActionState
// -----------------------------------------------------------------------------

  enum class OfferActionState {
    IGNORE,
    USABLE,
    STORE_FOR_LATER,
    MAKE_DYNAMIC_RESERVATION,
    MAKE_PERSISTENT_VOLUME
  };

// -----------------------------------------------------------------------------
// --SECTION--                                                 class OfferAction
// -----------------------------------------------------------------------------

  class OfferAction {
    public:
      OfferActionState _state;
      mesos::Resources _resources;
      std::string _name;
  };

// -----------------------------------------------------------------------------
// --SECTION--                                    enum class InstanceActionState
// -----------------------------------------------------------------------------

  enum class InstanceActionState {
    DONE,
    START,
  };

// -----------------------------------------------------------------------------
// --SECTION--                                              class InstanceAction
// -----------------------------------------------------------------------------

  class InstanceAction {
    public:
      InstanceActionState _state;
      ResourcesCurrentEntry _info;
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

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

    public:

////////////////////////////////////////////////////////////////////////////////
/// @brief tries to update the plan
////////////////////////////////////////////////////////////////////////////////

      void updatePlan ();

////////////////////////////////////////////////////////////////////////////////
/// @brief check if we can use a resource offer
////////////////////////////////////////////////////////////////////////////////

      OfferAction checkOffer (const mesos::Offer&);

////////////////////////////////////////////////////////////////////////////////
/// @brief check if we can start an instance
////////////////////////////////////////////////////////////////////////////////

      InstanceAction checkInstance ();

////////////////////////////////////////////////////////////////////////////////
/// @brief target as json string
////////////////////////////////////////////////////////////////////////////////

      std::string jsonTarget () const;

////////////////////////////////////////////////////////////////////////////////
/// @brief plan as json string
////////////////////////////////////////////////////////////////////////////////

      std::string jsonPlan () const;

////////////////////////////////////////////////////////////////////////////////
/// @brief current as json string
////////////////////////////////////////////////////////////////////////////////

      std::string jsonCurrent () const;

// -----------------------------------------------------------------------------
// --SECTION--                                                 private variables
// -----------------------------------------------------------------------------

    private:

////////////////////////////////////////////////////////////////////////////////
/// @brief target
////////////////////////////////////////////////////////////////////////////////

      Target _target;

////////////////////////////////////////////////////////////////////////////////
/// @brief plan
////////////////////////////////////////////////////////////////////////////////

      Plan _plan;

////////////////////////////////////////////////////////////////////////////////
/// @brief current
////////////////////////////////////////////////////////////////////////////////

      Current _current;
  };

}

#endif

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
