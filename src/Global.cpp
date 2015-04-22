///////////////////////////////////////////////////////////////////////////////
/// @brief global defines and configuration objects
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

#include "Global.h"

#include "CaretakerStandalone.h"

using namespace arangodb;
using namespace std;

// -----------------------------------------------------------------------------
// --SECTION--                                                 private variables
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief caretaker
////////////////////////////////////////////////////////////////////////////////

static Caretaker* CARETAKER = nullptr;


////////////////////////////////////////////////////////////////////////////////
/// @brief ArangoDB manager
////////////////////////////////////////////////////////////////////////////////

static ArangoManager* MANAGER = nullptr;

////////////////////////////////////////////////////////////////////////////////
/// @brief state
////////////////////////////////////////////////////////////////////////////////

static ArangoState* STATE = nullptr;

////////////////////////////////////////////////////////////////////////////////
/// @brief scheduler
////////////////////////////////////////////////////////////////////////////////

static ArangoScheduler* SCHEDULER = nullptr;

////////////////////////////////////////////////////////////////////////////////
/// @brief mode
////////////////////////////////////////////////////////////////////////////////

static OperationMode MODE = OperationMode::STANDALONE;

////////////////////////////////////////////////////////////////////////////////
/// @brief role
////////////////////////////////////////////////////////////////////////////////

static string ROLE = "arangodb";

////////////////////////////////////////////////////////////////////////////////
/// @brief principal
////////////////////////////////////////////////////////////////////////////////

static string PRINCIPAL = "arangodb";

// -----------------------------------------------------------------------------
// --SECTION--                                             static public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief caretaker
////////////////////////////////////////////////////////////////////////////////

Caretaker& Global::caretaker () {
  return *CARETAKER;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the caretaker
////////////////////////////////////////////////////////////////////////////////

void Global::setCaretaker (Caretaker* caretaker) {
  CARETAKER = caretaker;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief manager
////////////////////////////////////////////////////////////////////////////////

ArangoManager& Global::manager () {
  return *MANAGER;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the manager
////////////////////////////////////////////////////////////////////////////////

void Global::setManager (ArangoManager* manager) {
  MANAGER = manager;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief state
////////////////////////////////////////////////////////////////////////////////

ArangoState& Global::state () {
  return *STATE;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the state
////////////////////////////////////////////////////////////////////////////////

void Global::setState (ArangoState* state) {
  STATE = state;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief scheduler
////////////////////////////////////////////////////////////////////////////////

ArangoScheduler& Global::scheduler () {
  return *SCHEDULER;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the scheduler
////////////////////////////////////////////////////////////////////////////////

void Global::setScheduler (ArangoScheduler* scheduler) {
  SCHEDULER = scheduler;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief mode
////////////////////////////////////////////////////////////////////////////////

OperationMode Global::mode () {
  return MODE;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the role
////////////////////////////////////////////////////////////////////////////////

void Global::setMode (OperationMode mode) {
  MODE = mode;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief role
////////////////////////////////////////////////////////////////////////////////

string Global::role () {
  return ROLE;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the role
////////////////////////////////////////////////////////////////////////////////

void Global::setRole (const std::string& role) {
  ROLE = role;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief principal
////////////////////////////////////////////////////////////////////////////////

#if MESOS_PRINCIPAL

mesos::Resource::ReservationInfo Global::principal () {
  mesos::Resource::ReservationInfo reservation;
  reservation.set_principal(PRINCIPAL);

  return reservation;
}

#endif

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the principal
////////////////////////////////////////////////////////////////////////////////

void Global::setPrincipal (const std::string& principal) {
  PRINCIPAL = principal;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
