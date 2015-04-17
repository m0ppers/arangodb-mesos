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

#include "Caretaker.h"

using namespace arangodb;
using namespace std;

static Caretaker CARETAKER;


// -----------------------------------------------------------------------------
// --SECTION--                                             static public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief caretaker
////////////////////////////////////////////////////////////////////////////////

Caretaker& Global::caretaker (const string& name) {
  return CARETAKER;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief role
////////////////////////////////////////////////////////////////////////////////

string Global::role () {
  return "arangodb";
}


////////////////////////////////////////////////////////////////////////////////
/// @brief principal
////////////////////////////////////////////////////////////////////////////////

#if MESOS_PRINCIPAL

mesos::Resource::ReservationInfo Global::principal () {
  mesos::Resource::ReservationInfo reservation;
  reservation.set_principal("arangodb");

  return reservation;
}

#endif

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
