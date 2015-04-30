///////////////////////////////////////////////////////////////////////////////
/// @brief standalone caretaker
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

#include "CaretakerStandalone.h"

#include "ArangoState.h"
#include "Global.h"

#include "arangodb.pb.h"

using namespace arangodb;

// -----------------------------------------------------------------------------
// --SECTION--                                                   class Caretaker
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

CaretakerStandalone::CaretakerStandalone () {
  mesos::Resource* m;

  Target target = Global::state().target();

  // AGENCY
  TargetEntry* agency = target.mutable_agencies();
  agency->set_instances(0);

  // COORDINATOR
  TargetEntry* coordinator = target.mutable_coordinators();
  coordinator->set_instances(0);

  // DBSERVER
  TargetEntry* dbserver = target.mutable_dbservers();

  dbserver->set_instances(1);
  dbserver->set_number_ports(1);

  m = dbserver->add_minimal_resources();
  m->set_role("*");
  m->set_name("cpus");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(1);

  m = dbserver->add_minimal_resources();
  m->set_role("*");
  m->set_name("mem");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(1024);

  m = dbserver->add_minimal_resources();
  m->set_role("*");
  m->set_name("disk");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(1024);

  Global::state().setTarget(target);
}

// -----------------------------------------------------------------------------
// --SECTION--                                            virtual public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief check if we can start an instance
////////////////////////////////////////////////////////////////////////////////

InstanceAction CaretakerStandalone::checkInstance () {
  Target target = Global::state().target();
  Plan plan = Global::state().plan();
  Current current = Global::state().current();

  auto res = checkStartInstance(
    "dbserver",
    AspectType::PRIMARY_DBSERVER,
    InstanceActionState::START_PRIMARY_DBSERVER,
    plan.primary_dbservers(),
    current.mutable_primary_dbserver_resources(),
    current.mutable_primary_dbservers());

  Global::state().setPlan(plan);
  Global::state().setCurrent(current);

  return res;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
