#include "Global.h"

#include "Caretaker.h"

using namespace arangodb;
using namespace std;

static Caretaker CARETAKER;


Caretaker& Global::caretaker (const string& name) {
  return CARETAKER;
}


string Global::role () {
  return "arangodb";
}


mesos::Resource::ReservationInfo Global::principal () {
  mesos::Resource::ReservationInfo reservation;
  reservation.set_principal("arangodb");

  return reservation;
}
