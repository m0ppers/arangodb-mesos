#ifndef GLOBAL_H
#define GLOBAL_H 1

#include <string>

#include <mesos/resources.hpp>

namespace arangodb {
  class Caretaker;

  class Global {
    public:
      static Caretaker& caretaker (const std::string& name);
      static std::string role ();
      static mesos::Resource::ReservationInfo principal ();
  };
}

#endif
