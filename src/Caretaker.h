#ifndef CARETAKER_H
#define CARETAKER_H 1

#include "arangodb.pb.h"

#include <mesos/resources.hpp>

namespace arangodb {

  enum class OfferState {
    IGNORE,
    STORE_FOR_LATER,
    MAKE_DYNAMIC_RESERVATION,
    MAKE_PERSISTENT_VOLUME,
    START_INSTANCE
  };

  class OfferAction {
    public:
      OfferState _state;
      mesos::Resources _resources;
      std::string _name;
  };

  class Caretaker {
    public:
      Caretaker ();

    public:
      void updatePlan ();
      OfferAction checkOffer (const mesos::Offer&);

      std::string jsonTarget () const;
      std::string jsonPlan () const;
      std::string jsonCurrent () const;

    private:
      Target _target;
      Plan _plan;
      Current _current;
  };

}

#endif
