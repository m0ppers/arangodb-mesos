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

#include "ArangoManager.h"

#include <iostream>
#include <unordered_map>

#include <mesos/resources.hpp>

#include "common/type_utils.hpp"

using namespace mesos;
using namespace arangodb;

// -----------------------------------------------------------------------------
// --SECTION--                                           class ArangoManagerImpl
// -----------------------------------------------------------------------------

class arangodb::ArangoManagerImpl {
  public:
    unordered_map<string, Offer> _offers;
};

// -----------------------------------------------------------------------------
// --SECTION--                                               class ArangoManager
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

ArangoManager::ArangoManager () {
  _impl = new ArangoManagerImpl();
};

////////////////////////////////////////////////////////////////////////////////
/// @brief destructor
////////////////////////////////////////////////////////////////////////////////

ArangoManager::~ArangoManager () {
  delete _impl;
}

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief checks and adds an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::addOffer (const Offer& offer) {
  string const& id = offer.id().value();

  cout << "OFFER received: " << id << ": " << offer.resources() << "\n";

  bool dbServer = checkOfferDBServer(offer);
  bool coordinator = checkOfferCoordinator(offer);
  bool agency = checkOfferCoordinator(offer);

  if (! dbServer && ! coordinator && ! agency) {
    cout << "OFFER ignored: " << id << ": " << offer.resources() << "\n";
    return;
  }

  cout << "OFFER stored: " << id << ": " << offer.resources() << "\n";

  {
    lock_guard<mutex> lock(_lock);

    _impl->_offers.insert({ id, offer });
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief removes an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::removeOffer (const OfferID& offerId) {
  string const& id = offerId.value();

  cout << "OFFER removed: " << id << "\n";

  {
    lock_guard<mutex> lock(_lock);

    _impl->_offers.erase(id);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the current offers
////////////////////////////////////////////////////////////////////////////////

vector<Offer> ArangoManager::currentOffers () {
  vector<Offer> result;

  {
    lock_guard<mutex> lock(_lock);

    for (auto offer : _impl->_offers) {
      result.push_back(offer.second);
    }
  }

  return result;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                   private methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if an offer is usable for a DB server
////////////////////////////////////////////////////////////////////////////////

bool ArangoManager::checkOfferDBServer (const Offer& offer) {
  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if an offer is usable for a coordinator
////////////////////////////////////////////////////////////////////////////////

bool ArangoManager::checkOfferCoordinator (const Offer& offer) {
  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if an offer is usable for an agency
////////////////////////////////////////////////////////////////////////////////

bool ArangoManager::checkOfferAgency (const Offer& offer) {
  return true;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------

// Local Variables:
// mode: outline-minor
// outline-regexp: "/// @brief\\|/// {@inheritDoc}\\|/// @page\\|// --SECTION--\\|/// @\\}"
// End:
