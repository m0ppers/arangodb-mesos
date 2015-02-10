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
    ArangoManagerImpl ();

  public:
    bool checkOfferAgency (const Offer& offer);
    bool checkOfferCoordinator (const Offer& offer);
    bool checkOfferDBServer (const Offer& offer);

  public:
    unordered_map<string, Offer> _offers;

    size_t _agencyPlannedInstances;
    size_t _coordinatorPlannedInstances;
    size_t _dbserverPlannedInstances;

    ArangoManager::Resources _agencyResources;
    ArangoManager::Resources _coordinatorResources;
    ArangoManager::Resources _dbserverResources;
};

ArangoManagerImpl::ArangoManagerImpl ()
  : _agencyPlannedInstances(3),
    _coordinatorPlannedInstances(3),
    _dbserverPlannedInstances(3),
    _agencyResources(0.2, 10, 10),
    _coordinatorResources(4, 2048, 1024),
    _dbserverResources(2, 1024, 4096) {
}

bool ArangoManagerImpl::checkOfferAgency (const Offer& offer) {
  return true;
}

bool ArangoManagerImpl::checkOfferCoordinator (const Offer& offer) {
  return true;
}

bool ArangoManagerImpl::checkOfferDBServer (const Offer& offer) {
  return true;
}

// -----------------------------------------------------------------------------
// --SECTION--                                               class ArangoManager
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
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
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief returns planned number of agency instances
////////////////////////////////////////////////////////////////////////////////

size_t ArangoManager::agencyInstances () {
  return _impl->_agencyPlannedInstances;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns planned number of coordinator instances
////////////////////////////////////////////////////////////////////////////////

size_t ArangoManager::coordinatorInstances () {
  return _impl->_coordinatorPlannedInstances;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns planned number of dbserver instances
////////////////////////////////////////////////////////////////////////////////

size_t ArangoManager::dbserverInstances () {
  return _impl->_dbserverPlannedInstances;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns minimum resources for agency
////////////////////////////////////////////////////////////////////////////////

ArangoManager::Resources ArangoManager::agencyResources () {
  return _impl->_agencyResources;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns minimum resources for coordinator
////////////////////////////////////////////////////////////////////////////////

ArangoManager::Resources ArangoManager::coordinatorResources () {
  return _impl->_coordinatorResources;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns minimum resources for DBserver
////////////////////////////////////////////////////////////////////////////////

ArangoManager::Resources ArangoManager::dbserverResources () {
  return _impl->_dbserverResources;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks and adds an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::addOffer (const Offer& offer) {
  string const& id = offer.id().value();

  cout << "OFFER received: " << id << ": " << offer.resources() << "\n";

  bool dbServer = _impl->checkOfferDBServer(offer);
  bool coordinator = _impl->checkOfferCoordinator(offer);
  bool agency = _impl->checkOfferCoordinator(offer);

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
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------

// Local Variables:
// mode: outline-minor
// outline-regexp: "/// @brief\\|/// {@inheritDoc}\\|/// @page\\|// --SECTION--\\|/// @\\}"
// End:
