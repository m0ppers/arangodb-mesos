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

#ifndef ARANGO_MANAGER_H
#define ARANGO_MANAGER_H 1

#include "Caretaker.h"

#include <atomic>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include <mesos/resources.hpp>
#include <mesos/scheduler.hpp>

namespace arangodb {

// -----------------------------------------------------------------------------
// --SECTION--                                              class ReconcileTasks
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief helper class for reconciliation
////////////////////////////////////////////////////////////////////////////////

  class ReconcileTasks {
    public:
      mesos::TaskStatus _status;
      std::chrono::steady_clock::time_point _nextReconcile;
      std::chrono::steady_clock::duration _backoff;
  };

// -----------------------------------------------------------------------------
// --SECTION--                                               class ArangoManager
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief manager class
////////////////////////////////////////////////////////////////////////////////

  class ArangoManager {
    ArangoManager (const ArangoManager&) = delete;
    ArangoManager& operator= (const ArangoManager&) = delete;

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

    public:

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

      ArangoManager ();

////////////////////////////////////////////////////////////////////////////////
/// @brief destructor
////////////////////////////////////////////////////////////////////////////////

      ~ArangoManager ();

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

    public:

////////////////////////////////////////////////////////////////////////////////
/// @brief checks and adds an offer
////////////////////////////////////////////////////////////////////////////////

      void addOffer (const mesos::Offer&);

////////////////////////////////////////////////////////////////////////////////
/// @brief removes an offer
////////////////////////////////////////////////////////////////////////////////

      void removeOffer (const mesos::OfferID& offerId);

////////////////////////////////////////////////////////////////////////////////
/// @brief status update
////////////////////////////////////////////////////////////////////////////////

      void taskStatusUpdate (const mesos::TaskStatus& status);

////////////////////////////////////////////////////////////////////////////////
/// @brief destroys the cluster
////////////////////////////////////////////////////////////////////////////////

      void destroy ();

////////////////////////////////////////////////////////////////////////////////
/// @brief endpoints for reading
////////////////////////////////////////////////////////////////////////////////

      std::vector<std::string> coordinatorEndpoints ();

////////////////////////////////////////////////////////////////////////////////
/// @brief endpoints for writing
////////////////////////////////////////////////////////////////////////////////

      std::vector<std::string> dbserverEndpoints ();

// -----------------------------------------------------------------------------
// --SECTION--                                                   private methods
// -----------------------------------------------------------------------------

    private:

    void dispatch ();
    void prepareReconciliation ();
    void reconcileTasks ();
      bool checkTimeouts ();
    void applyStatusUpdates ();
    bool checkOutstandOffers ();
      bool startNewInstances ();
    bool makePersistentVolume (const std::string& name, const mesos::Offer&, const mesos::Resources&);
    bool makeDynamicReservation (const mesos::Offer&, const mesos::Resources&);
    void startInstance (InstanceActionState, const ResourceCurrent&, const AspectPosition&);
    void fillKnownInstances (AspectType, const InstancesCurrent&);
    void killAllInstances ();

// -----------------------------------------------------------------------------
// --SECTION--                                                 private variables
// -----------------------------------------------------------------------------

    private:

////////////////////////////////////////////////////////////////////////////////
/// @brief stop flag
////////////////////////////////////////////////////////////////////////////////

      std::atomic<bool> _stopDispatcher;

////////////////////////////////////////////////////////////////////////////////
/// @brief main dispatcher thread
////////////////////////////////////////////////////////////////////////////////

      std::thread* _dispatcher;

////////////////////////////////////////////////////////////////////////////////
/// @brief next implicit reconciliation
////////////////////////////////////////////////////////////////////////////////

      std::chrono::steady_clock::time_point _nextImplicitReconciliation;

////////////////////////////////////////////////////////////////////////////////
/// @brief implicit reconciliation intervall
////////////////////////////////////////////////////////////////////////////////

      std::chrono::steady_clock::duration _implicitReconciliationIntervall;

////////////////////////////////////////////////////////////////////////////////
/// @brief maximal reconciliation intervall
////////////////////////////////////////////////////////////////////////////////

      std::chrono::steady_clock::duration _maxReconcileIntervall;

////////////////////////////////////////////////////////////////////////////////
/// @brief tasks to reconcile
////////////////////////////////////////////////////////////////////////////////

      std::unordered_map<std::string, ReconcileTasks> _reconcilationTasks;

////////////////////////////////////////////////////////////////////////////////
/// @brief positions of tasks
////////////////////////////////////////////////////////////////////////////////

      std::unordered_map<std::string, AspectPosition> _task2position;

////////////////////////////////////////////////////////////////////////////////
/// @brief protects _taskStatusUpdates and _storedOffers
////////////////////////////////////////////////////////////////////////////////

      std::mutex _lock;

////////////////////////////////////////////////////////////////////////////////
/// @brief offers received
////////////////////////////////////////////////////////////////////////////////

      std::unordered_map<std::string, mesos::Offer> _storedOffers;

////////////////////////////////////////////////////////////////////////////////
/// @brief status updates received
////////////////////////////////////////////////////////////////////////////////

      std::vector<mesos::TaskStatus> _taskStatusUpdates;
  };
}

#endif

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
