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

#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <mesos/resources.hpp>
#include <mesos/scheduler.hpp>

namespace arangodb {
  using namespace std;

  class ArangoManagerImpl;
  class ArangoScheduler;
  class ArangoState;
  class Instance;
  class OfferAnalysis;

// -----------------------------------------------------------------------------
// --SECTION--                                                enum InstanceState
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief instance states
////////////////////////////////////////////////////////////////////////////////

  enum class InstanceState {
    STARTED,
    RUNNING,
    FINISHED,
    FAILED
  };

  inline string toString (const InstanceState& state) {
    switch (state) {
      case InstanceState::STARTED: return "STARTED"; break;
      case InstanceState::RUNNING: return "RUNNING"; break;
      case InstanceState::FINISHED: return "FINISHED"; break;
      case InstanceState::FAILED: return "FAILED"; break;
    }

    return "UNKNOWN";
  }

// -----------------------------------------------------------------------------
// --SECTION--                                                    class Instance
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief Instance
////////////////////////////////////////////////////////////////////////////////

  class Instance {
    public:
      string _taskId;
      size_t _aspectId;
      InstanceState _state;
      mesos::Resources _resources;
      string _slaveId;
      string _hostname;
      vector<uint32_t> _ports;
      chrono::system_clock::time_point _started;
      chrono::system_clock::time_point _lastUpdate;
  };

// -----------------------------------------------------------------------------
// --SECTION--                                             class InstanceManager
// -----------------------------------------------------------------------------

  class InstanceManager {
    public:
      unordered_map<string, Instance> _instances;
  };

// -----------------------------------------------------------------------------
// --SECTION--                                                    enum AspectsId
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief ASPECTS_ID_LEN
////////////////////////////////////////////////////////////////////////////////

#define ASPECTS_ID_AGENCY 0
#define ASPECTS_ID_COORDINATOR 1
#define ASPECTS_ID_DBSEVER 2
#define ASPECTS_ID_LEN 3

////////////////////////////////////////////////////////////////////////////////
/// @brief AspectsId
////////////////////////////////////////////////////////////////////////////////

  enum class AspectsId {
    ID_AGENCY = 0,
    ID_COORDINATOR = 1,
    ID_DBSERVER = 2
  };

// -----------------------------------------------------------------------------
// --SECTION--                                                     class Aspects
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief Aspects
////////////////////////////////////////////////////////////////////////////////

  class Aspects {
    public:
      Aspects (const string& name,
               const string& role,
               const string& principal,
               InstanceManager*);

    public:
      virtual size_t id () const = 0;
      virtual bool isUsable () const = 0;
      virtual string arguments (const mesos::Offer&, const OfferAnalysis&, const string& taskId) const = 0;
      virtual bool instanceUp (const Instance&) = 0;

    public:
      const string _name;
      const string _role;
      const string _principal;

      mesos::Resources _minimumResources;
      mesos::Resources _additionalResources;

      bool _persistentVolumeRequired;
      size_t _requiredPorts;

      size_t _plannedInstances;
      size_t _minimumInstances;

    public:
      size_t _startedInstances;
      size_t _runningInstances;

    public:
      unordered_set<string> _startedSlaves;             // slaveId
      unordered_set<string> _preferredSlaves;           // slaveId

      unordered_map<string, string> _slave2task;        // slaveId, instanceId

      InstanceManager* _instanceManager;
  };

// -----------------------------------------------------------------------------
// --SECTION--                                               class OfferAnalysis
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief OfferAnalysisStatus
///
/// Status:
///
/// TOO_SMALL: offer is too small
///
/// DYNAMIC_RESERVATION_REQUIRED: offer suitable, but requires dynamic
///                               reservation
///
/// PERSISTENT_VOLUME_REQUIRED: offer suitable, bute requires persistent
///                             volume
///
/// USABLE: offer is usable
///
/// WAIT: usable, but currently not required.
///
/// Events:
///
/// addOffer: offer with DYNAMIC_RESERVATION_REQUIRED, TOO_SMALL
///                      PERSISTENT_VOLUME_REQUIRED, USABLE
///
/// instance finished: change WAIT to USABLE for all
///
/// change in minimum requirements: change TOO_SMALL to NEW for all
///
////////////////////////////////////////////////////////////////////////////////

  enum class OfferAnalysisStatus {
    DYNAMIC_RESERVATION_REQUIRED,
    PERSISTENT_VOLUME_REQUIRED,
    TOO_SMALL,
    USABLE,
    WAIT
  };

  inline string toString (OfferAnalysisStatus type) {
    switch (type) {
      case OfferAnalysisStatus::DYNAMIC_RESERVATION_REQUIRED:
        return "DYNAMIC_RESERVATION_REQUIRED";

      case OfferAnalysisStatus::PERSISTENT_VOLUME_REQUIRED:
        return "PERSISTENT_VOLUME_REQUIRED";

      case OfferAnalysisStatus::TOO_SMALL:
        return "TOO_SMALL";

      case OfferAnalysisStatus::USABLE:
        return "USABLE";

      case OfferAnalysisStatus::WAIT:
        return "WAIT";
    }

    return "UNKNOWN";
  }


  inline string toStringShort (OfferAnalysisStatus type) {
    switch (type) {
      case OfferAnalysisStatus::DYNAMIC_RESERVATION_REQUIRED:
        return "DYNREQ";

      case OfferAnalysisStatus::PERSISTENT_VOLUME_REQUIRED:
        return "VOLREQ";

      case OfferAnalysisStatus::TOO_SMALL:
        return "SMALL";

      case OfferAnalysisStatus::USABLE:
        return "USE";

      case OfferAnalysisStatus::WAIT:
        return "WAIT";
    }

    return "UNKNOWN";
  }

////////////////////////////////////////////////////////////////////////////////
/// @brief OfferAnalysis
////////////////////////////////////////////////////////////////////////////////

  class OfferAnalysis {
    public:
      OfferAnalysisStatus _state;
      mesos::Resources _resources;
      string _containerPath;
      string _hostPath;
      vector<uint32_t> _ports;
      OfferAnalysisStatus _initialState;
  };

// -----------------------------------------------------------------------------
// --SECTION--                                                class OfferSummary
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief OfferSummary
////////////////////////////////////////////////////////////////////////////////

  class OfferSummary {
    public:
      bool _usable;
      mesos::Offer _offer;
      OfferAnalysis _analysis[ASPECTS_ID_LEN];
  };

// -----------------------------------------------------------------------------
// --SECTION--                                                   class SlaveInfo
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief Instance
////////////////////////////////////////////////////////////////////////////////

  class SlaveInfoDetails {
    public:
      double _cpus;
      double _memory;
      double _disk;
  };

  class SlaveInfo {
    public:
      string _name;
      SlaveInfoDetails _available;
      SlaveInfoDetails _used;
  };

// -----------------------------------------------------------------------------
// --SECTION--                                                 class ClusterInfo
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief ClusterInfo
////////////////////////////////////////////////////////////////////////////////

  class ClusterInfoDetails {
    public:
      double _servers;
      size_t _agencies;
      size_t _coordinators;
      size_t _dbservers;
      double _cpus;
      double _memory;
      double _disk;
  };

  class ClusterInfo {
    public:
      string _name;
      ClusterInfoDetails _planned;
      ClusterInfoDetails _running;
  };

// -----------------------------------------------------------------------------
// --SECTION--                                               class ArangoManager
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief manager class
////////////////////////////////////////////////////////////////////////////////

  class ArangoManager {

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

    public:

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

      ArangoManager (const string& role,
                     const string& principal,
                     ArangoScheduler*);

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

      void statusUpdate (const string&, InstanceState);

////////////////////////////////////////////////////////////////////////////////
/// @brief slave update
////////////////////////////////////////////////////////////////////////////////

      void slaveInfoUpdate (const mesos::SlaveInfo&);

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the configured clusters
////////////////////////////////////////////////////////////////////////////////

      vector<ClusterInfo> clusters () const;

////////////////////////////////////////////////////////////////////////////////
/// @brief returns information for one cluster
////////////////////////////////////////////////////////////////////////////////

      ClusterInfo cluster (const string& name) const;

////////////////////////////////////////////////////////////////////////////////
/// @brief adjusts the total number of servers
////////////////////////////////////////////////////////////////////////////////

      ClusterInfo adjustServers (const string& name, int);

////////////////////////////////////////////////////////////////////////////////
/// @brief adjusts the total number of agencies
////////////////////////////////////////////////////////////////////////////////

      ClusterInfo adjustAgencies (const string& name, int);

////////////////////////////////////////////////////////////////////////////////
/// @brief adjusts the total number of coordinators
////////////////////////////////////////////////////////////////////////////////

      ClusterInfo adjustCoordinators (const string& name, int);

////////////////////////////////////////////////////////////////////////////////
/// @brief adjusts the total number of dbservers
////////////////////////////////////////////////////////////////////////////////

      ClusterInfo adjustDbservers (const string& name, int);

////////////////////////////////////////////////////////////////////////////////
/// @brief returns information about the slaves
////////////////////////////////////////////////////////////////////////////////

      vector<SlaveInfo> slaveInfo (const string& name);

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the current offers for debugging
////////////////////////////////////////////////////////////////////////////////

      vector<OfferSummary> currentOffers ();

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the current instances for debugging
////////////////////////////////////////////////////////////////////////////////

      vector<Instance> currentInstances ();

// -----------------------------------------------------------------------------
// --SECTION--                                                 private variables
// -----------------------------------------------------------------------------

    private:

////////////////////////////////////////////////////////////////////////////////
/// @brief implementation
////////////////////////////////////////////////////////////////////////////////

      ArangoManagerImpl* _impl;

////////////////////////////////////////////////////////////////////////////////
/// @brief state
////////////////////////////////////////////////////////////////////////////////

      ArangoState* _state;

////////////////////////////////////////////////////////////////////////////////
/// @brief dispatcher thread
////////////////////////////////////////////////////////////////////////////////

      thread* _dispatcher;
  };
}

#endif

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------

// Local Variables:
// mode: outline-minor
// outline-regexp: "/// @brief\\|/// {@inheritDoc}\\|/// @page\\|// --SECTION--\\|/// @\\}"
// End:
