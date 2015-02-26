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
  class Instance;
  class OfferAnalysis;

// -----------------------------------------------------------------------------
// --SECTION--                                                    enum AspectsId
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief ASPECTS_ID_LEN
////////////////////////////////////////////////////////////////////////////////

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
      Aspects (const string& name, const string& role);

    public:
      virtual size_t id () const = 0;
      virtual bool isUsable () const = 0;
      virtual string arguments (const mesos::Offer&, const OfferAnalysis&) const = 0;
      virtual void instanceUp (const Instance&) = 0;

    public:
      const string _name;
      const string _role;

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
      unordered_set<string> _blockedSlaves;             // slaveId
      unordered_set<string> _startedSlaves;             // slaveId
      unordered_set<string> _preferredSlaves;           // slaveId

      unordered_map<string, uint64_t> _slave2task;      // slaveId, Instance
  };

// -----------------------------------------------------------------------------
// --SECTION--                                               class OfferAnalysis
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief OfferAnalysisType
////////////////////////////////////////////////////////////////////////////////

  enum class OfferAnalysisType {
    USABLE,
    DYNAMIC_RESERVATION_REQUIRED,
    PERSISTENT_VOLUME_REQUIRED,
    NOT_USABLE
  };

  inline const string& toString (OfferAnalysisType type) {
    static const string USABLE = "USABLE";
    static const string DYNAMIC_RESERVATION_REQUIRED = "DYNAMIC_RESERVATION_REQUIRED";
    static const string PERSISTENT_VOLUME_REQUIRED = "PERSISTENT_VOLUME_REQUIRED";
    static const string NOT_USABLE = "NOT_USABLE";
    static const string UNKNOWN = "UNKNOWN";

    switch (type) {
      case OfferAnalysisType::USABLE:
        return USABLE;

      case OfferAnalysisType::DYNAMIC_RESERVATION_REQUIRED:
        return DYNAMIC_RESERVATION_REQUIRED;

      case OfferAnalysisType::PERSISTENT_VOLUME_REQUIRED:
        return PERSISTENT_VOLUME_REQUIRED;

      case OfferAnalysisType::NOT_USABLE:
        return NOT_USABLE;
    }

    return UNKNOWN;
  }

////////////////////////////////////////////////////////////////////////////////
/// @brief OfferAnalysis
////////////////////////////////////////////////////////////////////////////////

  class OfferAnalysis {
    public:
      OfferAnalysisType _status;
      mesos::Resources _resources;
      string _containerPath;
      string _hostPath;
      vector<uint32_t> _ports;
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

  inline const string& toString (const InstanceState& state) {
    static const string STARTED = "STARTED";
    static const string RUNNING = "RUNNING";
    static const string FINISHED = "FINISHED";
    static const string FAILED = "FAILED";
    static const string UNKNOWN = "UNKNOWN";

    switch (state) {
      case InstanceState::STARTED: return STARTED; break;
      case InstanceState::RUNNING: return RUNNING; break;
      case InstanceState::FINISHED: return FINISHED; break;
      case InstanceState::FAILED: return FAILED; break;
    }

    return UNKNOWN;
  }

// -----------------------------------------------------------------------------
// --SECTION--                                                    class Instance
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief Instance
////////////////////////////////////////////////////////////////////////////////

  class Instance {
    public:
      uint64_t _taskId;
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

      ArangoManager (const string& role, ArangoScheduler*);

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

      void statusUpdate (uint64_t, InstanceState);

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
