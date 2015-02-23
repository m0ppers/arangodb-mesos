////////////////////////////////////////////////////////////////////////////////
/// @brief http server
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

#include "HttpServer.h"

#include <string.h>
#include <picojson.h>

#include <string>

#include <mesos/mesos.pb.h>

#include "ArangoManager.h"

using namespace std;
using namespace mesos;
using namespace arangodb;

// -----------------------------------------------------------------------------
// --SECTION--                                                  helper functions
// -----------------------------------------------------------------------------

/*
string JsonConfig (size_t instances,
                   const ArangoManager::BasicResources& resources) {
  picojson::object r1;

  r1["cpus"] = picojson::value(resources._cpus);
  r1["mem"] = picojson::value((double) resources._mem);
  r1["disk"] = picojson::value((double) resources._disk);
  r1["ports"] = picojson::value((double) resources._ports);

  picojson::object result;

  result["instances"] = picojson::value((double) instances);
  result["resources"] = picojson::value(r1);

  return picojson::value(result).serialize();
}
*/


namespace {
  picojson::object JsonOffer (const Offer& offer) {
    picojson::object o;

    o["id"] = picojson::value(offer.id().value());
    o["slaveId"] = picojson::value(offer.slave_id().value());

    picojson::array rs;

    for (int i = 0; i < offer.resources_size(); ++i) {
      picojson::object r;

      const auto& resource = offer.resources(i);

      r["type"] = picojson::value(resource.name());

      if (resource.type() == Value::SCALAR) {
        r["value"] = picojson::value(resource.scalar().value());
      }
      else if (resource.type() == Value::RANGES) {
        picojson::array ras;

        const auto& ranges = resource.ranges();

        for (int j = 0; j < ranges.range_size(); ++j) {
          picojson::object ra;

          const auto& range = ranges.range(j);

          ra["begin"] = picojson::value((double) range.begin());
          ra["end"] = picojson::value((double) range.end());

          ras.push_back(picojson::value(ra));
        }

        r["value"] = picojson::value(ras);
      }

      if (resource.has_role()) {
        r["role"] = picojson::value(resource.role());
      }

      if (resource.has_reserver_type()) {
        switch (resource.reserver_type()) {
          case Resource::SLAVE:
            r["reservationType"] = picojson::value("SLAVE");
            break;

          case Resource::FRAMEWORK:
            r["reservationType"] = picojson::value("FRAMEWORK");
            break;
        }
      }

      if (resource.has_disk()) {
        Resource_DiskInfo disk = resource.disk();
        picojson::object di;

        if (disk.has_persistence()) {
          di["id"] = picojson::value(disk.persistence().id());
        }

        if (disk.has_volume()) {
          Volume volume = disk.volume();
          picojson::object vo;

          vo["containerPath"] = picojson::value(volume.container_path());

          if (volume.has_host_path()) {
            vo["hostPath"] = picojson::value(volume.host_path());
          }

          switch (volume.mode()) {
            case Volume::RW:
              vo["mode"] = picojson::value("RW");
              break;

            case Volume::RO:
              vo["mode"] = picojson::value("RO");
              break;
          }

          di["volume"] = picojson::value(vo);
        }

        r["disk"] = picojson::value(di);
      }

      rs.push_back(picojson::value(r));
    }
      
    o["resources"] = picojson::value(rs);

    return o;
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                              class HttpServerImpl
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief http server implementation class
////////////////////////////////////////////////////////////////////////////////

class arangodb::HttpServerImpl {
  public:
    HttpServerImpl (ArangoManager* manager) 
      : _manager(manager) {
    }

  public:
    string GET_V1_CONFIG_AGENCY ();
    string GET_V1_CONFIG_COORDINATOR ();
    string GET_V1_CONFIG_DBSERVER ();
    string GET_DEBUG_OFFERS ();
    string GET_DEBUG_INSTANCES ();

  private:
    ArangoManager* _manager;
};

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /v1/config/agency
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_V1_CONFIG_AGENCY () {
  size_t instances = _manager->agencyInstances();
//  ArangoManager::BasicResources resources = _manager->agencyResources();

//  return JsonConfig(instances, resources);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /v1/config/coordinator
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_V1_CONFIG_COORDINATOR () {
  size_t instances = _manager->coordinatorInstances();
//  ArangoManager::BasicResources resources = _manager->coordinatorResources();

//  return JsonConfig(instances, resources);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /v1/config/dbserver
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_V1_CONFIG_DBSERVER () {
  size_t instances = _manager->dbserverInstances();
//  ArangoManager::BasicResources resources = _manager->dbserverResources();

//  return JsonConfig(instances, resources);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /debug/offers
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_DEBUG_OFFERS () {
  vector<OfferSummary> offers = _manager->currentOffers();

  picojson::object result;
  picojson::array list;

  for (const auto& offer : offers) {
    picojson::object r;

    r["offer"] = picojson::value(JsonOffer(offer._offer));
    r["status"] = picojson::value(toString(offer._analysis[0]._status));

    list.push_back(picojson::value(r));
  }

  result["offers"] = picojson::value(list);

  return picojson::value(result).serialize();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /debug/instances
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_DEBUG_INSTANCES () {
  vector<Instance> instances = _manager->currentInstances();

  picojson::object result;
  picojson::array list;

  for (const auto& instance : instances) {
    picojson::object o;

    o["taskId"] = picojson::value((double) instance._taskId);
    o["aspectId"] = picojson::value((double) instance._aspectId);
    o["state"] = picojson::value(toString(instance._state));
    o["slaveId"] = picojson::value(instance._slaveId);
    o["started"] = picojson::value((double) instance._started.time_since_epoch().count());
    o["lastUpdate"] = picojson::value((double) instance._lastUpdate.time_since_epoch().count());

    list.push_back(picojson::value(o));
  }

  result["instances"] = picojson::value(list);

  return picojson::value(result).serialize();
}

// -----------------------------------------------------------------------------
// --SECTION--                                                  class HttpServer
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// --SECTION--                                                  helper functions
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief callback for daemon
////////////////////////////////////////////////////////////////////////////////

static int answerRequest (
  void* cls,
  struct MHD_Connection* connection,
  const char* url,
  const char* method,
  const char* version,
  const char* upload_data,
  size_t* upload_data_size,
  void** ptr) {
  HttpServerImpl* me = reinterpret_cast<HttpServerImpl*>(cls);

  LOG(INFO)
  << "handling http request '" << method << " " << url << "'";

  // find correct collback
  string (HttpServerImpl::*getMethod)() = nullptr;

  if (0 == strcmp(method, "GET")) {
    if (0 == strcmp(url, "/v1/config/agency")) {
      getMethod = &HttpServerImpl::GET_V1_CONFIG_AGENCY;
    }
    else if (0 == strcmp(url, "/v1/config/coordinator")) {
      getMethod = &HttpServerImpl::GET_V1_CONFIG_COORDINATOR;
    }
    else if (0 == strcmp(url, "/v1/config/dbserver")) {
      getMethod = &HttpServerImpl::GET_V1_CONFIG_DBSERVER;
    }
    else if (0 == strcmp(url, "/debug/offers")) {
      getMethod = &HttpServerImpl::GET_DEBUG_OFFERS;
    }
    else if (0 == strcmp(url, "/debug/instances")) {
      getMethod = &HttpServerImpl::GET_DEBUG_INSTANCES;
    }
  }

  if (getMethod == nullptr) {
    return MHD_NO;
  }

  // do never respond on first call
  static int aptr;

  if (&aptr != *ptr) {
    *ptr = &aptr;
    return MHD_YES;
  }

  // generate response
  struct MHD_Response *response;
  int ret;

  /* reset when done */
  *ptr = NULL;

  const string r = (me->*getMethod)();

  response = MHD_create_response_from_buffer(
    r.length(), (void *) r.c_str(),
    MHD_RESPMEM_MUST_COPY);

  MHD_add_response_header(
    response, 
    "Content-Type", 
    "application/json; charset=utf-8");

  ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
  MHD_destroy_response(response);

  return ret;
}

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

HttpServer::HttpServer (ArangoManager* manager) 
  : _daemon(nullptr),
    _manager(manager) {
  _impl = new HttpServerImpl(manager);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief destructor
////////////////////////////////////////////////////////////////////////////////

HttpServer::~HttpServer () {
  delete _impl;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief starts the server on a given port
////////////////////////////////////////////////////////////////////////////////

void HttpServer::start (int port) {
  _daemon = MHD_start_daemon (
    MHD_USE_SELECT_INTERNALLY /* | MHD_USE_DEBUG */,
    port,
    nullptr, nullptr,
    &answerRequest, (void*) _impl,
    MHD_OPTION_CONNECTION_TIMEOUT, (unsigned int) 120,
    MHD_OPTION_END);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief stops the server
////////////////////////////////////////////////////////////////////////////////

void HttpServer::stop () {
  if (_daemon != nullptr) {
    MHD_stop_daemon(_daemon);
    _daemon = nullptr;
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------

// Local Variables:
// mode: outline-minor
// outline-regexp: "/// @brief\\|/// {@inheritDoc}\\|/// @page\\|// --SECTION--\\|/// @\\}"
// End:
