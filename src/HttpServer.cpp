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

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string>

#include <mesos/mesos.pb.h>

#include "ArangoManager.h"
#include "utils.h"

using namespace std;
using namespace mesos;
using namespace arangodb;

#define GET             0
#define POST            1
#define POSTBUFFERSIZE  512

// -----------------------------------------------------------------------------
// --SECTION--                                                  helper functions
// -----------------------------------------------------------------------------

namespace {
  string contentTypeByFilename (const string& filename) {
    string::size_type n = filename.find(".");

    if (n == string::npos) {
      return "text/plain";
    }

    string suffix = filename.substr(n + 1);

    if (suffix == "gif") return "image/gif";
    if (suffix == "jpg") return "image/jpg";
    if (suffix == "png") return "image/png";
    if (suffix == "tiff") return "image/tiff";
    if (suffix == "ico") return "image/x-icon";
    if (suffix == "css") return "text/css";
    if (suffix == "js") return "text/javascript";
    if (suffix == "json") return "application/json";
    if (suffix == "html") return "text/html";
    if (suffix == "htm") return "text/html";
    if (suffix == "pdf") return "application/pdf";
    if (suffix == "ps") return "application/postscript";
    if (suffix == "txt") return "text/plain";
    if (suffix == "text") return "text/plain";
    if (suffix == "xml") return "application/xml";
    if (suffix == "dtd") return "application/xml-dtd";
    if (suffix == "svg") return "image/svg+xml";
    if (suffix == "ttf") return "application/x-font-ttf";
    if (suffix == "otf") return "application/x-font-opentype";
    if (suffix == "woff") return "application/font-woff";
    if (suffix == "eot") return "application/vnd.ms-fontobject";
    if (suffix == "bz2") return "application/x-bzip2";
    if (suffix == "gz") return "application/x-gzip";
    if (suffix == "tgz") return "application/x-tar";
    if (suffix == "zip") return "application/x-compressed-zip";
    if (suffix == "doc") return "application/msword";

    return "text/plain";
  }

  picojson::object JsonClusterInfo (const ClusterInfo& info) {
    picojson::object o;

    o["name"] = picojson::value(info._name);

    picojson::object planned;

    planned["servers"] = picojson::value(info._planned._servers);
    planned["agencies"] = picojson::value((double) info._planned._agencies);
    planned["coordinators"] = picojson::value((double) info._planned._coordinators);
    planned["dbservers"] = picojson::value((double) info._planned._dbservers);
    planned["cpus"] = picojson::value(info._planned._cpus);
    planned["memory"] = picojson::value(info._planned._memory);
    planned["disk"] = picojson::value(info._planned._disk);

    o["planned"] = picojson::value(planned);

    picojson::object running;

    running["servers"] = picojson::value(info._running._servers);
    running["agencies"] = picojson::value((double) info._running._agencies);
    running["coordinators"] = picojson::value((double) info._running._coordinators);
    running["dbservers"] = picojson::value((double) info._running._dbservers);
    running["cpus"] = picojson::value(info._running._cpus);
    running["memory"] = picojson::value(info._running._memory);
    running["disk"] = picojson::value(info._running._disk);

    o["running"] = picojson::value(running);

    return o;
  }

  picojson::array JsonResources (const Resources& resources) {
    picojson::array rs;

    for (auto& resource : resources) {
      picojson::object r;

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

    return rs;
  }

  picojson::object JsonOffer (const Offer& offer) {
    picojson::object o;

    o["id"] = picojson::value(offer.id().value());
    o["slaveId"] = picojson::value(offer.slave_id().value());
    o["resources"] = picojson::value(JsonResources(offer.resources()));

    return o;
  }

  picojson::object JsonOfferSummary (const OfferSummary& summary) {
    const Offer& offer = summary._offer;

    picojson::object o;

    o["offerId"] = picojson::value(offer.id().value());
    o["slaveId"] = picojson::value(offer.slave_id().value());

    picojson::object resources;

    Resources r = offer.resources();

    resources["cpus"] = picojson::value(cpus(r));
    resources["memory"] = picojson::value(memory(r) * 1024 * 1024);
    resources["disk"] = picojson::value(diskspace(r) * 1024 * 1024);

    o["resources"] = picojson::value(resources);

    picojson::object status;

    status["agency"] = picojson::value(toStringShort(summary._analysis[(int) AspectsId::ID_AGENCY]._state));
    status["coordinator"] = picojson::value(toStringShort(summary._analysis[(int) AspectsId::ID_COORDINATOR]._state));
    status["dbserver"] = picojson::value(toStringShort(summary._analysis[(int) AspectsId::ID_DBSERVER]._state));

    o["status"] = picojson::value(status);

    return o;
  }

  picojson::object JsonInstance (const Instance& instance) {
    picojson::object o;

    o["taskId"] = picojson::value(instance._taskId);
    o["slaveId"] = picojson::value(instance._slaveId);
    o["hostname"] = picojson::value(instance._hostname);
    o["started"] = picojson::value(toStringSystemTime(instance._started));
    o["lastUpdate"] = picojson::value(toStringSystemTime(instance._lastUpdate));
    o["status"] = picojson::value(toString(instance._state));

    switch (instance._aspectId) {
      case ASPECTS_ID_AGENCY:
        o["aspect"] = picojson::value("AGENCY");
        break;

      case ASPECTS_ID_COORDINATOR:
        o["aspect"] = picojson::value("COORDINATOR");
        break;

      case ASPECTS_ID_DBSEVER:
        o["aspect"] = picojson::value("DBSERVER");
        break;

      default:
        o["aspect"] = picojson::value("");
        break;
    }

    picojson::object resources;

    const Resources& r = instance._resources;

    resources["cpus"] = picojson::value(cpus(r));
    resources["memory"] = picojson::value(memory(r) * 1024 * 1024);
    resources["disk"] = picojson::value(diskspace(r) * 1024 * 1024);

    o["resources"] = picojson::value(resources);

    if (instance._state == InstanceState::RUNNING) {
      string link = "http://" + instance._hostname + ":";

      if (instance._aspectId == ASPECTS_ID_AGENCY) {
        link += to_string(instance._ports[1]) + "/v2/keys";
      }
      else {
        link += to_string(instance._ports[0]) + "/";
      }

      o["link"] = picojson::value(link);
    }

    return o;
  }

  picojson::object JsonSlaveInfo (const arangodb::SlaveInfo& info) {
    picojson::object o;

    o["name"] = picojson::value(info._name);

    picojson::object available;

    available["cpus"] = picojson::value(info._available._cpus);
    available["memory"] = picojson::value(info._available._memory);
    available["disk"] = picojson::value(info._available._disk);

    o["available"] = picojson::value(available);

    picojson::object used;

    used["cpus"] = picojson::value(info._used._cpus);
    used["memory"] = picojson::value(info._used._memory);
    used["disk"] = picojson::value(info._used._disk);

    o["used"] = picojson::value(used);

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
    string GET_V1_CLUSTER (const string&);
    string GET_V1_CLUSTER_NAME (const string&);
    string POST_V1_CLUSTER_NAME (const string&, const string&);
    string GET_V1_SERVERS_NAME (const string&);
    string GET_V1_OFFERS_NAME (const string&);
    string GET_V1_INSTANCES_NAME (const string&);

    string GET_DEBUG_OFFERS (const string&);
    string GET_DEBUG_INSTANCES (const string&);

  private:
    ArangoManager* _manager;
};

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /v1/cluster
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_V1_CLUSTER (const string&) {
  const vector<ClusterInfo> infos = _manager->clusters();

  picojson::object result;
  picojson::array list;

  for (const auto& info : infos) {
    list.push_back(picojson::value(JsonClusterInfo(info)));
  }

  result["clusters"] = picojson::value(list);

  return picojson::value(result).serialize();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /v1/cluster/<name>
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_V1_CLUSTER_NAME (const string& name) {
  ClusterInfo info = _manager->cluster(name);

  return picojson::value(JsonClusterInfo(info)).serialize();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief POST /v1/cluster/<name>
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::POST_V1_CLUSTER_NAME (const string& name, const string& body) {
  picojson::value v;
  std::string err = picojson::parse(v, body);

  // TODO(fc) error handling

  if (! err.empty() || ! v.is<picojson::object>()) {
    return "failed";
  }

  const picojson::value::object& obj = v.get<picojson::object>();
  auto iter = obj.find("servers");

  if (iter != obj.end() && iter->second.is<double>()) {
    _manager->adjustServers(name, iter->second.get<double>());
  }
  else {
    iter = obj.find("agencies");

    if (iter != obj.end() && iter->second.is<double>()) {
      _manager->adjustAgencies(name, iter->second.get<double>());
    }
    else {
      iter = obj.find("coordinators");

      if (iter != obj.end() && iter->second.is<double>()) {
        _manager->adjustCoordinators(name, iter->second.get<double>());
      }
      else {
        iter = obj.find("dbservers");

        if (iter != obj.end() && iter->second.is<double>()) {
          _manager->adjustDbservers(name, iter->second.get<double>());
        }
      }
    }
  }

  ClusterInfo info = _manager->cluster(name);
  return picojson::value(JsonClusterInfo(info)).serialize();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /v1/servers/<name>
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_V1_SERVERS_NAME (const string& name) {
  const vector<SlaveInfo> infos = _manager->slaveInfo(name);

  picojson::object result;
  picojson::array list;

  for (const auto& info : infos) {
    list.push_back(picojson::value(JsonSlaveInfo(info)));
  }

  result["servers"] = picojson::value(list);

  return picojson::value(result).serialize();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /v1/offers/<name>
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_V1_OFFERS_NAME (const string& name) {
  vector<OfferSummary> offers = _manager->currentOffers();

  picojson::object result;
  picojson::array list;

  for (const auto& offer : offers) {
    list.push_back(picojson::value(JsonOfferSummary(offer)));
  }

  result["offers"] = picojson::value(list);

  return picojson::value(result).serialize();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /v1/instances/<name>
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_V1_INSTANCES_NAME (const string& name) {
  vector<Instance> instances = _manager->currentInstances();

  picojson::object result;
  picojson::array list;

  for (const auto& instance : instances) {
    list.push_back(picojson::value(JsonInstance(instance)));
  }

  result["instances"] = picojson::value(list);

  return picojson::value(result).serialize();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /debug/offers
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_DEBUG_OFFERS (const string& name) {
  vector<OfferSummary> offers = _manager->currentOffers();

  picojson::object result;
  picojson::array list;

  for (const auto& offer : offers) {
    picojson::object r;

    r["offer"] = picojson::value(JsonOffer(offer._offer));

    picojson::object s;

    s["agency"] = picojson::value(toString(offer._analysis[0]._state));
    s["coordinator"] = picojson::value(toString(offer._analysis[1]._state));
    s["dbserver"] = picojson::value(toString(offer._analysis[2]._state));

    r["status"] = picojson::value(s);

    list.push_back(picojson::value(r));
  }

  result["offers"] = picojson::value(list);

  return picojson::value(result).serialize();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /debug/instances
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_DEBUG_INSTANCES (const string& name) {
  vector<Instance> instances = _manager->currentInstances();

  picojson::object result;
  picojson::array list;

  for (const auto& instance : instances) {
    picojson::object o;

    o["taskId"] = picojson::value(instance._taskId);
    o["aspectId"] = picojson::value((double) instance._aspectId);
    o["state"] = picojson::value(toString(instance._state));
    o["slaveId"] = picojson::value(instance._slaveId);
    o["resources"] = picojson::value(JsonResources(instance._resources));
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
/// @brief file callback for read
////////////////////////////////////////////////////////////////////////////////

static ssize_t file_reader (void *cls, uint64_t pos, char *buf, size_t max) {
  FILE *file = reinterpret_cast<FILE*>(cls);

  (void) fseek(file, pos, SEEK_SET);
  return fread(buf, 1, max, file);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief file callback for free
////////////////////////////////////////////////////////////////////////////////

static void free_callback (void *cls) {
  FILE *file = reinterpret_cast<FILE*>(cls);

  fclose(file);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief connection structure
////////////////////////////////////////////////////////////////////////////////

struct ConnectionInfo {
  int type;

  string (HttpServerImpl::*getMethod)(const string&);
  string (HttpServerImpl::*postMethod)(const string&, const string&);

  string prefix;
  string body;

  string filename;

  struct MHD_PostProcessor* processor;
};

////////////////////////////////////////////////////////////////////////////////
/// @brief callback if request has completed
////////////////////////////////////////////////////////////////////////////////

static void requestCompleted (void* cls,
                              struct MHD_Connection* connection,
                              void** con_cls,
                              enum MHD_RequestTerminationCode toe) {
  ConnectionInfo *conInfo = reinterpret_cast<ConnectionInfo*>(*con_cls);

  if (NULL == conInfo) {
    return;
  }

  if (conInfo->type == POST) {
    MHD_destroy_post_processor(conInfo->processor);
  }

  delete conInfo;
  *con_cls = NULL;
}

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

  // find correct collback
  if (*ptr == nullptr) {
    ConnectionInfo* conInfo = new ConnectionInfo();

    if (0 == strcmp(method, MHD_HTTP_METHOD_GET)) {
      conInfo->type = GET;

      if (0 == strcmp(url, "/v1/cluster")) {
        conInfo->getMethod = &HttpServerImpl::GET_V1_CLUSTER;
      }
      else if (0 == strncmp(url, "/v1/cluster/", 12)) {
        conInfo->getMethod = &HttpServerImpl::GET_V1_CLUSTER_NAME;
        conInfo->prefix = url + 12;
      }
      else if (0 == strncmp(url, "/v1/servers/", 12)) {
        conInfo->getMethod = &HttpServerImpl::GET_V1_SERVERS_NAME;
        conInfo->prefix = url + 12;
      }
      else if (0 == strncmp(url, "/v1/offers/", 11)) {
        conInfo->getMethod = &HttpServerImpl::GET_V1_OFFERS_NAME;
        conInfo->prefix = url + 11;
      }
      else if (0 == strncmp(url, "/v1/instances/", 14)) {
        conInfo->getMethod = &HttpServerImpl::GET_V1_INSTANCES_NAME;
        conInfo->prefix = url + 14;
      }
      else if (0 == strcmp(url, "/debug/offers")) {
        conInfo->getMethod = &HttpServerImpl::GET_DEBUG_OFFERS;
      }
      else if (0 == strcmp(url, "/debug/instances")) {
        conInfo->getMethod = &HttpServerImpl::GET_DEBUG_INSTANCES;
      }
      else {
        conInfo->filename = "assets/";

        if (url[1] == '\0') {
          conInfo->filename += "index.html";
        }
        else {
          conInfo->filename += &url[1];
        }
      }
    }
    else if (0 == strcmp(method, MHD_HTTP_METHOD_POST)) {
      conInfo->type = POST;

      if (0 == strncmp(url, "/v1/cluster/", 12)) {
        conInfo->postMethod = &HttpServerImpl::POST_V1_CLUSTER_NAME;
        conInfo->prefix = url + 12;
      }
    }

    if (conInfo->getMethod == nullptr && conInfo->postMethod == nullptr && conInfo->filename.empty()) {
      return MHD_NO;
    }

    *ptr = reinterpret_cast<void*>(conInfo);
    return MHD_YES;
  }

  // generate response
  ConnectionInfo* conInfo = reinterpret_cast<ConnectionInfo*>(*ptr);
  struct MHD_Response *response;
  int ret;

  // handle GET
  if (conInfo->getMethod != nullptr) {
    LOG(INFO)
    << "handling http request '" << method << " " << url << "'";

    const string r = (me->*(conInfo->getMethod))(conInfo->prefix);

    response = MHD_create_response_from_buffer(
      r.length(), (void *) r.c_str(),
      MHD_RESPMEM_MUST_COPY);

    MHD_add_response_header(
      response, 
      "Content-Type", 
      "application/json; charset=utf-8");

    ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
    MHD_destroy_response(response);
  }

  // handle POST
  else if (conInfo->postMethod != nullptr) {
    if (*upload_data_size != 0) {
      conInfo->body += string(upload_data, *upload_data_size);
      *upload_data_size = 0;

      return MHD_YES;
    }

    LOG(INFO)
    << "handling http request '" << method << " " << url << "'";

    const string r = (me->*(conInfo->postMethod))(conInfo->prefix, conInfo->body);

    response = MHD_create_response_from_buffer(
      r.length(), (void *) r.c_str(),
      MHD_RESPMEM_MUST_COPY);

    MHD_add_response_header(
      response, 
      "Content-Type", 
      "application/json; charset=utf-8");

    ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
    MHD_destroy_response(response);
  }

  // handle FILE
  else if (! conInfo->filename.empty()) {
    LOG(INFO)
    << "handling http request '" << method << " " << url << "'";

    struct stat buf;

    if (0 != ::stat(conInfo->filename.c_str(), &buf))  {
      return MHD_NO;
    }

    FILE* file = fopen(conInfo->filename.c_str(), "rb");

    if (file == nullptr) {
      return MHD_NO;
    }

    response = MHD_create_response_from_callback(buf.st_size,
                                                 32 * 1024,
                                                 &file_reader,
                                                 file,
                                                 &free_callback);
    if (response == NULL) {
      fclose (file);
      return MHD_NO;
    }

    MHD_add_response_header(
      response, 
      "Content-Type", 
      contentTypeByFilename(conInfo->filename).c_str());

    ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
    MHD_destroy_response(response);
  }
  else {
    return MHD_NO;
  }

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
    MHD_OPTION_NOTIFY_COMPLETED, requestCompleted, nullptr,
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
