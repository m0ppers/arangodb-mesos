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

#include "ArangoManager.h"
#include "ArangoState.h"
#include "Caretaker.h"
#include "Global.h"
#include "utils.h"

#include <string.h>
#include <picojson.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string>
#include <thread>

#include <mesos/mesos.pb.h>

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
}

// -----------------------------------------------------------------------------
// --SECTION--                                              class HttpServerImpl
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief http server implementation class
////////////////////////////////////////////////////////////////////////////////

class arangodb::HttpServerImpl {
  public:
    string POST_V1_DESTROY (const string&, const string&);

    string GET_V1_STATE (const string&);
    string GET_V1_MODE (const string&);
    string GET_V1_HEALTH (const string&);
    string GET_V1_ENDPOINTS (const string&);

    string GET_DEBUG_TARGET (const string&);
    string GET_DEBUG_PLAN (const string&);
    string GET_DEBUG_CURRENT (const string&);
    string GET_DEBUG_OVERVIEW (const string&);
};

////////////////////////////////////////////////////////////////////////////////
/// @brief POST /v1/destroy.json
////////////////////////////////////////////////////////////////////////////////

//static void doDestroy () {
//}

string HttpServerImpl::POST_V1_DESTROY (const string& name, const string& body) {
  LOG(INFO) << "Got POST to destroy cluster and framework...";
  Global::manager().destroy();
  // std::thread killer(doDestroy);
  // killer.detach();

  sleep(15);  // Give the framework some time to kill all tasks and remove
              // the state before we answer. Then Marathon can kill us...

  picojson::object result;
  result["destroy"] = picojson::value(true);

  return picojson::value(result).serialize();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /v1/state.json
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_V1_STATE (const string&) {
  picojson::object result;
  result["mode"] = picojson::value(Global::modeLC());
  result["asyncReplication"] = picojson::value(Global::asyncReplication());
  result["health"] = picojson::value(true);
  result["role"] = picojson::value(Global::role());
  result["framework_name"] = picojson::value(Global::frameworkName());
  result["master_url"] = picojson::value(Global::masterUrl());
  result["volume_path"] = picojson::value(Global::volumePath());

  return picojson::value(result).serialize();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /v1/mode.json
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_V1_MODE (const string&) {
  picojson::object result;
  result["mode"] = picojson::value(Global::modeLC());

  return picojson::value(result).serialize();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /v1/endpoints.json
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_V1_ENDPOINTS (const string&) {
  vector<string> read = Global::manager().readEndpoints();
  vector<string> write = Global::manager().writeEndpoints();

  picojson::array readA;

  for (auto i : read) {
    readA.push_back(picojson::value(i));
  }

  picojson::array writeA;

  for (auto i : write) {
    writeA.push_back(picojson::value(i));
  }

  picojson::object result;
  result["read"] = picojson::value(readA);
  result["write"] = picojson::value(writeA);

  return picojson::value(result).serialize();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /v1/health.json
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_V1_HEALTH (const string&) {
  picojson::object result;
  result["health"] = picojson::value(true);

  return picojson::value(result).serialize();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /debug/target
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_DEBUG_TARGET (const string& name) {
  return Global::state().jsonTarget();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /debug/plan
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_DEBUG_PLAN (const string& name) {
  return Global::state().jsonPlan();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /debug/current
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_DEBUG_CURRENT (const string& name) {
  return Global::state().jsonCurrent();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief GET /debug/overview
////////////////////////////////////////////////////////////////////////////////

string HttpServerImpl::GET_DEBUG_OVERVIEW (const string& name) {
  ArangoState& state = Global::state();

  return "{ \"frameworkId\" : \"" + Global::state().frameworkId() + "\""
       + ", \"frameworkName\" : \"" + Global::frameworkName() + "\""
       + ", \"target\" : " + state.jsonTarget()
       + ", \"plan\" : " + state.jsonPlan()
       + ", \"current\" : " + state.jsonCurrent() + " }";
  
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

  // find correct callback
  if (*ptr == nullptr) {
    ConnectionInfo* conInfo = new ConnectionInfo();

    if (0 == strcmp(method, MHD_HTTP_METHOD_GET)) {
      conInfo->type = GET;

      if (0 == strcmp(url, "/v1/state.json")) {
        conInfo->getMethod = &HttpServerImpl::GET_V1_STATE;
      }
      else if (0 == strcmp(url, "/v1/mode.json")) {
        conInfo->getMethod = &HttpServerImpl::GET_V1_MODE;
      }
      else if (0 == strcmp(url, "/v1/health.json")) {
        conInfo->getMethod = &HttpServerImpl::GET_V1_HEALTH;
      }
      else if (0 == strcmp(url, "/v1/endpoints.json")) {
        conInfo->getMethod = &HttpServerImpl::GET_V1_ENDPOINTS;
      }
      else if (0 == strcmp(url, "/debug/target.json")) {
        conInfo->getMethod = &HttpServerImpl::GET_DEBUG_TARGET;
      }
      else if (0 == strcmp(url, "/debug/plan.json")) {
        conInfo->getMethod = &HttpServerImpl::GET_DEBUG_PLAN;
      }
      else if (0 == strcmp(url, "/debug/current.json")) {
        conInfo->getMethod = &HttpServerImpl::GET_DEBUG_CURRENT;
      }
      else if (0 == strcmp(url, "/debug/overview.json")) {
        conInfo->getMethod = &HttpServerImpl::GET_DEBUG_OVERVIEW;
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

      if (0 == strcmp(url, "/v1/destroy.json")) {
        conInfo->postMethod = &HttpServerImpl::POST_V1_DESTROY;
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

HttpServer::HttpServer () 
  : _daemon(nullptr) {
  _impl = new HttpServerImpl();
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
