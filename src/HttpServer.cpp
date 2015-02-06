#include "HttpServer.h"

#include <string.h>

#include <picojson.h>
#include <string>

using namespace std;

#define PAGE "<html><head><title>libmicrohttpd demo</title></head><body>libmicrohttpd demo</body></html>"

static const string get_json () {
  picojson::object v;
  picojson::object inner;
  string val = "tt";
 
  v["aa"] = picojson::value(val);
  v["bb"] = picojson::value(1.66);
  inner["test"] =  picojson::value(true);
  inner["integer"] =  picojson::value(1.0);
  v["inner"] =  picojson::value(inner);
 
  return picojson::value(v).serialize();
}




static int
ahc_echo (void *cls,
          struct MHD_Connection *connection,
          const char *url,
          const char *method,
          const char *version,
          const char *upload_data,
          size_t *upload_data_size,
          void **ptr) {
  static int aptr;
  const char *me = (const char*) cls;
  struct MHD_Response *response;
  int ret;

  if (0 != strcmp (method, "GET"))
    return MHD_NO;              /* unexpected method */

  if (&aptr != *ptr) {
    /* do never respond on first call */
    *ptr = &aptr;
    return MHD_YES;
  }

  *ptr = NULL;                  /* reset when done */

  const string r = get_json();

  response = MHD_create_response_from_buffer (r.length(),
                                              (void *) r.c_str(),
                                              MHD_RESPMEM_MUST_COPY);

  ret = MHD_queue_response (connection, MHD_HTTP_OK, response);
  MHD_destroy_response (response);

  return ret;
}


HttpServer::HttpServer () 
  : _daemon(nullptr) {
}

HttpServer::~HttpServer () {
}

void HttpServer::start (int port) {
  _daemon = MHD_start_daemon (
    MHD_USE_SELECT_INTERNALLY /* | MHD_USE_DEBUG */,
    port,
    nullptr, nullptr,
    &ahc_echo, (void*) PAGE,
    MHD_OPTION_CONNECTION_TIMEOUT, (unsigned int) 120,
    MHD_OPTION_END);
}

void HttpServer::stop () {
  if (_daemon != nullptr) {
    MHD_stop_daemon(_daemon);
    _daemon = nullptr;
  }
}
