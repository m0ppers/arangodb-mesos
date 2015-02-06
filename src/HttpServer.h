#include <microhttpd.h>

class HttpServer {
  public:
    HttpServer ();
    ~HttpServer ();

  public:
    void start (int);
    void stop ();

  private:
    MHD_Daemon* _daemon;
};
