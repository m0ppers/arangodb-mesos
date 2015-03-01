Directory structure:

    <path>/arangodb-mesos
    <path>/mesos


Commands:

    $ cd mesos/build
    $ make install  // Needed to install the libmesos library it seems.


    sudo apt-get install libprotobuf-dev libboost-dev libgoogle-glog-dev libmicro-httpd-dev 

 

    $ cd arangodb-mesos
    $ autoreconf
    $ ./configure CXXFLAGS="-I ../mesos/build/3rdparty/libprocess/3rdparty/picojson-4f93734"  // For #include <picojson.h>
    $ make
