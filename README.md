ArangoDB framework for Mesos
============================

Introduction
------------

Some Blabla about using Apache Mesos to deploy an ArangoDB cluster.
Some explanation about the structure of an ArangoDB cluster.


Installation
------------

The framework scheduler as well as all executables of the database
reside in a single Docker image:

    arangodb/arangodb-mesos:latest

To start the framework on a Mesos cluster with `docker` service, simply
do

    docker run --net=host arangodb/arangodb-mesos:latest \
        -e HOST=<host-address-or-name-of-framework> \
        -e PORT0=<port-of-framework> \
        -e MESOS_MASTER=<address-and-port-of-mesos-master> \
        framework <FURTHER-COMMAND-LINE-OPTIONS>

Where `<host-address-or-name-of-framework>` is an IP-address or name
that is reachable throughout the cluster and refers to an interface on
the local machine. Likewise, `<port-of-framework>` must be a free port
on the local machine. The framework scheduler will put together its own
webui from these two pieces of information and run an HTTP service on
that address and port. For a list of possible command line options and
environment variables see the next section.


Command line arguments and environment variables
------------------------------------------------

All options can be specified by a command line argument and by an
environment variable. If both are given, the environment variable takes
precedence.

       MESOS_MASTER         overrides '--master'\n"
       MESOS_AUTHENTICATE   enable authentication\n"
       ARANGODB_SECRET      secret for authentication\n"
       ARANGODB_PRINCIPAL   overrides '--principal'\n"
       ARANGODB_HTTP_PORT   overrides '--http_port'\n"
       ARANGODB_ROLE        overrides '--role'\n"
       ARANGODB_USER        overrides '--user'\n"
       ARANGODB_VOLUME_PATH overrides '--volume_path'\n"
       ARANGODB_WEBUI       overrides '--webui'\n"
       ARANGODB_ZK          overrides '--zk'\n"
       ARANGODB_MODE        overrides '--mode'\n"
       ARANGODB_FRAMEWORK_NAME\n" overrides '--framework_name'\n"
       ARANGODB_MINIMAL_RESOURCES_AGENT\n" overrides '--minimal_resources_agent'\n"
       ARANGODB_MINIMAL_RESOURCES_DBSERVER\n" overrides '--minimal_resources_dbserver'\n"
       ARANGODB_MINIMAL_RESOURCES_COORDINATOR\n" overrides '--minimal_resources_coordinator'\n"
       ARANGODB_NR_AGENTS   overrides '--nr_agents'\n"
       ARANGODB_NR_DBSERVERS\n" overrides '--nr_dbservers'\n"
       ARANGODB_NR_COORDINATORS\n" overrides '--nr_coordinators'\n"

