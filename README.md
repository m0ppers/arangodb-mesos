ArangoDB framework for Mesos
============================

This repository contains a C++ program that is a scheduler to create 
a "framework" or "service" on Mesos to run ArangoDB clusters. It is
distributed in binary form in the docker image

    arangodb/arangodb-mesos


Introduction
------------

ArangoDB is a distributed, multi-model database featuring JSON
documents, graphs and key/value pairs. It has a unifying query language
that allows to mix all three data models and supports joins and
transactions. As a distributed application, it is a very natural wish to
be able to deploy an ArangoDB cluster easily on an Apache Mesos cluster,
such that one can reap the benefits of better resource usage that Mesos
promises, and run ArangoDB alongside other distributed applications.
This framework makes this wish come true.

An ArangoDB cluster consists of different processes. As a central
fault-tolerant configuration store we use `etcd`, we call these
processes "agents" and the whole `etcd`-cluster "agency". Usually one
will deploy three agents to allow for a failure (or indeed upgrade) of
one of them without service interruption. The second type of processes
are the "DBservers", which are ArangoDB instances that actually store
data. No client should ever (need to) contact the DBservers directly.
The third type of processes are the "coordinators". They are
ArangoDB instances as well, they are the ones that receive client
requests, export the usual ArangoDB HTTP/REST API, know the structure of
the ArangoDB cluster (via the agency), and organise the distribution
of the queries to the actual DBservers.

The user does not actually have to know much about this structure,
however, since one can scale the coordinator layer independently from
the DBserver layer, it is useful to understand this structure. As a rule
of thumb, scale the DBserver layer up to get more storage space and
scale the coordinator layer up if the bottleneck is CPU power for
queries or Foxx apps (which run on the coordinators).


Installation
------------

The framework scheduler as well as all executables of the database
reside in a single Docker image:

    arangodb/arangodb-mesos:latest

To start the framework on a Mesos cluster with `docker` service, simply
do

    docker run --net=host \
        -e HOST=<host-address-or-name-of-framework> \
        -e PORT0=<port-of-framework> \
        -e MESOS_MASTER=<address-and-port-of-mesos-master> \
        arangodb/arangodb-mesos:latest \
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

