ArangoDB framework for Mesos
============================

This repository contains a C++ program that is a scheduler to create 
a "framework" or "service" on Mesos to run ArangoDB clusters. It is
distributed in binary form in the docker image

    arangodb/arangodb-mesos

which is built using the `Dockerfile` in

    https://github.com/ArangoDB/arangodb-mesos-docker


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
        -e PORT0=8181 \
        -e MESOS_MASTER=zk://master.mesos:2181/mesos \
        -e ARANGODB_ZK=zk://master.mesos:2181/arangodb/arangodb \
        arangodb/arangodb-mesos:latest \
        framework <FURTHER-COMMAND-LINE-OPTIONS>

where "8181" must be a free port on the host,
"zk://master.mesos.2181/mesos" must be replaced by a description of the Mesos
master, usually through a zookeeper URL, and
"zk://master.mesos:2181/arangodb/arangodb" must be replaced by a
zookeeper URL that is specific to the started ArangoDB framework.
What comes instead of "<FURTHER-COMMAND-LINE-OPTIONS>" is copied
verbatim to the command line of the scheduler executable.

The framework scheduler will expose an HTTP/REST API on the given port
listening on all interfaces. For a description of this API see below
in the corresponding section. 

Note that the "--net=host" is necessary such that the Mesos master and
other processes in the system can talk to the framework scheduler.

For a description of all observed environment variables and command line
arguments see the corresponding section below.


Shutting down the service
-------------------------

The service can be shut down by issueing a POST HTTP-request to

    http://<hostname>:<given port>/v1/destroy.json

with an empty body. This shuts down all running instances of processes,
removes the persisted state in zookeeper and then terminates the
scheduler.

Note that the scheduler is resilient to failure. If it is terminated, it
can simply be restarted and will pick up where it was, courtesy of the
persisted state kept in zookeeper and the Apache Mesos infrastructure.
Furthermore, it can survive a failover situation with the Mesos master,
as long as it can find the new one, again via the zookeeper link.


Command line arguments and environment variables
------------------------------------------------

Most options can be specified by a command line argument and by an
environment variable. If both are given, the environment variable takes
precedence.

  - `MESOS_MASTER`, overriding `--master`:

    This is the URL of the Mesos master, usually given as a zookeeper
    URL like `zk://master.mesos:2181/mesos`, do not forget the `/mesos`
    suffix. It is also possible to simply specify `<hostname>:<port>`.

  - `MESOS_AUTHENTICATE`: 
  
    If given, this enable authentication with the Mesos master, in that
    case, `ARANGODB_SECRET` and `ARANGODB_PRINCIPAL` have to be given,
    too.

  - `ARANGODB_SECRET`:
  
    The secret for authentication with the Mesos master.

  - `ARANGODB_ZK`, overriding `--zk`:

    This is the URL to access the persisted state in zookeeper, it will 
    usually be of the form `zk://master.mesos:2181/arangodb/arangodb`.
    Note that no two instances of the ArangoDB framework can have the
    same URL here, unless they have a different framework name (see
    below), because then their persisted state would be mixed up!
    It is possible to leave this empty, in this case the framework will
    use LevelDB and persist the state to a local directory.

  - `ARANGODB_MODE`, overriding `--mode`:

    This can be "cluster" or "standalone", the former is the default,
    which starts a normal ArangoDB cluster. The "standalone" mode simply
    starts a configurable number of independent, single server instances
    of ArangoDB.

  - `ARANGODB_ASYNC_REPLICATION`, overriding `--async_replication`:

    This can be "yes" or "no" or "true" or "false", and "no" is the default,
    which starts an ArangoDB cluster without secondary DBServers doing
    asynchronous replication. If this is set to "true", then for each
    primary DBServer a secondary one is started which replicates the
    data from the primary.

  - `ARANGODB_ROLE`, overriding `--role`:

    This is the Mesos role under which the framework/service is running.
    The default is "*". Note that if you for example specify "slave_public"
    here, then the framework will get offers for the role "slave_public"
    as well as for the default role "*", but not for any others. If you
    leave the default, you only get resource offers for the role "*" and
    none else, so "*" is not a wildcard!

  - `ARANGODB_MINIMAL_RESOURCES_AGENT` overriding `--minimal_resources_agent`:

    Here one can specify minimal resources for an agent process. The
    value has to be a standard mesos resource string like
    "cpus(*):1;mem(*):1024;disk(*):1024". Please specify all three types
    of resources, because otherwise the scheduler will accept offers
    which have 0 in one of the types, which is unhealthy.

  - `ARANGODB_MINIMAL_RESOURCES_DBSERVER` overriding 
    `--minimal_resources_dbserver`:

    Same as for the agent, but for a DBserver.

  - `ARANGODB_MINIMAL_RESOURCES_SECONDARY` overriding 
    `--minimal_resources_secondary`:

    Same as for the agent, but for a secondary.

  - `ARANGODB_MINIMAL_RESOURCES_COORDINATOR` overriding 
    `--minimal_resources_coordinator`:

    Same as for the agent, but for a coordinator.

  - `ARANGODB_NR_DBSERVERS` overriding `--nr_dbservers`:

    Initial number of DBservers to launch. This number can later be
    scaled up, and in future versions also scaled down.

  - `ARANGODB_NR_COORDINATORS` overriding `--nr_coordinators`:

    Initial number of coordinators to launch. Thus number can later be
    scaled up and down.

  - `ARANGODB_PRINCIPAL`, overriding `--principal`:

    The authentication principal for the Mesos master.

  - `ARANGODB_USER`, overriding `--user`:

    This is the user under which executors will be started by the Mesos
    slaves. The default is to use the same user name as the framework
    scheduler is running under.

  - `ARANGODB_FRAMEWORK_NAME`, overriding `--framework_name`:

    This is an ID which must be unique for the framework, in other
    words, if one starts multiple instances of the ArangoDB framework,
    each must have its own framework name. This name goes into the path
    under which the persisted framework state is kept in zookeeper.

  - `ARANGODB_WEBUI`, overriding `--webui`:

    This is the URL of the web UI, if not given explicitly it is put
    together with a guess of the hostname and the given port above.

  - `ARANGODB_HTTP_PORT`, overriding `--http_port`:

    This is the port the HTTP/REST API and web UI will listen to.

  - `ARANGODB_FAILOVER_TIMEOUT`, overriding `--failover_timeout`:

    This specifies the timeout in seconds until an automatic failover
    is done in case of a task failure.

  - `ARANGODB_VOLUME_PATH`, overriding `--volume_path`:

    All instances of ArangoDB will run in Docker containers and will
    mount some directory below the path given here as a volume into the
    container. That is, the actual persisted data and logs of the
    database will reside below this path. Note that before the age of
    persisted volumes this does not make much sense, since new instances
    will usually be started by different Mesos slaves and thus will no
    longer find the old state. Furthermore, nobody will clean up the
    space used after termination! This will all be better when we have
    persistent volumes and Mesos actually controls these resources in a
    better way.

  - `ARANGODB_SECONDARIES_WITH_DBSERVERS`, overriding `--secondaries_with_dbservers`:

    If this boolean value is set to "true", then the secondary DBServers
    will only be started on nodes, where there is already another
    primary server, but not its own master. This is often sensible for
    a good distribution but not always. Note that if you have only
    one DBServer, then you must set this option to "false".


The HTTP/REST API
-----------------

The API supports the following routes:

  - `GET /v1/state.json`: This route produces a JSON document describing
    the overall state of the service, similar to this one:

        {
           "framework_name" : "arangodb",
           "health" : true,
           "volume_path" : "/tmp",
           "role" : "*",
           "mode" : "cluster",
           "master_url" : "http://master0-dcos.westeurope.cloudapp.azure.com:5050/"
        }

  - `GET /v1/mode.json`: This produces a JSON document describing the
    mode of the service, which can either be "cluster" or "standalone"
    as in this:

        {"mode":"cluster"}

  - `GET /v1/endpoints.json`: This produces a JSON document describing
    the endpoints for communication with the ArangoDB cluster, as in
    this:

        {
           "coordinators" : [
              "http://10.0.0.4:2406",
              "http://10.0.0.9:5450"
           ]
        }

    These are the endpoints of the coordinator instances.

  - `GET /v1/health.json`: This is a healthcheck for the service,
    formatted as in:

        {"health":true}

  - `GET /index.html`: On this route the web UI is exposed.

  - `POST /v1/destroy.json`: As mentioned above, sending a POST request
    to this route shuts down the entire service, removes all persisted
    state and terminates the framework scheduler. A JSON of this form is
    returned:

        {"destroy":true}

    When this POST request has come back successfully, the framework 
    scheduler can safely be killed without leaving any state behind.
    Note that there is a 120 second delay before the scheduler process
    actually terminates. This is intentional and is used in the
    Mesosphere DCOS CLI utility to first shut down the service
    gracefully, and then having an opportunity to remove the entry 
    in Marathon for the ArangoDB framework to prevent an automatic
    restart.

  - `GET /debug/target`: This and the three following ones are for
    debugging purposes only. They expose the internal state of the
    framework scheduler. The general concept is the trinity of "target",
    "plan" and "current". The target part describes the state in which 
    the user of the service wishes it to be in. The plan part is what
    the state the scheduler derives from the target, sanitizing settings
    and adding further internal details. The current part is the state
    the service is actually currently in. So the purpose of the
    scheduler is to constantly adjust the plan to any target changes and
    then in turn to see to it that the current situation is according to
    the plan. The latter manipulations are done by communicating with
    the Mesos infrastructure. This setup is flexible and robust and
    leads to a self-healing and self-balancing system.

  - `GET /debug/plan`: See above.

  - `GET /debug/current`: See above.

  - `GET /debug/overview`: See above.


Support and bug reports
-----------------------

The ArangoDB Mesos framework as well as the DCOS subcommand are
supported by ArangoDB GmbH, the company behind ArangoDB. If you get
stuck, need help or have questions, just ask via one of the following
channels:

  - [Google Group](https://groups.google.com/forum/#!forum/arangodb)
  - `hackers@arangodb.com`: developer mailing list of ArangoDB
  - `max@arangodb.com`: direct email to Max Neunhöffer
  - `frank@arangodb.com`: direct email to Frank Celler

Additionally, we track issues, bug reports and questions via the github
issue trackers at

  - [arangodb-dcos](https://github.com/ArangoDB/arangodb-dcos/issues):
    the DCOS subcommand
  - [arangodb-mesos](https://github.com/arangodb/arangodb-mesos/issues):
    the ArangoDB framework/service

