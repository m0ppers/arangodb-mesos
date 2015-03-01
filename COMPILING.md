# Compile Apache Mesos

On the host

    host> docker run -it ubuntu:14.04

Inside the container: Follow the instruction on
[System Requirements](http://mesos.apache.org/gettingstarted/)
for Ubuntu 12.04 to upgrade and prepare the system.

Install git

    container> apt-get install git

and clone mesos

    container> mkdir /home
    container> cd /home
    container> git clone -b reservation https://github.com/mpark/mesos.git
    container> cd mesos
    container> git checkout 56cc41be9b18d3a15f1814327d01506742153305

Follow the instructions
[Building Mesos](http://mesos.apache.org/gettingstarted/)
and also install Mesos.

# Compile the ArangoDB Framework

Inside the container: Install the requirements.

    container> apt-get install libprotobuf-dev libboost-dev libgoogle-glog-dev libmicrohttpd-dev 

Checkout and compile:

    container> cd /home
    container> git clone https://github.com/fceller/arangodb-mesos.git
    container> cd arangodb-mesos
    container> autoreconf
    container> ./configure
    container> make

# Running

    container> IP=....
    container> nohup mesos-master --roles=arangodb --ip=$IP --work_dir=/tmp > mesos-master.log 2>&1 &
    container> nohup mesos-slave --master=$IP:5050 --containerizers=mesos --work_dir=/tmp/slave > mesos-slave.log 2>&1 &

    container> cd /home/arangodb-mesos
    container> ./bin/arangodb-framework --master=$IP:5050 --role=arangodb 

