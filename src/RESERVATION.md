Reservations
============

Valid states of (role, reservation) pair in the Resource object:

- Unreserved: ("*", None)
- Static reservation: (R, None)
- Dynamic reservation: (R, { principal: <framework_principal> })

Target / Plan / Current
-----------------------

* **Target**: The desired configuration, set by the administrator.

* **Plan**: The configuration need in order to implement the Target,
  set by the framework.

* **Current**: The current situation.

Creating a plan
---------------

The framework creates a Plan for each type (agents, coordinators, db-servers,
secondaries). Each Plan contains a number of **TasksPlanEntry**. A
TasksPlanEntry corresponds to a tasks. A tasks can already be running or in the
process of being started, In this case, a corresponding entry will exist in
Current. If the task is only planned, no such entry exists.

A persistent volume ID is globally unique. Its format is:

    <frameworkId>-<type><number>-<slaveId>

Initially a TasksPlanEntry is not bound to a slave or a persistent volume.

A TasksPlanEntry can either be **bound** to a slave or it can be **unbound**.
Note that using an offer is an asynchronous process. You get an offer. With this
offer, you can try to start a tasks. There is no guaranty that this will
work. The first time an offer is considered suitable for a task, the
TasksPlanEntry is bound to the slave. If we later fail to start with this offer
or if the slave is lost, the TasksPlanEntry will be unbound. There is also an
timeout. If we fail to start a task successfully within a certain time frame,
the TasksPlanEntry is also unbound again.

A TasksPlanEntry can either be **persistent**, that is to say, a persistent
volume has been created. This is only possible for bounded TasksPlanEntry. If a
TasksPlanEntry is unbound from its slave, its persistent volume is considered
lost.

A TasksPlanEntry can either be **satisified**, that is to say, a task in Current
is already created or is in the process of been created. Otherwise it is
**unsatisfied**.

Handling offers for types not requiring reservations
----------------------------------------------------

Coordinators hold no state, therefore they do not need a reservation or a
persistent volume.

If an offer contains enough resource, we try to use them regardless of the
reservation status. No dynamic reservation will be made.

Handling offers for types requiring reservations
------------------------------------------------

If we receive an offer, it can

* be unreserved
* dynamically reserved
* statically reserved

In addition a reserved offer can contain a persistent volume (or not). We only
start a task, if we have a reserved offer (dynamically or statically) with a
persistent volume.

All together there are the following cases, when receiving an offer: the offer

- is unreserved
- is reserved, but does not contain suitable resources
- is dynamically reserved without a persistent volume
- is statically reserved without a persistent volume
- is dynamically reserved with a persistent volume
- is statically reserved with a persistent volume

We make no difference between dynamically and statically reservation, therefore
we end up with three cases: the offer

- is unreserved or does not contain suitable reserved resources
- is reserved without a persistent volume
- is reserved with a persistent volume

### Handling an unreserved offer

(1) there exists a bounded TasksPlanEntry for the slave

(1.1) this TasksPlanEntry is not satisfied

* try to reserve the offer dynamically

(1.2) this TasksPlanEntry is satisfied

* decline the offer

(2) there does not exist a bounded TasksPlanEntry for the slave

(2.1) there exists an unbound TasksPlanEntry

* bind the TasksPlanEntry
* try to reserve the offer dynamically

(2.2) there does not exist an unbound TasksPlanEntry

* decline the offer

### Handling a reserved offer without a persistent volume

(1) there exists a bounded TasksPlanEntry for the slave

(1.1) this TasksPlanEntry is persistent

* try to persist the volume

(1.2) this TasksPlanEntry is not persistent

* set the persistent identifier and make the TasksPlanEntry persistent
* try to persist the volume

(2) there does not exist a bounded TasksPlanEntry for the slave

(2.1) there exists an unbounded TasksPlanEntry

* bind the TasksPlanEntry and make it persistent
* try to persist the volume

(2.2) there does not exists an unbounded TasksPlanEntry

* decline the offer, free the dynamically reserved resources

### Handling a reserved offer with a persistent volume

(1) there exists a bounded TasksPlanEntry for the slave

(1.1) this TasksPlanEntry is persistent

* set the 






* the TasksPlanEntry is bound to the slave
* the offer is first converted into a reserved offer

If the offer is a reserved offer:

* the TasksPlanEntry is switch to persistent
* a persistent volume is created

The decision tree is as follows:

(1) the offer contains a persistent volume

(1.1) a TasksPlanEntry exists for this persistent volume

(1.1.1) the TasksPlanEntry is already satisfied

* something is wrong
* try to revoke the persistent volume from the offer

(1.1.2) the TasksPlanEntry is already bound to a different slave

* something is wrong
* unbind the TasksPlanEntry and rebind it to the new slave
* take this offer

(1.1.3) the TasksPlanEntry is already bound to the same slave

* take this offer

(1.2) no TasksPlanEntry exists for this persistent volume

(1.2.2) there exists a TasksPlanEntry bound to the same slave and satisfied

* something is wrong
* try to revoke 
(1.2.1) there exists a TasksPlanEntry bound to the same slave

* something is wrong
* fix the mapping for persistent volumes to TasksPlanEntry
* take that offer

(1.2.2) there exists 


(2) the offer does not contain a persistent volume

(2.1) the offer is a reserved offer

(2.2) the offer is an unreserved offer
