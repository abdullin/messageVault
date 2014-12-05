Message Vault
=============

Publish-subscribe with replay and batching for Windows Azure.


> This project is heavily inspired by Apache Kafka and all the log shipping projects out there. 

Message Vault is a thin wrapper around capabilities provided by Windows Azure. Producers push messages to the Vault which serves them to consumers. Consumers can replay events from any point in time or chase the tail.

Messages are partitioned by streams. Each stream is an immutable and ordered sequence of messages. All messages in a stream are assigned a unique _offset_ and timestamp. Order of messages in different streams is not guaranteed (within the time drift on Azure).

### Design Trade-offs

Message Vault makes several design trade-offs:

* optimize for **high throughput** over _low latency_;
* optimize for message streams which are gigabytes large;
* prefer **code simplicity** over _complex performance optimizations_;
* **http protocol** instead of binary protocol;
* **rely on Windows Azure** to do all the heavy-lifting (this simplifies code, but couples implementation to Azure);
* **high-availability via master-slave setup** (uptime is limited by Azure uptime, no writes during failover);
* **no channel encryption** (if needed, use SSL with Azure Load Balancer or your load balancer);
* **no authorization schemes** (if needed, configure your load balancer or add a proxy on top);
* **implemented in imperative C#** (.NET runtime is heavy, but Windows Azure is optimized for it);
* **client library is intentionally simple** (view projections and even checkpoints are outside the scope);
* **each stream is a sepate page blob** (they can grow to 1TB out-of-the-box, having thousands of streams isn't a good idea).

> Message Vault is not an "event store", it is not designed for "event sourcing with aggregates" (you need [NEventStore](http://neventstore.org/) or [EventStore](http://geteventstore.com/))
