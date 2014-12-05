Message Vault
=============

Publish-subscribe with replay and batching for Windows Azure.


> This project is heavily inspired by Apache Kafka and all the log shipping projects out there. 



Message Vault is a thin wrapper around capabilities provided by Windows Azure. Producers push messages to Message Vault which serves them to consumers.

Messages are partitioned by streams. Each stream is an immutable and ordered sequence of messages. All messages in a stream are assigned a unique _offset_ and timestamp.



### Design Trade-offs

Message Vault makes several design trade-offs:

* optimize for _high throughput_ over _low latency_;
* prefer _code simplicity_ over _complex performance optimizations_;
* _http protocol_ instead of _binary protocol_;
* _rely on Windows Azure_ to do all the heavy-lifting (this simplifies code, but couples implementation to Azure);
* _high-availability via master-slave setup_ (uptime is limited by Azure uptime, no writes during failover);
* _no channel encryption_ (if needed, use SSL with Azure Load Balancer or your load balancer);
* _no authorization schemes_ (if needed, configure your load balancer or add a proxy on top);
* _implemented in imperative C#_ (runtime is heavy, but Windows Azure is optimized for it).
