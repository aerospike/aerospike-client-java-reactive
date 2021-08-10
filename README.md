# Aerospike Reactive Java Client

![Build](https://github.com/aerospike/aerospike-client-java-reactive/workflows/Build/badge.svg)

This repository provides reactive extensions for the Aerospike Java Client.

## Reactor
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.aerospike/aerospike-reactor-client/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.aerospike/aerospike-reactor-client/)
[![javadoc](https://javadoc.io/badge2/com.aerospike/aerospike-reactor-client/javadoc.svg)](https://javadoc.io/doc/com.aerospike/aerospike-reactor-client)  

### Package
[Aerospike Reactor Client](./reactor-client) - a [Reactor](https://projectreactor.io/) interface extension for the Aerospike Java Client.

### Usage
The documentation for this project can be found on [javadoc.io](https://javadoc.io/doc/com.aerospike/aerospike-reactor-client).

This project can be added through [Maven Central](https://maven-badges.herokuapp.com/maven-central/com.aerospike/aerospike-reactor-client/).

Create a Reactor client
```java
// Create a ClientPolicy.
ClientPolicy policy = new ClientPolicy();
// Set event loops to use in asynchronous commands.
policy.eventLoops = new NioEventLoops(1);

// Instantiate an AerospikeReactorClient which embeds an AerospikeClient.
AerospikeClient client = new AerospikeClient(policy, "localhost", 3000);
AerospikeReactorClient reactorClient = new AerospikeReactorClient(client);
```

Write record bin(s) using the Reactor client
```java
Key key = new Key("test", null, "k1");
reactorClient.put(key, new Bin("bin1", 100)).block();
```

Get the record using the Reactor client
```java
reactorClient.get(key).block();
```

Delete the record using the Reactor client
```java
reactorClient.delete(key).block();
```

Check the tests for more usage examples.

### Prerequisites
* Java 8 or greater.
* Maven 3.0 or greater.

### Build
```sh
mvn clean package
```
