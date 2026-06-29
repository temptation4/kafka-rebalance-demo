# Kafka Producer & Consumer Demo with Spring Boot

## Overview

This project demonstrates how to build a Kafka Producer and Consumer using Spring Boot. It explains how Kafka stores messages in partitions, assigns offsets, manages consumer groups, detects failed consumers using heartbeats, and automatically performs rebalancing.

## Tech Stack

* Java 21
* Spring Boot
* Spring Kafka
* Apache Kafka
* Docker
* AKHQ
* Maven

---

# Architecture

```text
Producer Service
       |
       v
Kafka Broker (Topic : orders)
       |
  -------------------------
  |          |           |
 P0         P1          P2
       |
Consumer Group (order-group)
       |
Consumer-1    Consumer-2
```

---

# Setup

### 1. Start Kafka

```bash
docker compose up -d
```

Verify

```bash
docker ps
```

Kafka runs on:

```text
localhost:9092
```

---

### 2. Start AKHQ

```bash
java -Dmicronaut.config.files=/Users/neelu/akhq/application.yml \
     -jar akhq-0.27.0-all.jar
```

Configuration

```yaml
micronaut:
  server:
    port: 30081

akhq:
  connections:
    local:
      properties:
        bootstrap.servers: localhost:9092
```

Open

```text
http://localhost:30081
```

---

## Create Topic

```java
@Bean
public NewTopic ordersTopic() {
    return TopicBuilder.name("orders")
            .partitions(3)
            .replicas(1)
            .build();
}
```

---

# Producer API

```http
GET /orders/{customerId}
```

Example

```text
GET http://localhost:8080/orders/customer1
```

Internally

```text
REST API
    ↓
KafkaTemplate.send()
    ↓
Serialize Key & Value
    ↓
Calculate Partition
    ↓
Store Message
    ↓
Generate Offset
    ↓
Return ACK
```

---

# How Kafka Chooses a Partition

Since a **key** is provided, Kafka uses the default partitioner.

```text
Partition = murmur2(key) % numberOfPartitions
```

Example

```text
customer1 → Partition 2
customer2 → Partition 1
```

The same key always goes to the same partition, ensuring message ordering.

---

# Offset

Every message inside a partition receives a unique offset.

```text
Partition 2

Offset 0
Offset 1
Offset 2
```

Offsets are unique only within a partition.

---

# Consumer

Consumer continuously polls Kafka for new messages.

```text
Consumer
    ↓
poll()
    ↓
Receive Records
    ↓
Process
    ↓
Commit Offset
    ↓
poll() again
```

---

# Consumer Groups

Consumers in the same group share partitions.

Example

```text
Partitions : 3

Consumer-1 → P0, P2
Consumer-2 → P1
```

Only one consumer can consume a partition within the same consumer group.

---

# Heartbeat

Consumers periodically send heartbeats to the Group Coordinator.

```text
Consumer
    ↓
Heartbeat
    ↓
Group Coordinator
```

If heartbeats stop, Kafka assumes the consumer has failed.

---

# Rebalancing

Rebalancing occurs when:

* A consumer joins
* A consumer leaves
* A consumer crashes
* Heartbeats stop
* Partitions are added

Example

```text
Before

Consumer1 → P0
Consumer2 → P1
Consumer3 → P2

Consumer2 crashes

After Rebalance

Consumer1 → P0, P1
Consumer3 → P2
```

Kafka automatically redistributes partitions so no partition remains idle.

---

# Verify Using AKHQ

Open

```text
Topics → orders → Data
```

Example

```text
Key        : customer1
Partition  : 2
Offset     : 0
Value      : Order for customer1
```

Consumer Groups page displays:

* Active consumers
* Partition assignment
* Consumer lag
* Offsets

---

# Commands

Start Kafka

```bash
docker compose up -d
```

Producer API

```text
GET http://localhost:8080/orders/customer1
```

Open AKHQ

```text
http://localhost:30081
```

---

# Concepts Covered

* Kafka Producer
* Kafka Consumer
* Topics
* Partitions
* Offsets
* Serialization
* ACK (`0`, `1`, `all`)
* Consumer Groups
* Heartbeats
* Group Coordinator
* Rebalancing
* Polling
* Partition Assignment
* Murmur2 Hashing
* AKHQ Monitoring

---

# Future Enhancements

* Manual Offset Commit
* Retry Mechanism
* Dead Letter Topic (DLT)
* Idempotent Producer
* Transactions
* Exactly Once Semantics (EOS)
