# Kafka Producer Demo with Spring Boot

## Overview

This project demonstrates how a Spring Boot application sends messages to Apache Kafka and how Kafka internally stores those messages inside partitions.

The goal of this project is to understand the complete producer flow, partitioning, offsets, acknowledgements, and message visualization using AKHQ.

---

# Technologies Used

* Java 21+
* Spring Boot
* Spring Kafka
* Apache Kafka (Docker)
* AKHQ
* Maven

---

# Project Architecture

```
                +---------------------+
                |  Spring Boot App    |
                |  Producer Service   |
                +----------+----------+
                           |
                           | KafkaTemplate.send()
                           |
                           v
                  +------------------+
                  | Kafka Producer   |
                  +------------------+
                           |
                    Serialize Key & Value
                           |
                           v
                  Partition Calculation
                           |
                           v
               +------------------------+
               | Kafka Broker           |
               | Topic : orders         |
               +------------------------+
                |      |         |
                |      |         |
           Partition0 Partition1 Partition2
```

---

# Step 1 : Start Kafka using Docker

Start Kafka Broker.

```
docker compose up -d
```

Verify Kafka is running.

```
docker ps
```

Output

```
kafka
0.0.0.0:9092->9092/tcp
```

Kafka Broker is now listening on

```
localhost:9092
```

---

# Step 2 : Verify Kafka

Check broker logs.

```
docker logs kafka
```

Kafka should print

```
Kafka Server started
```

This confirms the broker is healthy.

---

# Step 3 : Start AKHQ

AKHQ is a Kafka Web UI.

It allows us to

* View Topics
* View Partitions
* View Consumer Groups
* View Offsets
* View Messages

Start AKHQ

```
java \
-Dmicronaut.config.files=/Users/neelu/akhq/application.yml \
-jar akhq-0.27.0-all.jar
```

Configuration

```yaml
micronaut:
  server:
    port:30081

akhq:
  connections:
    local:
      properties:
        bootstrap.servers: localhost:9092
```

Open

```
http://localhost:30081
```

---

# Step 4 : Create Topic

Spring Boot automatically creates the topic.

```java
@Bean
public NewTopic ordersTopic() {
    return TopicBuilder
            .name("orders")
            .partitions(3)
            .replicas(1)
            .build();
}
```

Topic

```
orders
```

Partitions

```
3
```

Replication Factor

```
1
```

---

# Step 5 : Producer API

Controller

```java
@GetMapping("/{customerId}")
public String send(@PathVariable String customerId){

    kafkaTemplate.send(
            "orders",
            customerId,
            "Order for " + customerId
    );

    return "Sent";
}
```

Request

```
GET
http://localhost:8080/orders/customer1
```

Response

```
Sent
```

---

# Step 6 : What Happens Internally?

The moment the API is called

```
GET /orders/customer1
```

Spring executes

```
KafkaTemplate.send(...)
```

Internally Kafka Producer performs the following steps.

```
Producer
      |
      |
      v
Key Serializer
      |
Value Serializer
      |
Partition Calculation
      |
Leader Partition
      |
Write Message
      |
Offset Generated
      |
ACK Returned
```

---

# Step 7 : Serialization

Producer converts Java objects into bytes.

Key

```
customer1
```

becomes

```
byte[]
```

Value

```
Order for customer1
```

also becomes

```
byte[]
```

because Kafka stores bytes.

---

# Step 8 : Partition Calculation

Kafka now decides

```
Which partition should store this message?
```

Since we supplied a key

```
customer1
```

Kafka uses the Default Partitioner.

Internally

```
partition =
murmur2(key)
%
numberOfPartitions
```

Example

```
Topic Partitions = 3

customer1

↓

murmur2(customer1)

↓

Positive Hash

↓

hash % 3

↓

Partition 2
```

That is exactly why AKHQ showed

```
customer1

Partition = 2
```

---

# Step 9 : Why customer2 goes to another partition?

Request

```
GET /orders/customer2
```

Kafka again calculates

```
murmur2(customer2)
%
3
```

Suppose

```
Result = 1
```

Kafka stores the message inside

```
Partition 1
```

AKHQ confirms

```
customer2

Partition = 1
```

---

# Why Same Key Always Goes To Same Partition?

Suppose

```
customer1
customer1
customer1
```

Kafka always calculates

```
murmur2(customer1)
```

which always produces the same hash.

Therefore

```
Partition = 2
```

always.

This preserves message ordering.

```
Offset 0

Offset 1

Offset 2

Offset 3
```

---

# Step 10 : Offset

Offset is simply the sequence number of messages inside a partition.

Example

Partition 2

```
Offset 0

Order1

Offset 1

Order2

Offset 2

Order3
```

Offsets are unique **within a partition**, not across the topic.

---

# Step 11 : Acknowledgement

After writing the message

Leader Partition sends acknowledgement.

Depending on Producer configuration

```
acks=0

No acknowledgement
```

```
acks=1

Leader acknowledges
```

```
acks=all

Leader + ISR replicas acknowledge
```

Only after receiving ACK does the producer consider the send successful.

---

# Step 12 : Verify in AKHQ

Navigate to

```
Topics

↓

orders

↓

Data
```

Example

```
Key

customer1

Partition

2

Offset

0

Value

Order for customer1
```

---

# Flow Diagram

```
REST API

        |

        v

ProducerController

        |

        v

KafkaTemplate.send()

        |

        v

Serializer

        |

        v

Default Partitioner

        |

        v

Partition 2

        |

        v

Kafka Broker

        |

        v

Offset Generated

        |

        v

ACK Returned

        |

        v

AKHQ Displays Message
```

---

# Commands Used

Start Kafka

```
docker compose up -d
```

Verify Kafka

```
docker ps
```

Start AKHQ

```
java -Dmicronaut.config.files=/Users/neelu/akhq/application.yml \
     -jar akhq-0.27.0-all.jar
```

Call Producer

```
GET http://localhost:8080/orders/customer1
```

Open AKHQ

```
http://localhost:30081
```

---

# Kafka Consumer

## Consumer Service

The consumer continuously polls Kafka for new messages.

```java
@KafkaListener(
        topics = "orders",
        groupId = "order-group"
)
public void consume(String message,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset) {

    System.out.println(
        "Received : " + message +
        " Partition : " + partition +
        " Offset : " + offset
    );
}
```

---

# Consumer Flow

```
Producer
      |
      v
Kafka Broker
      |
      v
Topic (orders)
      |
      v
Consumer Group
      |
      v
Consumer Instance
```

Unlike a Producer, a Consumer never receives pushed messages.

The Consumer continuously asks Kafka

```
Do you have any new messages?
```

This process is called

```
Polling
```

Internally

```
Consumer

↓

poll()

↓

Broker

↓

New Records

↓

Consumer
```

---

# What is a Consumer Group?

Suppose we have

```
Topic : orders

Partitions

P0
P1
P2
```

Now we start

```
Consumer-1

Consumer-2
```

Both belong to

```
order-group
```

Kafka automatically distributes partitions.

Example

```
Consumer-1

Partition 0

Partition 2

Consumer-2

Partition 1
```

This is called

```
Partition Assignment
```

Each partition is consumed by only one consumer inside the same consumer group.

This guarantees that a message is processed only once by the group.

---

# What Happens If We Have More Consumers?

Example

```
Partitions = 3

Consumers = 5
```

Assignment

```
Consumer1 -> P0

Consumer2 -> P1

Consumer3 -> P2

Consumer4 -> Idle

Consumer5 -> Idle
```

Extra consumers remain idle because Kafka cannot assign the same partition to multiple consumers within the same group.

---

# What is Heartbeat?

Every consumer periodically sends a heartbeat to Kafka.

Heartbeat simply means

```
"I'm alive."
```

Internally

```
Consumer

↓

Heartbeat

↓

Group Coordinator
```

Kafka checks these heartbeats continuously.

If heartbeats stop arriving within the configured session timeout, Kafka assumes that the consumer has failed.

Default heartbeat interval is usually around **3 seconds**, and the default session timeout is typically **45 seconds** (depending on client configuration).

---

# Group Coordinator

Every consumer group has one broker acting as the **Group Coordinator**.

Responsibilities

* Register consumers
* Receive heartbeats
* Detect failed consumers
* Trigger rebalancing
* Commit offsets

---

# What is Rebalancing?

Suppose

Initially

```
Consumer1 -> Partition0

Consumer2 -> Partition1

Consumer3 -> Partition2
```

Now Consumer2 crashes.

Heartbeats stop.

Kafka waits until the session timeout expires.

The Group Coordinator detects the missing heartbeat.

Kafka immediately starts

```
Rebalancing
```

New assignment

```
Consumer1

Partition0

Partition1

Consumer3

Partition2
```

No partition remains unassigned.

---

# What Triggers Rebalancing?

Rebalancing occurs when

* A new consumer joins the group
* A consumer leaves the group
* A consumer crashes
* Heartbeats stop
* Number of partitions changes (for example, partitions are added)

---

# What is Offset?

Every message inside a partition has a unique offset.

Example

Partition 2

```
Offset 0

Order1

Offset 1

Order2

Offset 2

Order3
```

Consumers remember the last processed offset.

Next poll starts from the next offset.

---

# Offset Commit

After successfully processing a message, the consumer commits its offset.

Example

```
Message

↓

Process

↓

Commit Offset

↓

Next Message
```

If the consumer crashes before committing the offset, Kafka can deliver the same message again when the consumer restarts.

This is why Kafka provides **at-least-once delivery** by default.

---

# Consumer Poll Cycle

```
Consumer

↓

poll()

↓

Receive Messages

↓

Process

↓

Commit Offset

↓

Heartbeat

↓

poll() again
```

This loop continues as long as the consumer is running.

---

# AKHQ Demonstration

Using AKHQ we can observe

* Topics
* Partitions
* Producer Messages
* Consumer Groups
* Offsets
* Consumer Lag
* Partition Assignment

After sending

```
GET /orders/customer1
```

AKHQ displayed

```
Key

customer1

Partition

2

Offset

0
```

Starting multiple consumers shows partition assignments under the **Consumer Groups** section.

Stopping one consumer demonstrates **heartbeat timeout** followed by **automatic rebalancing**.

---

# Interview Questions Covered

* What is a Consumer Group?
* What is Polling?
* Why does Kafka use poll() instead of push?
* What is Heartbeat?
* What is Group Coordinator?
* What triggers Rebalancing?
* Why can only one consumer read one partition within a group?
* What is Offset?
* What is Offset Commit?
* What happens when a consumer crashes?
* What is Consumer Lag?
* Explain the complete consumer lifecycle.

