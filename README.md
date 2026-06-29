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

# Interview Questions Covered

* What happens when a producer sends a message?
* How does Kafka calculate partitions?
* Why do messages with the same key go to the same partition?
* What is Murmur2 hashing?
* What is Offset?
* What is ACK?
* What is Serialization?
* Why is ordering guaranteed only within a partition?
* How are topics created?
* How does Spring Boot integrate with Kafka?

---

# Next Steps

* Kafka Consumer
* Consumer Groups
* Heartbeats
* Rebalancing
* Offset Commit (Auto & Manual)
* Retry Mechanism
* Dead Letter Topic (DLT)
* Idempotent Producer
* Transactions
* Exactly Once Semantics (EOS)
