# Kafka Rebalance Demo

Modules:
- producer-service
- consumer-1
- consumer-2

Prerequisites:
- Java 21+
- Kafka running on localhost:9092
- Topic: orders (3 partitions)

Producer:
kafkaTemplate.send("orders", customerId, message)

Consumers:
Both use group.id=order-group.
Start consumer-1, then consumer-2 to observe rebalance.
Kill consumer-2 to see partitions reassigned.
Enable DEBUG logging for org.apache.kafka.clients.consumer to observe heartbeats.
