package com.example.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    @KafkaListener(topics="orders",groupId="order-group")
    public void consume(String message,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition)
            throws Exception{
        System.out.println("Received from partition "+partition+" : "+message);
        Thread.sleep(8000);
    }
}
