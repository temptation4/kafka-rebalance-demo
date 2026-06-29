package com.example.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
public class ProducerController {
    private final KafkaTemplate<String,String> kafkaTemplate;
    public ProducerController(KafkaTemplate<String,String> kafkaTemplate){
        this.kafkaTemplate=kafkaTemplate;
    }

    @GetMapping("/{customerId}")
    public String send(@PathVariable String customerId){
        kafkaTemplate.send("orders", customerId, "Order for "+customerId);
        return "Sent";
    }
}
