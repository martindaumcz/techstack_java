package org.mdaum.techstack.kafka.controller;

import org.mdaum.techstack.kafka.model.KafkaConsumerDto;
import org.mdaum.techstack.kafka.service.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("kafka/admin/consumers")
public class KafkaAdminConsumersController {

    private KafkaService kafkaService;

    @Autowired
    public KafkaAdminConsumersController(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    @PostMapping
    public void createOrAlterConsumer(@RequestBody KafkaConsumerDto kafkaConsumer) {
        kafkaService.createOrAlterConsumer(kafkaConsumer);
    }

    @DeleteMapping
    public void deleteConsumer(@RequestParam("consumerName") String consumerName) {
        kafkaService.deleteConsumer(consumerName);
    }

    @GetMapping
    @ResponseBody
    public List<KafkaConsumerDto> getKafkaConsumers() {
        return kafkaService.getKafkaConsumers();
    }

}
