package org.mdaum.techstack.kafka.controller;

import org.mdaum.techstack.kafka.model.KafkaTopicDto;
import org.mdaum.techstack.kafka.model.KafkaTopicDescriptionDto;
import org.mdaum.techstack.kafka.service.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("kafka/admin/topics")
public class KafkaAdminTopicsController {

    private KafkaService kafkaService;

    @Autowired
    public KafkaAdminTopicsController(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    @PostMapping
    public void createKafkaTopics(@RequestBody List<KafkaTopicDto> kafkaTopics) {
        kafkaService.createKafkaTopics(kafkaTopics);
    }

    @DeleteMapping
    public void deleteKafkaTopics(@RequestParam("topicNames") List<String> topicNames) {
        kafkaService.deleteKafkaTopics(topicNames);
    }

    @GetMapping
    @ResponseBody
    public List<KafkaTopicDescriptionDto> getKafkaTopics(@RequestParam("internal") boolean internal) {
        return kafkaService.getKafkaTopics(internal);
    }

}
