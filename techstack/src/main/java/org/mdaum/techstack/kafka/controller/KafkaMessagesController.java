package org.mdaum.techstack.kafka.controller;

import org.mdaum.techstack.kafka.model.KafkaInputMessageDto;
import org.mdaum.techstack.kafka.model.KafkaOutputMessageDto;
import org.mdaum.techstack.kafka.service.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("kafka/messages")
public class KafkaMessagesController {

    private KafkaService kafkaService;

    @Autowired
    public KafkaMessagesController(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    @PostMapping
    public void produceKafkaMessage(@RequestBody KafkaInputMessageDto kafkaInputMessageDto) {
        kafkaService.produceKafkaMessage(kafkaInputMessageDto);
    }

    @GetMapping(produces = MediaType.APPLICATION_NDJSON_VALUE)
    @RequestMapping("stream/by-topic")
    public Flux<KafkaOutputMessageDto> streamKafkaMessagesByTopic(
            @RequestParam("topicNames") List<String> topicNames,
            @RequestParam(value = "consumerGroup", required = false) String consumerGroup,
            @RequestParam(value = "maxMessages", defaultValue = "0") int maxMessages) {
        return kafkaService.streamKafkaMessagesByTopics(topicNames, Optional.ofNullable(consumerGroup), maxMessages);
    }
}
