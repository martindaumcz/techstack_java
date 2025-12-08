package org.mdaum.techstack.kafka.controller;

import org.mdaum.techstack.kafka.model.KafkaInputMessageDto;
import org.mdaum.techstack.kafka.model.KafkaOutputMessageDto;
import org.mdaum.techstack.kafka.service.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
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

    @ResponseStatus(HttpStatus.OK)
    @PostMapping("produce-single")
    public void produceKafkaMessage(@RequestBody KafkaInputMessageDto kafkaInputMessageDto) {
        kafkaService.produceKafkaMessage(kafkaInputMessageDto);
    }

    @ResponseStatus(HttpStatus.OK)
    @PostMapping(value = "produce-multiple", consumes = MediaType.APPLICATION_NDJSON_VALUE)
    public void produceKafkaMessages(@RequestBody Flux<KafkaInputMessageDto> kafkaInputMessageDtoFlux) {
        kafkaService.produceKafkaMessages(kafkaInputMessageDtoFlux);
    }

    @GetMapping(path="stream/by-topic", produces = MediaType.APPLICATION_NDJSON_VALUE)
    @ResponseBody
    public Flux<KafkaOutputMessageDto> streamKafkaMessagesByTopic(
            @RequestParam("topicNames") List<String> topicNames,
            @RequestParam(value = "consumerGroup", required = false) String consumerGroup,
            @RequestParam(value = "maxMessages", defaultValue = "0") int maxMessages) {
        return kafkaService.streamKafkaMessagesByTopics(topicNames, Optional.ofNullable(consumerGroup), maxMessages);
    }
}
