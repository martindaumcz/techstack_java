package org.mdaum.techstack.kafka.controller;

import org.mdaum.techstack.kafka.model.KafkaInputMessageDto;
import org.mdaum.techstack.kafka.model.KafkaOutputMessageDto;
import org.mdaum.techstack.kafka.service.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

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

    @GetMapping
    @RequestMapping("stream/by-topic")
    public Flux<KafkaOutputMessageDto> streamKafkaMessagesByTopic(
            @RequestParam("topicName") String topicName,
            @RequestParam(value = "maxMessages", defaultValue = "0") int maxMessages) {
        return kafkaService.streamKafkaMessagesByTopic(topicName, maxMessages);
    }

    @GetMapping
    @RequestMapping("stream/by-consumer")
    public Flux<KafkaOutputMessageDto> streamKafkaMessagesByConsumerName(
            @RequestParam("consumerName") String consumerName,
            @RequestParam(value = "maxMessages", defaultValue = "0") int maxMessages) {
        return kafkaService.streamKafkaMessagesByConsumerName(consumerName, maxMessages);
    }


}
