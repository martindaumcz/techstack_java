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
    @PostMapping(value = "produce-multiple-flux", consumes = MediaType.APPLICATION_NDJSON_VALUE)
    public void produceKafkaMessageFlux(@RequestBody Flux<KafkaInputMessageDto> kafkaInputMessageDtoFlux) {
        kafkaService.produceKafkaMessageFlux(kafkaInputMessageDtoFlux);
    }

    @ResponseStatus(HttpStatus.OK)
    @PostMapping(value = "produce-multiple", consumes = MediaType.APPLICATION_JSON_VALUE)
    public void produceKafkaMessages(@RequestBody List<KafkaInputMessageDto> kafkaInputMessageDtos) {
        kafkaService.produceKafkaMessages(kafkaInputMessageDtos);
    }

    @GetMapping(path="stream/by-topic", produces = MediaType.APPLICATION_NDJSON_VALUE)
    @ResponseBody
    public Flux<KafkaOutputMessageDto> streamKafkaMessagesByTopic(
            @RequestParam("topicNames") List<String> topicNames,
            @RequestParam(value = "consumerGroup", required = false) String consumerGroup,
            @RequestParam(value = "fromBeginning", defaultValue = "false") boolean fromBeginning,
            @RequestParam(value = "pollTimeoutSeconds", defaultValue="5") int pollTimeoutSeconds) {
        return kafkaService.streamKafkaMessagesByTopics(topicNames, Optional.ofNullable(consumerGroup), fromBeginning, pollTimeoutSeconds);
    }

    @GetMapping(path="get/by-topic", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public List<KafkaOutputMessageDto> getKafkaMessagesByTopic(
            @RequestParam("topicNames") List<String> topicNames,
            @RequestParam(value = "consumerGroup", required = false) String consumerGroup,
            @RequestParam(value = "fromBeginning", defaultValue = "false") boolean fromBeginning,
            @RequestParam(value = "pollTimeoutSeconds", defaultValue="5") int pollTimeoutSeconds) {
        return kafkaService.getKafkaMessages(topicNames, Optional.ofNullable(consumerGroup), fromBeginning, pollTimeoutSeconds);
    }
}
