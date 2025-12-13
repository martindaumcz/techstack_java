package org.mdaum.techstack.kafka.controller;

import org.mdaum.techstack.kafka.model.KafkaInputMessageDto;
import org.mdaum.techstack.kafka.model.KafkaOutputMessageDto;
import org.mdaum.techstack.kafka.service.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("kafka/messages")
public class KafkaMessagesController {

    private static Logger LOGGER = LoggerFactory.getLogger(KafkaMessagesController.class);

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
    public Mono<Void> produceKafkaMessageFlux(@RequestBody Flux<KafkaInputMessageDto> kafkaInputMessageDtoFlux) {
        return kafkaService.produceKafkaMessageFlux(kafkaInputMessageDtoFlux);
    }

    @ResponseStatus(HttpStatus.OK)
    @PostMapping(value = "flux-test", consumes = MediaType.APPLICATION_NDJSON_VALUE, produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Flux<KafkaOutputMessageDto> fluxTest(@RequestBody Flux<KafkaInputMessageDto> kafkaInputMessageDtoFlux) {
        return kafkaInputMessageDtoFlux
                .doOnNext(next -> LOGGER.info("consuming test dto {}", next))
                .doOnError(error -> LOGGER.error("Error consuming flux", error))
                .doOnComplete(() -> LOGGER.info("Completed consuming flux"))
                .map(dto -> new KafkaOutputMessageDto(dto.key(), dto.topic(), dto.content(), null, null));
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
