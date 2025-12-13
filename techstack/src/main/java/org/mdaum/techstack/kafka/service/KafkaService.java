package org.mdaum.techstack.kafka.service;

import org.mdaum.techstack.kafka.model.*;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Optional;

public interface KafkaService {

    void produceKafkaMessage(KafkaInputMessageDto kafkaMessage);
    void produceKafkaMessages(List<KafkaInputMessageDto> kafkaInputMessageDtos);
    reactor.core.publisher.Mono<Void> produceKafkaMessageFlux(Flux<KafkaInputMessageDto> kafkaInputMessageDtoFlux);
    List<KafkaOutputMessageDto> getKafkaMessages(List<String> topics, Optional<String> consumerGroup, boolean fromBeginning, int pollTimeoutSeconds);
    Flux<KafkaOutputMessageDto> streamKafkaMessagesByTopics(List<String> topic, Optional<String> consumerGroup, boolean fromBeginning, int pollTimeoutSeconds);
    void createKafkaTopics(List<KafkaTopicDto> kafkaTopics);
    void deleteKafkaTopics(List<String> topicNames);
    List<KafkaTopicDescriptionDto> getKafkaTopics(boolean internal);
}
