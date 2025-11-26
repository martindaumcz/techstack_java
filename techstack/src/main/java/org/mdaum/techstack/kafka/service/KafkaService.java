package org.mdaum.techstack.kafka.service;

import org.mdaum.techstack.kafka.model.*;
import reactor.core.publisher.Flux;

import java.util.List;

public interface KafkaService {

    void produceKafkaMessage(KafkaInputMessageDto kafkaMessage);
    List<KafkaOutputMessageDto> getKafkaMessages(String topic, int maxMessages);
    Flux<KafkaOutputMessageDto> streamKafkaMessagesByTopic(String topic, int maxMessages);
    Flux<KafkaOutputMessageDto> streamKafkaMessagesByConsumerName(String consumerName, int maxMessages);
    void createKafkaTopics(List<KafkaTopicDto> kafkaTopics);
    void deleteKafkaTopics(List<String> topicNames);
    List<KafkaTopicDescriptionDto> getKafkaTopics(boolean internal);
    void createOrAlterConsumer(KafkaConsumerDto kafkaConsumer);
    void deleteConsumer(String consumerName);
    List<KafkaConsumerDto> getKafkaConsumers();
    void createOrAlterConsumerGroup(KafkaConsumerGroupDto kafkaConsumerGroup);
    void deleteConsumerGroup(String groupName);
    List<KafkaConsumerGroupDto> getKafkaConsumerGroups();

}
