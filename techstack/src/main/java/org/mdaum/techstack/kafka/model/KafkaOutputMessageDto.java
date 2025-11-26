package org.mdaum.techstack.kafka.model;

public record KafkaOutputMessageDto(String key, String topic, String content, String consumerName, String consumerGroupName) {
}
