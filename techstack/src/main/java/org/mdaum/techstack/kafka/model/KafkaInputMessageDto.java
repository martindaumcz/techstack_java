package org.mdaum.techstack.kafka.model;

public record KafkaInputMessageDto(String key, String topic, String content) {
}
