package org.mdaum.techstack.kafka.model;

import java.util.List;

public record KafkaConsumerDto(String name, String consumerGroup, List<String> topics) {
}