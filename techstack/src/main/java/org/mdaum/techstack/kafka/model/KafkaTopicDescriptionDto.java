package org.mdaum.techstack.kafka.model;

import org.apache.kafka.common.Uuid;

public record KafkaTopicDescriptionDto(Uuid topicId, String name, boolean internal, int partitions){
}
