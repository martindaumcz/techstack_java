package org.mdaum.techstack.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Objects;
import java.util.Optional;

public record KafkaTopicDto(String name, Integer partitions, Short replicationFactor){

    @JsonIgnore
    public Optional<Integer> optionalPartitions() {
        return Objects.nonNull(partitions()) ? Optional.of(partitions) : Optional.empty();
    }

    @JsonIgnore
    public Optional<Short> optionalReplicationFactor() {
        return Objects.nonNull(replicationFactor()) ? Optional.of(replicationFactor) : Optional.empty();
    }

}
