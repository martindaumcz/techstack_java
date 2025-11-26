package org.mdaum.techstack.kafka.model;

import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;

public record KafkaConsumerAndFluxDto(KafkaConsumerDto kafkaConsumer, Flux<ReceiverRecord<Object, Object>> flux) {

}
