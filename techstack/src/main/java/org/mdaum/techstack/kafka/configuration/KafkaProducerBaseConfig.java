package org.mdaum.techstack.kafka.configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;

public record KafkaProducerBaseConfig(String bootstrapServerUrl, Class keySerializer, Class valueSerializer) {

    public Map<String, Object> createConfigurationMap() {
        Map<String, Object> producerBaseConfig = new HashMap<>();
        producerBaseConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerUrl());
        producerBaseConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer());
        producerBaseConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer());
        return producerBaseConfig;
    }

}
