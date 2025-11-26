package org.mdaum.techstack.kafka.configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;

public record KafkaConsumerBaseConfig(String bootstrapServerUrl, Class keyDeserializer, Class valueDeserializer) {

    public Map<String, Object> createConfigurationMap() {
        Map<String, Object> consumerBaseConfig = new HashMap<>();
        consumerBaseConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerUrl());
        consumerBaseConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer());
        consumerBaseConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer());
        return consumerBaseConfig;
    }

}
