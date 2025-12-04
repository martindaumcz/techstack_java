package org.mdaum.techstack.kafka.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties(prefix = "org.mdaum.techstack.kafka")
public record KafkaConfigurationProperties(String bootstrapServerUrl) {

}
