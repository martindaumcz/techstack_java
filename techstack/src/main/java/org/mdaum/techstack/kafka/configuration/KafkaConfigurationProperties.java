package org.mdaum.techstack.kafka.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "org.mdaum.techstack.kafka")
public record KafkaConfigurationProperties(String bootstrapServerUrl, @DefaultValue("3") int maxPollRecords) {

}
