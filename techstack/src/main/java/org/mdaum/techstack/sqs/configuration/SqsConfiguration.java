package org.mdaum.techstack.sqs.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "org.mdaum.techstack.sqs")
public record SqsConfiguration(String queueName) {

}
