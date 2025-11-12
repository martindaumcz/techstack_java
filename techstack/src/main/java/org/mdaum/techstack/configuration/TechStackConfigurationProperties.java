package org.mdaum.techstack.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "org.mdaum.techstack")
public record TechStackConfigurationProperties(String awsRegion) {
}
