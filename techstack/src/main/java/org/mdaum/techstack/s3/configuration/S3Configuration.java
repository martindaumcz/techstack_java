package org.mdaum.techstack.s3.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "org.mdaum.techstack.s3")
public record S3Configuration(String bucketName, String prefix) {
}
