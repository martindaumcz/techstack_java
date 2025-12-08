package org.mdaum.techstack.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

@Configuration
public class TechStackConfiguration {

    private static Logger LOGGER = LoggerFactory.getLogger(TechStackConfiguration.class);

    @Bean
    public Region region(TechStackConfigurationProperties properties) {
        return Region.of(properties.awsRegion());
    }

    @Bean
    public AwsCredentialsProvider awsCredentialsProvider(
            @Value("${aws.accessKeyId}") String accessKeyId,
            @Value("${aws.secretAccessKey}") String secretAccessKey
    ) {

        AwsBasicCredentials awsCredentials = AwsBasicCredentials.builder()
                .accessKeyId(accessKeyId)
                .secretAccessKey(secretAccessKey).build();

        return StaticCredentialsProvider.create(awsCredentials);
    }

}
