package org.mdaum.techstack.s3.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

@Configuration
public class S3ClientConfiguration {

    @Bean
    public S3Client s3Client(
            Region region,
            AwsCredentialsProvider awsCredentialsProvider) {
        return S3Client.builder().region(region).credentialsProvider(awsCredentialsProvider).build();
    }

    @Bean
    public S3AsyncClient s3AsyncClient(
            Region region,
            AwsCredentialsProvider awsCredentialsProvider) {
        return S3AsyncClient.builder()
                .region(region)
                .credentialsProvider(awsCredentialsProvider)
                .build();
    }

}
