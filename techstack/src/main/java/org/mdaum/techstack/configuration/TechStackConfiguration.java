package org.mdaum.techstack.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;

@Configuration
public class TechStackConfiguration {

    @Bean
    public Region region(TechStackConfigurationProperties properties) {
        return Region.of(properties.awsRegion());
    }

}
