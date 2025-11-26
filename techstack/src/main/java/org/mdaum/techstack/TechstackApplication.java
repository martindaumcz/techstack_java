package org.mdaum.techstack;

import org.mdaum.techstack.configuration.TechStackConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;
import org.springframework.web.reactive.config.EnableWebFlux;

@SpringBootApplication(
        scanBasePackages = {"org.mdaum.techstack"},
        exclude = {DataSourceAutoConfiguration.class})
@Import({TechStackConfiguration.class})
@EnableConfigurationProperties
@ConfigurationPropertiesScan
@EnableWebFlux
public class TechstackApplication {

	public static void main(String[] args) {
		SpringApplication.run(TechstackApplication.class, args);
	}

}
