package org.mdaum.techstack.kafka.configuration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
public class KafkaConfiguration {

    @Bean
    public KafkaAdmin kafkaAdmin(KafkaConfigurationProperties kafkaConfigurationProperties) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigurationProperties.bootstrapServerUrl());
        return new KafkaAdmin(configs);
    }

    @Bean
    public AdminClient adminClient(KafkaConfigurationProperties kafkaConfigurationProperties) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigurationProperties.bootstrapServerUrl());
        return KafkaAdminClient.create(properties);
    }

    @Bean
    public KafkaConsumerBaseConfig kafkaConsumerBaseConfig(KafkaConfigurationProperties kafkaConfigurationProperties) {
        return new KafkaConsumerBaseConfig(
                kafkaConfigurationProperties.bootstrapServerUrl(),
                StringDeserializer.class,
                StringDeserializer.class
        );
    }

    @Bean
    public KafkaProducerBaseConfig kafkaProducerBaseConfig(KafkaConfigurationProperties kafkaConfigurationProperties) {
        return new KafkaProducerBaseConfig(
                kafkaConfigurationProperties.bootstrapServerUrl(),
                StringSerializer.class,
                StringSerializer.class
        );
    }

    @Bean
    public ProducerFactory<String, String> producerFactory(KafkaProducerBaseConfig kafkaProducerBaseConfig) {
        return new DefaultKafkaProducerFactory<>(kafkaProducerBaseConfig.createConfigurationMap());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
