package org.mdaum.techstack.kafka.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.util.Strings;
import org.mdaum.techstack.kafka.configuration.KafkaConsumerBaseConfig;
import org.mdaum.techstack.kafka.configuration.KafkaProducerBaseConfig;
import org.mdaum.techstack.kafka.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Component
public class KafkaServiceImpl implements KafkaService{

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaServiceImpl.class);

    private KafkaConsumerBaseConfig kafkaConsumerBaseConfig;
    private KafkaProducerBaseConfig kafkaProducerBaseConfig;
    private AdminClient kafkaAdminClient;
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public KafkaServiceImpl(
            KafkaConsumerBaseConfig kafkaConsumerBaseConfig,
            AdminClient kafkaAdminClient,
            KafkaTemplate<String, String> kafkaTemplate,
            KafkaProducerBaseConfig kafkaProducerBaseConfig) {
        this.kafkaConsumerBaseConfig = kafkaConsumerBaseConfig;
        this.kafkaAdminClient = kafkaAdminClient;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProducerBaseConfig = kafkaProducerBaseConfig;
    }

    @Override
    public void produceKafkaMessage(KafkaInputMessageDto kafkaMessage) {

        SendResult<String, String> sendResult;

        try {
            if (Strings.isEmpty(kafkaMessage.key())) {
                sendResult = kafkaTemplate.send(kafkaMessage.topic(), kafkaMessage.content()).get();
            } else {
                sendResult = kafkaTemplate.send(kafkaMessage.topic(), kafkaMessage.key(), kafkaMessage.content()).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void produceKafkaMessages(Flux<KafkaInputMessageDto> kafkaInputMessageDtoFlux) {
        SenderOptions<String, Object> senderOptions = SenderOptions.create(kafkaProducerBaseConfig.createConfigurationMap());

        KafkaSender<String, Object> kafkaSender = KafkaSender.create(senderOptions);

        kafkaSender.send(kafkaInputMessageDtoFlux
                        .doOnNext(next -> LOGGER.info("Sending input message for topic [{}]: {}:{}", next.topic(), next.key(), next.content()))
                        .map(dto -> SenderRecord.create(
                                new ProducerRecord<>(dto.topic(),
                                        null,
                                        System.currentTimeMillis(),
                                        dto.key(),
                                        dto.content()),
                                dto.key()
                        )))
                .doOnNext(next -> LOGGER.info("Sent input message to topic: {}", next.recordMetadata().topic()))
                .subscribe();
    }

    @Override
    public List<KafkaOutputMessageDto> getKafkaMessages(String topic, int maxMessages) {
        return List.of();
    }

    @Override
    public Flux<KafkaOutputMessageDto> streamKafkaMessagesByTopics(List<String> topics, Optional<String> consumerGroup, int maxMessages) {

        Map<String, Object> consumerBaseConfigMap = kafkaConsumerBaseConfig.createConfigurationMap();
        consumerBaseConfigMap.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                consumerGroup.orElse(UUID.randomUUID().toString()));

        ReceiverOptions<Object, Object> receiverOptions = ReceiverOptions.create(consumerBaseConfigMap)
                .subscription(topics);
        KafkaReceiver<Object, Object> kafkaReceiver = KafkaReceiver.create(receiverOptions);
        Flux<KafkaOutputMessageDto> outputMsgFlux = kafkaReceiver.receive()
                .map(record -> new KafkaOutputMessageDto(
                        record.key().toString(),
                        record.topic(),
                        record.value().toString(),
                        null,
                        null
                )).doOnNext(kafkaOutputMessageDto ->
                        LOGGER.info("Consumed message {}:{} from topic {}",
                                kafkaOutputMessageDto.key(),
                                kafkaOutputMessageDto.content(),
                                kafkaOutputMessageDto.topic()));

        return outputMsgFlux;
    }

    @Override
    public void createKafkaTopics(List<KafkaTopicDto> kafkaTopics) {
        List<NewTopic> newTopics = kafkaTopics.stream().map(kafkaTopic ->
                new NewTopic(
                        kafkaTopic.name(),
                        kafkaTopic.optionalPartitions(),
                        kafkaTopic.optionalReplicationFactor())
        ).toList();

        kafkaAdminClient.createTopics(newTopics);
    }

    @Override
    public void deleteKafkaTopics(List<String> topicNames) {
        kafkaAdminClient.deleteTopics(topicNames);
    }

    @Override
    public List<KafkaTopicDescriptionDto> getKafkaTopics(boolean listInternal) {
        try {
            return kafkaAdminClient.describeTopics(kafkaAdminClient.listTopics().names().get())
                    .allTopicIds().get().entrySet().stream().map(topicDescriptionEntry ->
                            new KafkaTopicDescriptionDto(
                                    topicDescriptionEntry.getKey(),
                                    topicDescriptionEntry.getValue().name(),
                                    topicDescriptionEntry.getValue().isInternal(),
                                    topicDescriptionEntry.getValue().partitions().size())).toList();
        } catch(InterruptedException | ExecutionException e) {
            LOGGER.error("Error listing kafka topics", e);
            throw new RuntimeException(e);
        }
    }

}
