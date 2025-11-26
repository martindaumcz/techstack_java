package org.mdaum.techstack.kafka.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.mdaum.techstack.kafka.configuration.KafkaConsumerBaseConfig;
import org.mdaum.techstack.kafka.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Component
public class KafkaServiceImpl implements KafkaService{

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaServiceImpl.class);

    private KafkaConsumerBaseConfig kafkaConsumerBaseConfig;
    private KafkaAdmin kafkaAdmin;
    private AdminClient kafkaAdminClient;
    private KafkaTemplate<String, String> kafkaTemplate;

    private Map<String, KafkaConsumerAndFluxDto> kafkaConsumersAndFluxByName = new HashMap<>();

    @Autowired
    public KafkaServiceImpl(
            KafkaConsumerBaseConfig kafkaConsumerBaseConfig,
            KafkaAdmin kafkaAdmin,
            AdminClient kafkaAdminClient,
            KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaConsumerBaseConfig = kafkaConsumerBaseConfig;
        this.kafkaAdmin = kafkaAdmin;
        this.kafkaAdminClient = kafkaAdminClient;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void produceKafkaMessage(KafkaInputMessageDto kafkaMessage) {
        kafkaTemplate.send(kafkaMessage.topic(), kafkaMessage.key(), kafkaMessage.content());
    }

    @Override
    public List<KafkaOutputMessageDto> getKafkaMessages(String topic, int maxMessages) {
        return List.of();
    }

    @Override
    public Flux<KafkaOutputMessageDto> streamKafkaMessagesByTopic(String topic, int maxMessages) {
        List<Flux<ReceiverRecord<Object, Object>>> topicFluxes = kafkaConsumersAndFluxByName.entrySet().stream()
                .filter(
                        entry -> entry.getValue().kafkaConsumer().topics().contains(topic))
                .map(entry -> entry.getValue().flux()).toList();

        Flux<KafkaOutputMessageDto> mergedFlux = Flux.merge(topicFluxes)
                .filter(record -> record.topic().equals(topic))
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

        return mergedFlux;
    }

    @Override
    public Flux<KafkaOutputMessageDto> streamKafkaMessagesByConsumerName(String consumerName, int maxMessages) {
        KafkaConsumerAndFluxDto kafkaConsumerAndFluxDto = kafkaConsumersAndFluxByName.get(consumerName);
        return kafkaConsumerAndFluxDto.flux()
                .take(maxMessages)
                .map(receiverRecord -> new KafkaOutputMessageDto(
                        receiverRecord.key().toString(),
                        receiverRecord.topic(),
                        receiverRecord.value().toString(),
                        consumerName,
                        kafkaConsumerAndFluxDto.kafkaConsumer().consumerGroup()
                )).doOnNext(kafkaOutputMessageDto ->
                        LOGGER.info("Consumed message {}:{} from topic {} by consumer {}",
                                kafkaOutputMessageDto.key(),
                                kafkaOutputMessageDto.content(),
                                kafkaOutputMessageDto.topic(),
                                kafkaOutputMessageDto.consumerName()));
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

    @Override
    public void createOrAlterConsumer(KafkaConsumerDto kafkaConsumer) {

        Map<String, Object> consumerBaseConfigMap = kafkaConsumerBaseConfig.createConfigurationMap();
        consumerBaseConfigMap.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumer.consumerGroup());

        ReceiverOptions<Object, Object> receiverOptions = ReceiverOptions.create(consumerBaseConfigMap)
                .subscription(kafkaConsumer.topics());

        KafkaReceiver<Object, Object> kafkaReceiver = KafkaReceiver.create(receiverOptions);

        Flux<ReceiverRecord<Object, Object>> flux = kafkaReceiver.receive();

        kafkaConsumersAndFluxByName.put(kafkaConsumer.name(), new KafkaConsumerAndFluxDto(kafkaConsumer, flux));
    }

    @Override
    public void deleteConsumer(String consumerName) {
        kafkaConsumersAndFluxByName.remove(consumerName);
    }

    @Override
    public List<KafkaConsumerDto> getKafkaConsumers() {
        return kafkaConsumersAndFluxByName.entrySet().stream().map(entry ->
                entry.getValue().kafkaConsumer()).toList();
    }

    @Override
    public void createOrAlterConsumerGroup(KafkaConsumerGroupDto kafkaConsumerGroup) {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public void deleteConsumerGroup(String groupName) {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public List<KafkaConsumerGroupDto> getKafkaConsumerGroups() {
        throw new RuntimeException("Not implemented yet");
    }
}
