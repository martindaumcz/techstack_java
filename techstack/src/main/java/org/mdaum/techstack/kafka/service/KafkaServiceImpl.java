package org.mdaum.techstack.kafka.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.util.Strings;
import org.mdaum.techstack.kafka.configuration.KafkaConfigurationProperties;
import org.mdaum.techstack.kafka.configuration.KafkaConsumerBaseConfig;
import org.mdaum.techstack.kafka.configuration.KafkaProducerBaseConfig;
import org.mdaum.techstack.kafka.exception.RecordProcessingException;
import org.mdaum.techstack.kafka.exception.RecoverableException;
import org.mdaum.techstack.kafka.exception.UnrecoverableException;
import org.mdaum.techstack.kafka.model.KafkaInputMessageDto;
import org.mdaum.techstack.kafka.model.KafkaOutputMessageDto;
import org.mdaum.techstack.kafka.model.KafkaTopicDescriptionDto;
import org.mdaum.techstack.kafka.model.KafkaTopicDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Component
public class KafkaServiceImpl implements KafkaService{

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaServiceImpl.class);

    private KafkaConsumerBaseConfig kafkaConsumerBaseConfig;
    private KafkaProducerBaseConfig kafkaProducerBaseConfig;
    private KafkaConfigurationProperties kafkaConfigurationProperties;
    private AdminClient kafkaAdminClient;
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public KafkaServiceImpl(
            KafkaConsumerBaseConfig kafkaConsumerBaseConfig,
            AdminClient kafkaAdminClient,
            KafkaTemplate<String, String> kafkaTemplate,
            KafkaProducerBaseConfig kafkaProducerBaseConfig,
            KafkaConfigurationProperties kafkaConfigurationProperties) {
        this.kafkaConsumerBaseConfig = kafkaConsumerBaseConfig;
        this.kafkaAdminClient = kafkaAdminClient;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProducerBaseConfig = kafkaProducerBaseConfig;
        this.kafkaConfigurationProperties = kafkaConfigurationProperties;
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
    public Mono<Void> produceKafkaMessageFlux(Flux<KafkaInputMessageDto> kafkaInputMessageDtoFlux) {
        SenderOptions<String, Object> senderOptions = SenderOptions.create(kafkaProducerBaseConfig.createConfigurationMap());

        KafkaSender<String, Object> kafkaSender = KafkaSender.create(senderOptions);

        return kafkaSender.send(kafkaInputMessageDtoFlux
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
                .doOnError(error -> LOGGER.error("Error sending messages to Kafka", error))
                .doOnComplete(() -> LOGGER.info("Completed sending all messages to Kafka"))
                .then()
                .doFinally(signalType -> kafkaSender.close());
    }

    @Override
    public void produceKafkaMessages(List<KafkaInputMessageDto> kafkaInputMessageDtos) {

        kafkaInputMessageDtos.forEach(kafkaMessage -> {

            try {
                if (Strings.isEmpty(kafkaMessage.key())) {
                    kafkaTemplate.send(kafkaMessage.topic(), kafkaMessage.content()).get();
                    LOGGER.info("Produced kafka message without key");
                } else {
                    kafkaTemplate.send(kafkaMessage.topic(), kafkaMessage.key(), kafkaMessage.content()).get();
                    LOGGER.info("Produced kafka message with key");
                }
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Failed to send kafka message", e);
            }
        });
    }

    @Override
    public List<KafkaOutputMessageDto> getKafkaMessages(
            List<String> topics,
            Optional<String> consumerGroup,
            boolean fromBeginning,
            int pollTimeoutSeconds) {

        Map<String, Object> consumerBaseConfigMap = kafkaConsumerBaseConfig.createConfigurationMap();

        String consumerGroupStr = consumerGroup.orElse(UUID.randomUUID().toString());

        consumerBaseConfigMap.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                consumerGroupStr);

        if (fromBeginning) {
            consumerBaseConfigMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerBaseConfigMap);
        kafkaConsumer.subscribe(topics);

        List<KafkaOutputMessageDto> outputMsgs = new ArrayList<>();

        if (fromBeginning) {
            LOGGER.info("Getting topics {} from beginning", topics);
            kafkaConsumer.poll(Duration.ofSeconds(0));
            kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());
        }

        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(pollTimeoutSeconds));

        LOGGER.info("Poll has new messages: {}", consumerRecords.count());

        consumerRecords.forEach(record -> {

                LOGGER.info("Consumed message {}:{} from topic {}",
                        record.key(),
                        record.value(),
                        record.topic());
                outputMsgs.add(new KafkaOutputMessageDto(
                        record.key(),
                        record.topic(),
                        record.value(),
                        null,
                        consumerGroupStr));
        });
        return outputMsgs;
    }

    @Override
    public Flux<KafkaOutputMessageDto> streamKafkaMessagesByTopics(
            List<String> topics,
            Optional<String> consumerGroup,
            boolean fromBeginning,
            int pollTimeoutSeconds) {

        Map<String, Object> consumerBaseConfigMap = kafkaConsumerBaseConfig.createConfigurationMap();
        consumerBaseConfigMap.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                consumerGroup.orElse(UUID.randomUUID().toString()));

        if (fromBeginning) {
            consumerBaseConfigMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }

        ReceiverOptions<Object, Object> receiverOptions = ReceiverOptions.create(consumerBaseConfigMap)
                .subscription(topics);
        KafkaReceiver<Object, Object> kafkaReceiver = KafkaReceiver.create(receiverOptions);
        Flux<KafkaOutputMessageDto> outputMsgFlux = kafkaReceiver
                .receive()
                //.timeout(Duration.ofSeconds(pollTimeoutSeconds), Mono.empty())
                .flatMap(receiverRecord ->
                    Mono.fromCallable(() -> {
                        int random = new Random().ints(0, 100).findFirst().getAsInt();
                        if (receiverRecord.value().toString().contains("recoverable error")) {
                            LOGGER.info("Deciding whether to throw a recoverable exception - is {} 90 or less?", random);
                            if (random <= 90) {
                                LOGGER.info("Throwing a recoverable exception");
                                throw new RecoverableException(String.format("deliberate recoverable error in message %s", receiverRecord));
                            }
                        }
                        if (receiverRecord.value().toString().contains("fatal error")) {
                            throw new UnrecoverableException(String.format("deliberate fatal error in message %s", receiverRecord));
                        }
                        return receiverRecord;
                    })
                    .retryWhen(
                        Retry.fixedDelay(50, Duration.ofMillis(20))
                                .filter(RecoverableException.class::isInstance)
                                .doBeforeRetry(signal -> LOGGER.info("Retrying message - attempt {}", signal.totalRetries() + 1))
                                .doAfterRetry(signal -> LOGGER.info("Completed retry attempt {}", signal.totalRetries()))
                                .onRetryExhaustedThrow((spec, signal) -> signal.failure()))
                    .doOnError(RecoverableException.class, ex -> LOGGER.error("Retry exhausted for recoverable error", ex))
                    .onErrorResume(UnrecoverableException.class, ex -> {
                        LOGGER.error("Unrecoverable error processing message, skipping", ex);
                        return Mono.empty(); // Skip this message and continue with the stream
                    })
                    .map(record -> new KafkaOutputMessageDto(
                        record.key().toString(),
                        record.topic(),
                        record.value().toString(),
                        null,
                        null))
                );



        LOGGER.info("Returning output flux");
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
