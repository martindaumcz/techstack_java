package org.mdaum.techstack.kafka.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.mdaum.techstack.kafka.configuration.KafkaProducerBaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Component
public class DeadLetterTopicProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeadLetterTopicProducer.class);

    private static final String DLT_RECOVERABLE_SUFFIX = "-dltrecoverable";
    private static final String DLT_UNRECOVERABLE_SUFFIX = "-dltunrecoverable";

    private static final String HEADER_EXCEPTION_CLASS = "exception-class";
    private static final String HEADER_EXCEPTION_MESSAGE = "exception-message";
    private static final String HEADER_ORIGINAL_TOPIC = "original-topic";
    private static final String HEADER_ORIGINAL_PARTITION = "original-partition";
    private static final String HEADER_ORIGINAL_OFFSET = "original-offset";
    private static final String HEADER_ORIGINAL_TIMESTAMP = "original-timestamp";
    private static final String HEADER_DLT_TIMESTAMP = "dlt-timestamp";

    private final KafkaSender<Object, Object> kafkaSender;

    @Autowired
    public DeadLetterTopicProducer(KafkaProducerBaseConfig kafkaProducerBaseConfig) {
        SenderOptions<Object, Object> senderOptions = SenderOptions.create(kafkaProducerBaseConfig.createConfigurationMap());
        this.kafkaSender = KafkaSender.create(senderOptions);
    }

    /**
     * Sends a record to the dead letter topic for recoverable exceptions.
     * The record will be sent to [originalTopic]-dltrecoverable
     *
     * @param originalRecord The original Kafka record that failed processing
     * @param exception The recoverable exception that was thrown
     * @return A Mono that completes when the record is sent
     */
    public Mono<Void> sendToRecoverableDLT(ReceiverRecord<Object, Object> originalRecord, Throwable exception) {
        String dltTopic = originalRecord.topic() + DLT_RECOVERABLE_SUFFIX;
        return sendToDeadLetterTopic(originalRecord, exception, dltTopic);
    }

    /**
     * Sends a record to the dead letter topic for unrecoverable exceptions.
     * The record will be sent to [originalTopic]-dltunrecoverable
     *
     * @param originalRecord The original Kafka record that failed processing
     * @param exception The unrecoverable exception that was thrown
     * @return A Mono that completes when the record is sent
     */
    public Mono<Void> sendToUnrecoverableDLT(ReceiverRecord<Object, Object> originalRecord, Throwable exception) {
        String dltTopic = originalRecord.topic() + DLT_UNRECOVERABLE_SUFFIX;
        return sendToDeadLetterTopic(originalRecord, exception, dltTopic);
    }

    /**
     * Internal method to send a record to a dead letter topic with exception metadata in headers.
     *
     * @param originalRecord The original Kafka record
     * @param exception The exception that caused the failure
     * @param dltTopic The dead letter topic name
     * @return A Mono that completes when the record is sent
     */
    private Mono<Void> sendToDeadLetterTopic(
            ReceiverRecord<Object, Object> originalRecord,
            Throwable exception,
            String dltTopic) {

        ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(
                dltTopic,
                null, // partition - let Kafka decide
                originalRecord.key(),
                originalRecord.value()
        );

        // Add headers with exception and original record metadata
        producerRecord.headers().add(new RecordHeader(
                HEADER_EXCEPTION_CLASS,
                exception.getClass().getName().getBytes(StandardCharsets.UTF_8)
        ));

        producerRecord.headers().add(new RecordHeader(
                HEADER_EXCEPTION_MESSAGE,
                (exception.getMessage() != null ? exception.getMessage() : "").getBytes(StandardCharsets.UTF_8)
        ));

        producerRecord.headers().add(new RecordHeader(
                HEADER_ORIGINAL_TOPIC,
                originalRecord.topic().getBytes(StandardCharsets.UTF_8)
        ));

        producerRecord.headers().add(new RecordHeader(
                HEADER_ORIGINAL_PARTITION,
                String.valueOf(originalRecord.partition()).getBytes(StandardCharsets.UTF_8)
        ));

        producerRecord.headers().add(new RecordHeader(
                HEADER_ORIGINAL_OFFSET,
                String.valueOf(originalRecord.offset()).getBytes(StandardCharsets.UTF_8)
        ));

        producerRecord.headers().add(new RecordHeader(
                HEADER_ORIGINAL_TIMESTAMP,
                String.valueOf(originalRecord.timestamp()).getBytes(StandardCharsets.UTF_8)
        ));

        producerRecord.headers().add(new RecordHeader(
                HEADER_DLT_TIMESTAMP,
                String.valueOf(Instant.now().toEpochMilli()).getBytes(StandardCharsets.UTF_8)
        ));

        SenderRecord<Object, Object, String> senderRecord = SenderRecord.create(
                producerRecord,
                originalRecord.topic() + ":" + originalRecord.offset()
        );

        LOGGER.info("Sending record to DLT topic [{}] - original: {}:{} from topic [{}]",
                dltTopic,
                originalRecord.key(),
                originalRecord.value(),
                originalRecord.topic());

        return kafkaSender.send(Mono.just(senderRecord))
                .doOnNext(result -> LOGGER.info(
                        "Successfully sent to DLT [{}] - partition: {}, offset: {}",
                        dltTopic,
                        result.recordMetadata().partition(),
                        result.recordMetadata().offset()
                ))
                .doOnError(error -> LOGGER.error(
                        "Failed to send record to DLT topic [{}]",
                        dltTopic,
                        error
                ))
                .then();
    }

    /**
     * Closes the Kafka sender and releases resources.
     */
    public void close() {
        kafkaSender.close();
    }
}
