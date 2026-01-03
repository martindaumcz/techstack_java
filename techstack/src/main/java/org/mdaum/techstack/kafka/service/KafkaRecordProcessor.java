package org.mdaum.techstack.kafka.service;

import org.mdaum.techstack.kafka.exception.RecoverableException;
import org.mdaum.techstack.kafka.exception.UnrecoverableException;
import org.mdaum.techstack.kafka.model.KafkaOutputMessageDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Random;

@Component
public class KafkaRecordProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRecordProcessor.class);

    public Flux<KafkaOutputMessageDto> processRecords(Flux<ReceiverRecord<Object, Object>> recordFlux) {
        // here concatMap is necessary to process records sequentially (rather than flatMap, which does so concurrently
        // this is to maintain the order of the records as well as delaying the subsequent entries until retries in case of recoverable exception have been done
        // with flatmap, this would just process the subsequent records immediately before retries were done and possibly error the flux if a record were to cause the UnrecoverableException
        return recordFlux.concatMap(receiverRecord ->
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
                        // if the retries are to work, we must resubscribe to the particular record upon retry rather than to the whole flux - in the latter case, the flux would just provide the
                        // following record instead of the one that failed and that we want to retry
                        .retryWhen(
                                Retry.fixedDelay(50, Duration.ofMillis(20))
                                        .filter(RecoverableException.class::isInstance)
                                        .doBeforeRetry(signal -> LOGGER.info("Retrying message - attempt {}", signal.totalRetries() + 1))
                                        .doAfterRetry(signal -> LOGGER.info("Completed retry attempt {}", signal.totalRetries()))
                                        .onRetryExhaustedThrow((spec, signal) -> signal.failure()))
                        .doOnError(RecoverableException.class, ex -> LOGGER.error("Retry exhausted for recoverable error", ex))
                        .onErrorResume(UnrecoverableException.class, ex -> {
                            LOGGER.error("Unrecoverable error processing message, terminating", ex);
                            return Mono.error(ex); // Skip this message and continue with the stream
                        })
                        .map(record -> new KafkaOutputMessageDto(
                                record.key().toString(),
                                record.topic(),
                                record.value().toString(),
                                null,
                                null))
                );
    }

}
