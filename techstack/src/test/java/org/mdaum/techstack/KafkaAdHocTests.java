package org.mdaum.techstack;

import org.junit.jupiter.api.Test;
import org.mdaum.techstack.kafka.exception.RecoverableException;
import org.mdaum.techstack.kafka.model.KafkaInputMessageDto;
import org.mdaum.techstack.kafka.model.KafkaOutputMessageDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

public class KafkaAdHocTests {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAdHocTests.class);

    private static final String MSG_1 = "mdaum message 1";
    private static final String MSG_2 = "mdaum message 2";
    private static final String MSG_ERROR = "mdaum message recoverable error blabla";
    private static final String MSG_FATAL_ERROR = "mdaum message fatal error blabla";

    private static final String TOPIC = "mdaum-topic-001";
    private static final String KEY = "mdaum-key-001";

    private static final String KAFKA_BASE_URL = "http://localhost:8080/kafka";

    private static KafkaInputMessageDto dto1 = new KafkaInputMessageDto(KEY, TOPIC, MSG_1);
    private static KafkaInputMessageDto dto2 = new KafkaInputMessageDto(KEY, TOPIC, MSG_2);
    private static KafkaInputMessageDto errorDto = new KafkaInputMessageDto(KEY, TOPIC, MSG_ERROR);
    private static KafkaInputMessageDto fatalErrorDto = new KafkaInputMessageDto(KEY, TOPIC, MSG_FATAL_ERROR);

    private WebClient webClient = WebClient.builder().baseUrl(KAFKA_BASE_URL).build();
    private WebClient webClient2 = WebClient.builder().baseUrl(KAFKA_BASE_URL).build();
    private RestClient restClient = RestClient.builder().baseUrl(KAFKA_BASE_URL).build();

    @Test
    public void produceFlux() {

        Flux<KafkaInputMessageDto> inputMessageFlux = Flux.fromIterable(List.of(dto1, dto2));

        WebClient.ResponseSpec responseSpec = webClient.post()
                .uri(URI.create("messages/produce-multiple-flux"))
                .body(BodyInserters.fromProducer(inputMessageFlux, KafkaInputMessageDto.class))
                        .header("Content-Type", MediaType.APPLICATION_NDJSON_VALUE)
                                .retrieve();

        responseSpec.toEntity(Void.class).doOnNext(
                next ->
                        LOGGER.info("Produce multiple flux response status: {}", next.getStatusCode().value())
        );
    }

    @Test
    public void produceAndConsumeFlux() {

        Flux<KafkaInputMessageDto> inputMessageFlux = Flux.fromIterable(List.of(dto1, dto2))
                .doOnNext(next -> LOGGER.info("Sending dto {}", next))
                .doOnComplete(() -> LOGGER.info("Done sending DTOs"));

        Flux<KafkaOutputMessageDto> consumerFlux = webClient2.get()
                .uri("/messages/stream/by-topic?topicNames=mdaum-topic-001&pollTimeoutSeconds=15")
                .retrieve()
                .bodyToFlux(KafkaOutputMessageDto.class)
                .doFirst(() -> LOGGER.info("Ready to receive messages"))
                .doOnNext(next -> LOGGER.info("Received message {}", next))
                .doOnComplete(() -> LOGGER.info("Completed message receiving"))
                .doOnError(error -> LOGGER.error("Received error", error));

        consumerFlux.subscribe();

        threadSleep(5000);

        Mono<Void> producedMono = webClient.post()
                .uri("/messages/produce-multiple-flux")
                .contentType(MediaType.APPLICATION_NDJSON)
                .body(BodyInserters.fromPublisher(inputMessageFlux, KafkaInputMessageDto.class))
                .retrieve()
                .toBodilessEntity()
                .doOnSuccess(response -> LOGGER.info("Successfully sent all messages with status: {}", response.getStatusCode()))
                .doOnError(error -> LOGGER.error("Error in flux test", error))
                .then();

        producedMono.subscribe();

        LOGGER.info("Test completed - all messages sent to Kafka");

        threadSleep(20000);

    }

    @Test
    public void produceAndCosumeFluxWithErrors() {

        Flux<KafkaInputMessageDto> inputMessageFlux = Flux.fromIterable(List.of(dto1, errorDto, fatalErrorDto, dto2))
                .doOnNext(next -> LOGGER.info("Sending dto {}", next))
                .doOnComplete(() -> LOGGER.info("Done sending DTOs"));

        Flux<KafkaOutputMessageDto> consumerFlux = webClient2.get()
                .uri("/messages/stream/by-topic?topicNames=mdaum-topic-001&pollTimeoutSeconds=15")
                .retrieve()
                .bodyToFlux(KafkaOutputMessageDto.class)
                .doFirst(() -> LOGGER.info("Ready to receive messages"))
                .doOnNext(next -> LOGGER.info("Received message {}", next))
                .doOnComplete(() -> LOGGER.info("Completed message receiving"))
                .doOnError(error -> LOGGER.error("Received error", error));

        consumerFlux.subscribe();

        threadSleep(5000);

        Mono<Void> producedMono = webClient.post()
                .uri("/messages/produce-multiple-flux")
                .contentType(MediaType.APPLICATION_NDJSON)
                .body(BodyInserters.fromPublisher(inputMessageFlux, KafkaInputMessageDto.class))
                .retrieve()
                .toBodilessEntity()
                .doOnSuccess(response -> LOGGER.info("Successfully sent all messages with status: {}", response.getStatusCode()))
                .doOnError(error -> LOGGER.error("Error in flux test", error))
                .then();

        producedMono.subscribe();

        LOGGER.info("Test completed - all messages sent to Kafka");

        threadSleep(20000);

    }

    private void threadSleep(long millis) {

        try {
            Thread.sleep(millis);
        } catch (InterruptedException e ) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void reactorRetryTest() throws InterruptedException {

        Flux<Integer> myFlux = Flux.<Integer>create(sink -> sink.next(new Random().ints(0, 100).findFirst().getAsInt()))
        .doOnNext(next -> {
            if (next <= 90 ) {
                LOGGER.error("{}, throwing error", next);
                throw new RecoverableException("recoverable exception with value "+ next);
            } else {
                LOGGER.info("Consumed {}", next);
            }
        })
        .retryWhen(Retry
                .fixedDelay(200, Duration.ofMillis(20))
                .filter(RecoverableException.class::isInstance)
                .doBeforeRetry(signal -> LOGGER.info("Retrying {} times", signal.totalRetries()))
                .doAfterRetry(signal -> LOGGER.info("Retries {} times", signal.totalRetries())))
        .take(50);

        myFlux.subscribe();

        Thread.sleep(Duration.ofMillis(10000));



    }
}
