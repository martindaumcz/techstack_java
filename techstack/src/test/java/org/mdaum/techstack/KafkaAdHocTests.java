package org.mdaum.techstack;

import org.junit.jupiter.api.Test;
import org.mdaum.techstack.kafka.model.KafkaInputMessageDto;
import org.mdaum.techstack.kafka.model.KafkaOutputMessageDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.net.URI;
import java.util.List;
import java.util.UUID;

public class KafkaAdHocTests {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAdHocTests.class);

    WebClient webClient = WebClient.builder().baseUrl("http://localhost:8080/kafka").build();
    RestClient restClient = RestClient.builder().baseUrl("http://localhost:8080/kafka").build();

    @Test
    public void produceFlux() {

        KafkaInputMessageDto dto1 = new KafkaInputMessageDto("my-key-001", "mdaum-topic-001", "verga " + UUID.randomUUID().toString());
        KafkaInputMessageDto dto2 = new KafkaInputMessageDto("my-key-001", "mdaum-topic-001", "verga " + UUID.randomUUID().toString());

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
    public void produceMultiple() {

        KafkaInputMessageDto dto1 = new KafkaInputMessageDto("my-key-001", "mdaum-topic-001", "verga " + UUID.randomUUID().toString());
        KafkaInputMessageDto dto2 = new KafkaInputMessageDto("my-key-001", "mdaum-topic-001", "verga " + UUID.randomUUID().toString());

        List<KafkaInputMessageDto> inputMessages = List.of(dto1, dto2);

        RestClient.ResponseSpec responseSpec = restClient.post()
                .uri(URI.create("messages/produce-multiple"))
                .body(inputMessages)
                .header("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                .retrieve();

        LOGGER.info("Produce multiple response status: {}", responseSpec.toEntity(Void.class).getStatusCode().value());

    }

    @Test
    public void consumeFlux() {

        LOGGER.info("Waiting for kafka messages");

        Flux<KafkaOutputMessageDto> msgFlux = webClient.get()
                .uri("/messages/stream/by-topic?topicNames=mdaum-topic-001&pollTimeoutSeconds=15")
                .retrieve()
                .bodyToFlux(KafkaOutputMessageDto.class);

        msgFlux.doOnNext(next -> LOGGER.info("Received message {}", next))
                .subscribe();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        KafkaInputMessageDto dto1 = new KafkaInputMessageDto("my-key-001", "mdaum-topic-001", "verga " + UUID.randomUUID().toString());
        KafkaInputMessageDto dto2 = new KafkaInputMessageDto("my-key-001", "mdaum-topic-001", "verga " + UUID.randomUUID().toString());

        Flux<KafkaInputMessageDto> inputMessageFlux = Flux.fromIterable(List.of(dto1, dto2));

        WebClient.ResponseSpec responseSpec = webClient.post()
                .uri(URI.create("/messages/produce-multiple-flux"))
                .body(BodyInserters.fromProducer(inputMessageFlux, KafkaInputMessageDto.class))
                .header("Content-Type", MediaType.APPLICATION_NDJSON_VALUE)
                .retrieve();

        responseSpec.toEntity(Void.class).doOnNext(
                next ->
                        LOGGER.info("Produce multiple flux response status: {}", next.getStatusCode().value())
        );

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


    }
}
