package org.mdaum.techstack;

import org.junit.jupiter.api.Test;
import org.mdaum.techstack.kafka.model.KafkaInputMessageDto;
import org.mdaum.techstack.kafka.model.KafkaOutputMessageDto;
import org.mdaum.techstack.kafka.service.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.BodyInserters;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@TestPropertySource(locations = {"/application.properties", "/credentials.properties"})
public class KafkaSpringBootTests {

    private static Logger LOGGER = LoggerFactory.getLogger(KafkaSpringBootTests.class);

    private static final String MSG_1 = "mdaum message 1";
    private static final String MSG_2 = "mdaum message 2";

    private static KafkaInputMessageDto dto1 = new KafkaInputMessageDto("my-key-001", "mdaum-topic-001", "mdaum message 1");
    private static KafkaInputMessageDto dto2 = new KafkaInputMessageDto("my-key-001", "mdaum-topic-001", "mdaum message 2");

    @Autowired
    private KafkaService kafkaService;

    @Test
    public void kafkaProduceConsumeFluxTest(@Autowired WebTestClient webClient, @Autowired WebTestClient webClient2) {

        webClient.mutate().responseTimeout(Duration.ofSeconds(7)).build()
                .get()
                .uri("/kafka/messages/stream/by-topic?topicNames=mdaum-topic-001&pollTimeoutSeconds=3")
                .exchange()
                .returnResult(new ParameterizedTypeReference<KafkaOutputMessageDto>() {
                }).getResponseBody().doOnNext(next -> LOGGER.info("Received kafka message {}", next));

        LOGGER.info("Finished receiving kafka message flux");

        Flux<KafkaInputMessageDto> inputMessageFlux = Flux.fromIterable(List.of(dto1, dto2))
                .doOnNext(next -> LOGGER.info("Sending dto {}", next))
                .doOnComplete(() -> LOGGER.info("Done sending DTOs"));

        HttpStatusCode statusCode = webClient2.post()
                .uri("/kafka/messages/produce-multiple-flux")
                .contentType(MediaType.APPLICATION_NDJSON)
                .body(BodyInserters.fromPublisher(inputMessageFlux, KafkaInputMessageDto.class))
                .exchange()
                .expectStatus().isOk()
                .returnResult(Void.class)
                .getStatus();
    }

    @Test
    void kafkaFluxProduceConsumeTest() throws InterruptedException {

        Flux<KafkaOutputMessageDto> outputMessageFlux = kafkaService.streamKafkaMessagesByTopics(
                Collections.singletonList("mdaum-topic-001"),
                Optional.empty(),
                false,
                5).doOnNext(next -> LOGGER.info("Output message for topic [{}]: {}:{}", next.topic(), next.key(), next.content()));

        StepVerifier.create(outputMessageFlux)
                .expectNextCount(2)
                .expectComplete()
                .verify();

        outputMessageFlux.subscribe();

        KafkaInputMessageDto dto1 = new KafkaInputMessageDto("my-key-001", "mdaum-topic-001", "verga 1");
        KafkaInputMessageDto dto2 = new KafkaInputMessageDto("my-key-001", "mdaum-topic-001", "verga 2");

        Flux<KafkaInputMessageDto> inputMessageFlux = Flux.fromIterable(List.of(dto1, dto2));

        kafkaService.produceKafkaMessageFlux(inputMessageFlux);
    }

//    @Test
//    void produceMultipleKafkaRecords() {
//
//        WebTestClient webClient = WebTestClient.bindToServer()
//                .baseUrl("http://localhost:8080").build();
//
//        KafkaInputMessageDto dto1 = new KafkaInputMessageDto("my-key-001", "mdaum-topic-001", "verga 1");
//        KafkaInputMessageDto dto2 = new KafkaInputMessageDto("my-key-001", "mdaum-topic-001", "verga 2");
//
//        Flux<KafkaInputMessageDto> messageFlux = Flux.fromIterable(List.of(dto1, dto2));
//
//        HttpStatusCode statusCode = webClient.post()
//                .uri("kafka/messages/produce-multiple")
//                .contentType(MediaType.APPLICATION_NDJSON)
//                .body(BodyInserters.fromPublisher(messageFlux, KafkaInputMessageDto.class))
//                .exchange()
//                .expectStatus().isCreated()
//                .returnResult(Void.class)
//                .getStatus();
//
//        LOGGER.info("Response status: {}", statusCode);
//
//    }
}
