package org.mdaum.techstack;

import org.junit.jupiter.api.Test;
import org.mdaum.techstack.kafka.model.KafkaInputMessageDto;
import org.mdaum.techstack.kafka.model.KafkaOutputMessageDto;
import org.mdaum.techstack.kafka.service.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.client.RestClientTest;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@TestPropertySource(locations = {"/application.properties", "/credentials.properties"})
public class KafkaSpringBootTests {

    private static Logger LOGGER = LoggerFactory.getLogger(KafkaSpringBootTests.class);

    @Autowired
    private KafkaService kafkaService;

    @Test
    public void kafkaProduceConsumeTest(@Autowired WebTestClient webClient) {

        KafkaInputMessageDto dto1 = new KafkaInputMessageDto("my-key-001", "mdaum-topic-001", "verga 1");
        KafkaInputMessageDto dto2 = new KafkaInputMessageDto("my-key-001", "mdaum-topic-001", "verga 2");

        CompletableFuture<Void> webClientGet = CompletableFuture.runAsync(() -> {
            WebTestClient.BodyContentSpec spec = webClient.get()
                    .uri("/kafka/messages/get/by-topic?topicNames=mdaum-topic-001&pollTimeoutSeconds=10")
                    .exchange()
                    .expectStatus().isOk()
                    .expectBody().jsonPath("$[*]").isNotEmpty();

            LOGGER.info("Received messages: {}", spec.returnResult().getResponseBody());
        }, Executors.newSingleThreadExecutor());

        HttpStatusCode statusCode = webClient.post()
                .uri("/kafka/messages/produce-multiple")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(List.of(dto1, dto2))
                .exchange()
                .expectStatus().isOk()
                .returnResult(Void.class)
                .getStatus();

        webClientGet.orTimeout(15, TimeUnit.SECONDS);

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
