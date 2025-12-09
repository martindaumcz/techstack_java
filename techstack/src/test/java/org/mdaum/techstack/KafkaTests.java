package org.mdaum.techstack;

import org.junit.jupiter.api.Test;
import org.mdaum.techstack.configuration.KafkaTestConfiguration;
import org.mdaum.techstack.kafka.model.KafkaInputMessageDto;
import org.mdaum.techstack.kafka.model.KafkaOutputMessageDto;
import org.mdaum.techstack.kafka.service.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.TestPropertySource;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

@SpringBootTest
@TestPropertySource(locations = {"/application.properties", "/credentials.properties"})
public class KafkaTests {

    private static Logger LOGGER = LoggerFactory.getLogger(KafkaTests.class);

    @Autowired
    private KafkaService kafkaService;

    @Bean("testBean")
    public String testBean() {
        LOGGER.info("test bean being created");
        return "blablabla";
    }

    @Test
    public void kafkaProduceConsumerTest() {

    }


    @Test
    void kafkaFluxProduceConsumeTest() throws InterruptedException {

        Flux<KafkaOutputMessageDto> outputMessageFlux = kafkaService.streamKafkaMessagesByTopics(
                Collections.singletonList("mdaum-topic-001"),
                Optional.empty(),
                2).doOnNext(next -> LOGGER.info("Output message for topic [{}]: {}:{}", next.topic(), next.key(), next.content()));

        StepVerifier.create(outputMessageFlux)
                .expectNextCount(2)
                .expectComplete()
                .verify();

        outputMessageFlux.subscribe();

        KafkaInputMessageDto dto1 = new KafkaInputMessageDto("my-key-001", "mdaum-topic-001", "verga 1");
        KafkaInputMessageDto dto2 = new KafkaInputMessageDto("my-key-001", "mdaum-topic-001", "verga 2");

        Flux<KafkaInputMessageDto> inputMessageFlux = Flux.fromIterable(List.of(dto1, dto2));

        kafkaService.produceKafkaMessages(inputMessageFlux);
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
