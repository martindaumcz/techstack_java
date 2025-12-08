package org.mdaum.techstack;

import org.junit.jupiter.api.Test;
import org.mdaum.techstack.kafka.model.KafkaInputMessageDto;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;

@SpringBootTest
class TechstackApplicationTests {

	@Test
	void contextLoads() {
	}

}
