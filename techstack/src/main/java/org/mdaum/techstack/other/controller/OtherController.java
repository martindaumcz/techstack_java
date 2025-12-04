package org.mdaum.techstack.other.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("other")
public class OtherController {

    private static Logger LOGGER = LoggerFactory.getLogger(OtherController.class);

    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @RequestMapping("random-string-flux")
    public Flux<String> randomStringFlux() {

        Flux<String> flux = Flux.<String>create(sink -> {
            for (int i = 0; i <= 10; i++) {
                String str = "ha";
                sink.next(str);
                threadSleep(300);
            }
        }).doOnNext(next -> LOGGER.info("Produced {}", next));

        return flux;
    }

    @GetMapping(produces = MediaType.APPLICATION_NDJSON_VALUE)
    @RequestMapping("random-dto-flux")
    public Flux<RandomDto> randomDtoFlux() {

        Flux<RandomDto> flux = Flux.<RandomDto, Integer>generate(() -> 0, (state, sink) -> {

            RandomDto dto = new RandomDto(state, "ha"+state);
            sink.next(dto);
            threadSleep(300);

            if (state >= 10) {
                sink.complete();
            }
            return state + 1;
        }).doOnNext(next -> LOGGER.info("Produced {}", next));

        return flux;
    }

    private void threadSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static record RandomDto(int someNumber, String someString) {

    }

}
