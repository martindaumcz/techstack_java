package org.mdaum.techstack.s3.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.mdaum.techstack.s3.model.S3Request;
import org.mdaum.techstack.s3.model.S3UploadJsonRequest;
import org.mdaum.techstack.s3.model.S3UploadStringRequest;
import org.mdaum.techstack.s3.service.S3Service;
import org.mdaum.techstack.util.serialization.ObjectMappers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Flux;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

@RestController
@RequestMapping("s3")
public class S3Controller {

    private final static Logger LOGGER = LoggerFactory.getLogger(S3Controller.class);

    private final S3Service s3Service;

    @Autowired
    public S3Controller(S3Service s3Service) {
        this.s3Service = s3Service;
    }

    @PostMapping("upload-json")
    public void uploadJson(@RequestBody S3UploadJsonRequest s3UploadJsonRequest) {
        s3Service.uploadJson(s3UploadJsonRequest);
    }

    @PostMapping("upload-string")
    public void uploadString(@RequestBody S3UploadStringRequest s3UploadStringRequest) {
        s3Service.uploadString(s3UploadStringRequest);
    }

    @PostMapping(value = "upload-multipart-file-reactive", consumes = { MediaType.MULTIPART_FORM_DATA_VALUE })
    public void uploadMultipartFile(@RequestPart("s3Request") String s3UploadObjectRequest,
                                    @RequestPart("file") Flux<FilePart> files) {

//        files.doOnNext(
//                file -> file.content().doOnNext(
//                        dataBuffer -> {
//                            byte[] bytes = new byte[dataBuffer.readableByteCount()];
//                            dataBuffer.read(bytes);
//                            DataBufferUtils.release(dataBuffer);
//                            LOGGER.info(new String(bytes, StandardCharsets.UTF_8));
//                        }
//                ).subscribe()
//        ).subscribe();

        try {

            s3Service.uploadMultipartFileReactive(ObjectMappers.GENERAL.readValue(s3UploadObjectRequest, S3Request.class), files);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @PostMapping(value = "upload-multipart-file", consumes = { MediaType.MULTIPART_FORM_DATA_VALUE })
    public void uploadMultipartFile(@RequestPart("s3Request") String s3UploadObjectRequest,
                                    @RequestPart("file") MultipartFile[] files) {
        try {
            s3Service.uploadMultipartFile(ObjectMappers.GENERAL.readValue(s3UploadObjectRequest, S3Request.class), files);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @GetMapping
    public Flux<ByteBuffer> getObject(@RequestParam("fileName") S3Request s3GetRequest) {
        InputStream inputStream = s3Service.getObject(s3GetRequest);

        Flux<ByteBuffer> stringFlux = DataBufferUtils.readByteChannel(() -> Channels.newChannel(inputStream), DefaultDataBufferFactory.sharedInstance, 4096).map(DataBuffer::asByteBuffer);
        return stringFlux;
    }
}
