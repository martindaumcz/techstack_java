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
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.InputStream;

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

    @PostMapping(value = "upload-multipart-file", consumes = { MediaType.MULTIPART_FORM_DATA_VALUE, MediaType.APPLICATION_JSON_VALUE })
    public void uploadMultipartFile(@RequestPart("s3Request") String s3UploadObjectRequest,
                                    @RequestPart("file") MultipartFile[] files) {
        try {
            s3Service.uploadMultipartFile(ObjectMappers.GENERAL.readValue(s3UploadObjectRequest, S3Request.class), files);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @GetMapping
    public StreamingResponseBody getObject(@RequestParam("fileName") S3Request s3GetRequest) {
        InputStream is = s3Service.getObject(s3GetRequest);
        final StreamingResponseBody streamingResponseBody = (os) -> {
            is.transferTo(os);
        };
        return streamingResponseBody;
    }
}
