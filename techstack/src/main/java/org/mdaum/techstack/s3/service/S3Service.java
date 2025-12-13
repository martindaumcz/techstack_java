package org.mdaum.techstack.s3.service;

import org.mdaum.techstack.s3.model.S3Request;
import org.mdaum.techstack.s3.model.S3UploadJsonRequest;
import org.mdaum.techstack.s3.model.S3UploadStringRequest;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.InputStream;

public interface S3Service {

    void uploadJson(@RequestBody S3UploadJsonRequest s3UploadJsonRequest);

    void uploadString(@RequestBody S3UploadStringRequest s3UploadStringRequest);

    Mono<Void> uploadMultipartFileReactive(@RequestPart S3Request s3UploadObjectRequest,
                                           @RequestPart Flux<FilePart> files);

    void uploadMultipartFile(@RequestPart S3Request s3UploadObjectRequest,
                                     @RequestPart MultipartFile[] files);

    InputStream getObject(@RequestParam("fileName") S3Request s3GetRequest);
}
