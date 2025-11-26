package org.mdaum.techstack.s3.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.mdaum.techstack.s3.configuration.S3ConfigurationProperties;
import org.mdaum.techstack.s3.model.S3Request;
import org.mdaum.techstack.s3.model.S3UploadJsonRequest;
import org.mdaum.techstack.s3.model.S3UploadStringRequest;
import org.mdaum.techstack.util.serialization.ObjectMappers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

@Component
public class S3ServiceImpl implements S3Service {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3ServiceImpl.class);

    private final S3Client s3Client;
    private final S3AsyncClient s3AsyncClient;
    private final S3ConfigurationProperties s3ConfigurationProperties;

    @Autowired
    public S3ServiceImpl(
            S3Client s3Client,
            S3AsyncClient s3AsyncClient,
            S3ConfigurationProperties s3ConfigurationProperties) {
        this.s3Client = s3Client;
        this.s3AsyncClient = s3AsyncClient;
        this.s3ConfigurationProperties = s3ConfigurationProperties;
    }

    @Override
    public void uploadJson(S3UploadJsonRequest s3UploadJsonRequest) {
        String targetPath = getS3PathStringFromConfigAndS3Request(s3UploadJsonRequest);
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(s3ConfigurationProperties.bucketName())
                .key(targetPath)
                .contentType("application/json")
                .build();

        LOGGER.info("Uploading json into {}/{}", s3ConfigurationProperties.bucketName(), targetPath);

        try {
            RequestBody requestBody = RequestBody.fromString(ObjectMappers.GENERAL.writeValueAsString(s3UploadJsonRequest.getJsonObject()));
            s3Client.putObject(putObjectRequest, requestBody);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error uploading JSON object to s3://{}", targetPath
                    , e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void uploadString(S3UploadStringRequest s3UploadStringRequest) {

        String targetPath = getS3PathStringFromConfigAndS3Request(s3UploadStringRequest);

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(s3ConfigurationProperties.bucketName())
                .key(targetPath)
                .contentType("text/plain")
                .build();

        LOGGER.info("Uploading string into {}/{}", s3ConfigurationProperties.bucketName(), targetPath);

        RequestBody requestBody = RequestBody.fromString(s3UploadStringRequest.getContent());
        s3Client.putObject(putObjectRequest, requestBody);
    }

    @Override
    public void uploadMultipartFile(S3Request s3UploadObjectRequest, MultipartFile[] files) {

        String targetPath = getS3PathStringFromConfigAndS3Request(s3UploadObjectRequest);

        LOGGER.info("Uploading multipart into {}/{}, size: {}", s3ConfigurationProperties.bucketName(), targetPath, files.length);

        List<InputStream> inputStreams = Stream.of(files).map(mpf -> {
            try {
                return mpf.getInputStream();
            } catch (IOException e) {
                LOGGER.error("IOException uploading file stream to {}/{}", s3ConfigurationProperties.bucketName(), targetPath, e);
                throw new RuntimeException(e);
            }
        }).toList();

        SequenceInputStream multipartsJoinedStream = new SequenceInputStream(Collections.enumeration(inputStreams));
        RequestBody body = RequestBody.fromContentProvider(
                ContentStreamProvider.fromInputStream(multipartsJoinedStream), MediaType.APPLICATION_OCTET_STREAM_VALUE);

        s3Client.putObject(b -> b.bucket(s3ConfigurationProperties.bucketName()).key(targetPath), body);
    }

    @Override
    public InputStream getObject(S3Request s3GetRequest) {
        String targetPath = getS3PathStringFromConfigAndS3Request(s3GetRequest);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(s3ConfigurationProperties.bucketName())
                .key(getS3PathStringFromConfigAndS3Request(s3GetRequest))
                .build();

        try {
            // Returns the InputStream directly from S3 without loading entire file into memory
            return s3Client.getObject(getObjectRequest, ResponseTransformer.toInputStream());
        } catch (Exception e) {
            LOGGER.error("Error retrieving object from s3://{}/{}",
                    s3ConfigurationProperties.bucketName(), targetPath, e);
            throw new RuntimeException("Failed to get object from S3", e);
        }
    }

    private String getS3PathStringFromConfigAndS3Request(S3Request s3Request) {
        return Path.of(s3ConfigurationProperties.prefix(), s3Request.getFileName()).toString();

    }

}
