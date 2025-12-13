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
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
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
    public Mono<Void> uploadMultipartFileReactive(S3Request s3UploadObjectRequest, Flux<FilePart> filesFlux) {

        String targetPath = getS3PathStringFromConfigAndS3Request(s3UploadObjectRequest);
        String bucket = s3ConfigurationProperties.bucketName();

        LOGGER.info("Starting S3 multipart upload to s3://{}/{}", bucket, targetPath);

        // Step 1: Initiate multipart upload
        CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(targetPath)
                .contentType(MediaType.APPLICATION_OCTET_STREAM_VALUE)
                .build();

        return Mono.fromFuture(s3AsyncClient.createMultipartUpload(createRequest))
                .flatMap(createResponse -> {
                    String uploadId = createResponse.uploadId();
                    LOGGER.info("Initiated multipart upload with ID: {}", uploadId);

                    // Step 2: Upload each FilePart as a separate S3 part
                    // We collect ByteBuffers for each part individually (not all at once)
                    Flux<CompletedPart> completedPartsFlux = filesFlux
                            .index() // Add index to track part numbers (0-based)
                            .concatMap(tuple -> {
                                long index = tuple.getT1();
                                FilePart filePart = tuple.getT2();
                                int partNumber = (int) (index + 1); // S3 part numbers are 1-based

                                LOGGER.info("Processing file part {}: {}", partNumber, filePart.filename());

                                // Collect all ByteBuffers for this specific FilePart
                                // This buffers ONE part at a time, not all parts
                                return filePart.content()
                                        .map(dataBuffer -> {
                                            ByteBuffer byteBuffer = dataBuffer.asByteBuffer();
                                            DataBufferUtils.release(dataBuffer);
                                            return byteBuffer;
                                        })
                                        .collectList()
                                        .flatMap(byteBuffers -> {
                                            // Calculate size of this part
                                            long partSize = byteBuffers.stream()
                                                    .mapToLong(ByteBuffer::remaining)
                                                    .sum();

                                            LOGGER.info("Uploading part {} with size: {} bytes", partNumber, partSize);

                                            // Create a combined ByteBuffer for this part
                                            ByteBuffer combinedBuffer = ByteBuffer.allocate((int) partSize);
                                            byteBuffers.forEach(combinedBuffer::put);
                                            combinedBuffer.flip();

                                            // Upload this part
                                            UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                                                    .bucket(bucket)
                                                    .key(targetPath)
                                                    .uploadId(uploadId)
                                                    .partNumber(partNumber)
                                                    .contentLength(partSize)
                                                    .build();

                                            return Mono.fromFuture(
                                                    s3AsyncClient.uploadPart(
                                                            uploadPartRequest,
                                                            AsyncRequestBody.fromByteBuffer(combinedBuffer)
                                                    )
                                            ).map(uploadPartResponse -> {
                                                LOGGER.info("Uploaded part {} with ETag: {}", partNumber, uploadPartResponse.eTag());
                                                return CompletedPart.builder()
                                                        .partNumber(partNumber)
                                                        .eTag(uploadPartResponse.eTag())
                                                        .build();
                                            });
                                        });
                            });

                    // Step 3: Collect all completed parts and complete the multipart upload
                    return completedPartsFlux
                            .collectList()
                            .flatMap(completedParts -> {
                                LOGGER.info("All {} parts uploaded, completing multipart upload", completedParts.size());

                                CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                                        .parts(completedParts)
                                        .build();

                                CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
                                        .bucket(bucket)
                                        .key(targetPath)
                                        .uploadId(uploadId)
                                        .multipartUpload(completedMultipartUpload)
                                        .build();

                                return Mono.fromFuture(s3AsyncClient.completeMultipartUpload(completeRequest));
                            })
                            .doOnSuccess(response ->
                                    LOGGER.info("Successfully completed multipart upload to s3://{}/{}", bucket, targetPath)
                            )
                            .onErrorResume(error -> {
                                // If anything fails, abort the multipart upload to avoid orphaned parts
                                LOGGER.error("Error during multipart upload, aborting upload ID: {}", uploadId, error);
                                AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
                                        .bucket(bucket)
                                        .key(targetPath)
                                        .uploadId(uploadId)
                                        .build();
                                return Mono.fromFuture(s3AsyncClient.abortMultipartUpload(abortRequest))
                                        .then(Mono.error(error));
                            });
                })
                .then();
    }

    @Override
    public void uploadMultipartFile(S3Request s3UploadObjectRequest, MultipartFile[] files) {
        String targetPath = getS3PathStringFromConfigAndS3Request(s3UploadObjectRequest);

        List<InputStream> multipartFileInputStreams = List.of(files).stream().map(file -> {
            try {
                return file.getInputStream();
            } catch (IOException e) {
                LOGGER.error("IOException uploading file stream to {}/{}", s3ConfigurationProperties.bucketName(), targetPath, e);
                throw new RuntimeException(e);
            }
        }).toList();

        SequenceInputStream multipartsJoinedStream = new SequenceInputStream(Collections.enumeration(multipartFileInputStreams));
        RequestBody body = RequestBody.fromContentProvider(
                ContentStreamProvider.fromInputStream(multipartsJoinedStream), MediaType.APPLICATION_OCTET_STREAM_VALUE);

        LOGGER.info("Uploading multipart into {}/{}", s3ConfigurationProperties.bucketName(), targetPath);

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

            InputStream inputStream = s3Client.getObject(getObjectRequest, ResponseTransformer.toInputStream());
            return inputStream;

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
