package org.mdaum.techstack.sqs.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.mdaum.techstack.configuration.TechStackConfigurationProperties;
import org.mdaum.techstack.sqs.configuration.SqsConfiguration;
import org.mdaum.techstack.sqs.model.SqsSampleMessage;
import org.mdaum.techstack.sqs.model.SqsSampleMessageAttributes;
import org.mdaum.techstack.sqs.model.SqsSampleMessagePayload;
import org.mdaum.techstack.util.serialization.ObjectMappers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class SqsMessageServiceImpl implements SqsMessageService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqsMessageServiceImpl.class);

    private final SqsClient sqsClient;
    private final String queueUrl;

    @Autowired
    public SqsMessageServiceImpl(SqsConfiguration sqsConfiguration, Region region, AwsCredentialsProvider awsCredentialsProvider) {
        sqsClient = SqsClient.builder()
                .region(region)
                .credentialsProvider(awsCredentialsProvider)
                .build();

        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(sqsConfiguration.queueName())
                .build();
        sqsClient.createQueue(request);

        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(sqsConfiguration.queueName())
                .build();

        queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();

        LOGGER.info("SQS queue {} created in region {}", queueUrl, region.id());
    }

    @Override
    public void sendMessage(SqsSampleMessage sqsSampleMessage) {

        Map<String, MessageAttributeValue> messageAttributeValueMap = Map.of(
                "stringAttribute", MessageAttributeValue.builder()
                        .stringValue(sqsSampleMessage.attributes().stringAttribute())
                        .dataType("String").build(),
                "numberAttribute", MessageAttributeValue.builder()
                        .stringValue(String.valueOf(sqsSampleMessage.attributes().numberAttribute()))
                        .dataType("Number.int").build());

        SendMessageRequest sendMsgRequest;

        try {
            sendMsgRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(ObjectMappers.GENERAL.writeValueAsString(sqsSampleMessage.payload()))
                    .messageAttributes(messageAttributeValueMap)
                    .delaySeconds(0)
                    .build();

            LOGGER.info("Sending SQS message {}", ObjectMappers.GENERAL.writeValueAsString(sqsSampleMessage));
        } catch (JsonProcessingException e) {
            LOGGER.error("Error serializing SQS message", e);
            throw new RuntimeException("Error serializing SQS message", e);
        }
        sqsClient.sendMessage(sendMsgRequest);
    }

    @Override
    public List<SqsSampleMessage> receiveMessages() {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(5)
                .build();

        return sqsClient.receiveMessage(receiveMessageRequest).messages().stream().map(
                    message -> {
                        try {
                            LOGGER.info("Received message {}",
                                    message.body());
                            SqsSampleMessagePayload payload = ObjectMappers.GENERAL
                                    .readValue(message.body(), SqsSampleMessagePayload.class);
                            return new SqsSampleMessage(null, payload);
                        } catch (JsonProcessingException e) {
                            LOGGER.error("Error deserializing SQS message {}", message.body(), e);
                            throw new RuntimeException("Error deserializing SQS message", e);
                        }
                    }
            ).toList();
    }
}
