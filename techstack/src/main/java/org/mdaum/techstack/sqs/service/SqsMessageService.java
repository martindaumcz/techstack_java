package org.mdaum.techstack.sqs.service;

import org.mdaum.techstack.sqs.model.SqsSampleMessage;

import java.util.List;

public interface SqsMessageService {

    void sendMessage(SqsSampleMessage sqsSampleMessage);
    List<SqsSampleMessage> receiveMessages();
}
