package org.mdaum.techstack.kafka.exception;

import reactor.kafka.receiver.ReceiverRecord;

public class RecordProcessingException extends RuntimeException{

    private final ReceiverRecord<Object, Object> receiverRecord;

    public RecordProcessingException(Throwable cause, ReceiverRecord<Object, Object> receiverRecord) {
        super(cause);
        this.receiverRecord = receiverRecord;
    }

    public ReceiverRecord<Object, Object> getReceiverRecord() {
        return receiverRecord;
    }
}
