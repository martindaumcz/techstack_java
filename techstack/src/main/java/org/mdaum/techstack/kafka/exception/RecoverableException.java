package org.mdaum.techstack.kafka.exception;

public class RecoverableException extends RuntimeException {

    public RecoverableException(String message) {
        super(message);
    }

}
