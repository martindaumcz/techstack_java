package org.mdaum.techstack.kafka.exception;

public class UnrecoverableException extends RuntimeException{
    public UnrecoverableException(String message) {
        super(message);
    }
}
