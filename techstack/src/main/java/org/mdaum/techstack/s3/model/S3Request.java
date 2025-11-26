package org.mdaum.techstack.s3.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class S3Request {

    private final String fileName;

    @JsonCreator
    public static S3Request of(@JsonProperty("fileName") String fileName) {
        return new S3Request(fileName);
    }

    protected S3Request(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }
}
