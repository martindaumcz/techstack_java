package org.mdaum.techstack.s3.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class S3UploadStringRequest extends S3Request {

    private final String content;

    @JsonCreator
    public static S3UploadStringRequest of(
            @JsonProperty("fileName") String fileName,
            @JsonProperty("content") String string) {
        return new S3UploadStringRequest(fileName, string);
    }

    protected S3UploadStringRequest(String fileName, String content) {
        super(fileName);
        this.content = content;
    }

    public String getContent() {
        return content;
    }
}
