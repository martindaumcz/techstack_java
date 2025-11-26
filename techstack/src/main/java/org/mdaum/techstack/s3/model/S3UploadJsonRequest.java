package org.mdaum.techstack.s3.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class S3UploadJsonRequest extends S3Request {

    private final Object jsonObject;

    @JsonCreator
    public static S3UploadJsonRequest of(
            @JsonProperty("fileName") String fileName,
            @JsonProperty("jsonObject") Object jsonObject) {
        return new S3UploadJsonRequest(fileName, jsonObject);
    }

    protected S3UploadJsonRequest(String fileName, Object jsonObject) {
        super(fileName);
        this.jsonObject = jsonObject;
    }

    public Object getJsonObject() {
        return jsonObject;
    }
}
