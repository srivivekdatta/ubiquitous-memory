package com.example.kafkaapp;

public class InstanceObject {
    private String transactionId;
    private String rawData;
    private String externalApiResponse;
    private String soapApiResponse;

    public InstanceObject() {}

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getRawData() {
        return rawData;
    }

    public void setRawData(String rawData) {
        this.rawData = rawData;
    }

    public String getExternalApiResponse() {
        return externalApiResponse;
    }

    public void setExternalApiResponse(String externalApiResponse) {
        this.externalApiResponse = externalApiResponse;
    }

    public String getSoapApiResponse() {
        return soapApiResponse;
    }

    public void setSoapApiResponse(String soapApiResponse) {
        this.soapApiResponse = soapApiResponse;
    }

    @Override
    public String toString() {
        return "{" +
                "\"transactionId\":\"" + transactionId + '\"' +
                ", \"rawData\":\"" + rawData + '\"' +
                ", \"externalApiResponse\":\"" + externalApiResponse + '\"' +
                ", \"soapApiResponse\":\"" + soapApiResponse + '\"' +
                '}';
    }
}
