package com.example.kafkaapp;

import org.springframework.stereotype.Service;

@Service
public class SoapClientService {

    /**
     * Simulates a SOAP API call that takes 1 to 2 seconds.
     * In a real application, you would inject the generated CXF client Port type here
     * and invoke the web service method.
     */
    public String callExternalSystem(String payload) throws Exception {
        // Raw info demonstration: simulate 1.5 seconds latency
        try {
            Thread.sleep((long) (1000 + Math.random() * 1000));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Randomly simulate success or failure (e.g. 10% failure rate)
        if (Math.random() < 0.1) {
            throw new RuntimeException("SOAP Web Service call failed: Connection Timeout");
        }

        return "SUCCESS_RESPONSE_" + System.currentTimeMillis();
    }
}
