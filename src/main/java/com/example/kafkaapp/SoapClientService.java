package com.example.kafkaapp;

import org.springframework.stereotype.Service;

@Service
public class SoapClientService {

    /**
     * Simulates an Actimize SOAP API call that takes 1 to 2 seconds.
     * With CXF connection/request timeouts configured at 2.5 seconds, this simulates
     * both success and occasional timeout failures.
     */
    public String callExternalSystem(String payload) throws Exception {
        // Raw info demonstration: simulate typical 1-2 seconds latency
        long latency = (long) (1000 + Math.random() * 1000);

        // Randomly simulate a timeout scenario where latency exceeds the 2.5s CXF limit
        if (Math.random() < 0.05) { // 5% chance of timeout
            latency = 3000;
        }

        try {
            Thread.sleep(latency);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (latency >= 2500) {
            throw new RuntimeException("Actimize SOAP Web Service call failed: Read timed out");
        }

        return "ACTIMIZE_FRAUD_CHECK_SUCCESS_" + System.currentTimeMillis();
    }
}
