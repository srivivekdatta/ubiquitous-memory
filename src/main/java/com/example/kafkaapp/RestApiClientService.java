package com.example.kafkaapp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class RestApiClientService {

    private static final Logger log = LoggerFactory.getLogger(RestApiClientService.class);

    public String callExternalApi(String payload) {
        log.info("Calling initial external REST API...");
        // Mock external API latency
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return "REST_API_SUCCESS_DATA";
    }
}
