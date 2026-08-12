package com.example.kafkaapp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class SoapClientService {

    private static final Logger log = LoggerFactory.getLogger(SoapClientService.class);

    private final ActimizeSoapService actimizeSoapService;

    public SoapClientService(ActimizeSoapService actimizeSoapService) {
        this.actimizeSoapService = actimizeSoapService;
    }

    /**
     * Executes the actual Apache CXF SOAP API call to Actimize.
     */
    public String callExternalSystem(String payload) throws Exception {
        log.info("Executing CXF SOAP call to Actimize...");
        // This will block the thread (which is running in the dedicated soapApiExecutor pool)
        // until the response returns or the configured 2.5-second timeout is exceeded.
        return actimizeSoapService.checkFraud(payload);
    }
}
