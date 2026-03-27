package com.example.kafkaapp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class MessageProcessorAsync {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessorAsync.class);

    private final DatabaseService databaseService;
    private final SoapClientService soapClientService;

    public MessageProcessorAsync(DatabaseService databaseService, SoapClientService soapClientService) {
        this.databaseService = databaseService;
        this.soapClientService = soapClientService;
    }

    /**
     * This method executes asynchronously in the configured ThreadPoolTaskExecutor.
     * It allows us to process significantly more messages concurrently than the number
     * of Kafka partitions, while adhering strictly to the maximum Hikari DB connection pool size.
     */
    @Async("messageProcessingExecutor")
    public void processMessage(String transactionId, Map<String, Object> messagePayload) {
        log.info("Processing message in Async Thread. TxId: {}", transactionId);

        try {
            // 1. Database Inserts: Record the transaction locally and insert an audit log
            String payloadJson = messagePayload.toString();
            databaseService.insertInitialState(transactionId, payloadJson);

            // 2. Make the synchronous 1-2 sec SOAP API Call to Actimize
            log.info("Initiating Actimize SOAP API Call for TxId: {}", transactionId);
            String soapResponse = soapClientService.callExternalSystem(payloadJson);

            log.info("Actimize SOAP call success. TxId: {}, Response: {}", transactionId, soapResponse);

            // 3. Mark the transaction as success in the DB
            databaseService.markAsSuccess(transactionId);

        } catch (Exception e) {
            log.error("Actimize SOAP API call failed for TxId: {}, Exception: {}", transactionId, e.getMessage());

            // 4. On failure, set the F flag in DB
            databaseService.markAsFailed(transactionId, e.getMessage());
        }
    }
}
