package com.example.kafkaapp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;

@Service
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final DatabaseService databaseService;
    private final SoapClientService soapClientService;

    public KafkaConsumerService(DatabaseService databaseService, SoapClientService soapClientService) {
        this.databaseService = databaseService;
        this.soapClientService = soapClientService;
    }

    /**
     * Consumes JSON messages from the Kafka topic. The concurrency is configured to 300
     * in the KafkaConfig to handle 150 TPS with 2 second processing time.
     * Note: We avoid @Transactional at the class/method level because holding a database
     * connection open during the slow (1-2s) SOAP call would exhaust the connection pool
     * quickly under the 150 TPS load.
     */
    @KafkaListener(topics = "transactions-topic", containerFactory = "kafkaListenerContainerFactory")
    public void consume(Map<String, Object> messagePayload) {
        String transactionId = UUID.randomUUID().toString();
        log.info("Received message to process. TxId: {}", transactionId);

        try {
            // 1. Database Inserts: Record the transaction locally and insert an audit log
            String payloadJson = messagePayload.toString();
            databaseService.insertInitialState(transactionId, payloadJson);

            // 2. Make the synchronous 1-2 sec SOAP API Call
            log.info("Initiating SOAP API Call for TxId: {}", transactionId);
            String soapResponse = soapClientService.callExternalSystem(payloadJson);

            log.info("SOAP call success. TxId: {}, Response: {}", transactionId, soapResponse);

            // 3. Mark the transaction as success in the DB
            databaseService.markAsSuccess(transactionId);

        } catch (Exception e) {
            log.error("SOAP API call failed for TxId: {}, Exception: {}", transactionId, e.getMessage());

            // 4. On failure, set the F flag in DB
            databaseService.markAsFailed(transactionId, e.getMessage());

            // Depending on requirements, we can throw the exception to trigger retry or let it silently finish.
            // If we throw here, Kafka container will retry the message (unless custom error handler configured).
        }
    }
}
