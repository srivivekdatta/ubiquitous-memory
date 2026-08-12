package com.example.kafkaapp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

@Service
public class MessageProcessorAsync {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessorAsync.class);

    private final DatabaseService databaseService;
    private final RestApiClientService restApiClientService;
    private final SoapClientService soapClientService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Executor soapApiExecutor;

    public MessageProcessorAsync(
            DatabaseService databaseService,
            RestApiClientService restApiClientService,
            SoapClientService soapClientService,
            KafkaTemplate<String, Object> kafkaTemplate,
            Executor soapApiExecutor) {
        this.databaseService = databaseService;
        this.restApiClientService = restApiClientService;
        this.soapClientService = soapClientService;
        this.kafkaTemplate = kafkaTemplate;
        this.soapApiExecutor = soapApiExecutor;
    }

    @Async("messageProcessingExecutor")
    public CompletableFuture<Void> processMessage(String transactionId, Map<String, Object> messagePayload) {
        log.info("Processing message in Async Thread. TxId: {}", transactionId);
        String payloadJson = messagePayload.toString();

        try {
            // 1. Async insert raw data into db
            databaseService.insertInitialState(transactionId, payloadJson);

            // 2. Populate an instanceobject
            InstanceObject instance = new InstanceObject();
            instance.setTransactionId(transactionId);
            instance.setRawData(payloadJson);

            // 3. Call external api
            log.info("Calling external API for TxId: {}", transactionId);
            String restResponse = restApiClientService.callExternalApi(payloadJson);

            // 4. Set this data to this instanceobject
            instance.setExternalApiResponse(restResponse);

            // 5. Finally call soap api (async on separate thread pool)
            log.info("Submitting SOAP call for TxId: {} to soapApiExecutor", transactionId);
            CompletableFuture.supplyAsync(() -> soapClientService.callExternalSystem(instance.toString()), soapApiExecutor)
                .thenAccept(soapResponse -> {
                    log.info("SOAP call returned for TxId: {}", transactionId);
                    instance.setSoapApiResponse(soapResponse);

                    // 6. Once we have the result, do some db writes
                    databaseService.markAsSuccess(transactionId);

                    // 7. Publish final data thru kafka
                    kafkaTemplate.send("final-transactions-topic", transactionId, instance.toString());
                })
                .exceptionally(ex -> {
                    log.error("SOAP API call failed for TxId: {}, Exception: {}", transactionId, ex.getMessage());
                    databaseService.markAsFailed(transactionId, ex.getMessage());
                    return null;
                });

        } catch (Exception e) {
            log.error("Initial processing failed for TxId: {}, Exception: {}", transactionId, e.getMessage());
            databaseService.markAsFailed(transactionId, e.getMessage());
        }

        // Return immediately so the initial processing thread pool is freed up while the SOAP call happens async
        return CompletableFuture.completedFuture(null);
    }
}
