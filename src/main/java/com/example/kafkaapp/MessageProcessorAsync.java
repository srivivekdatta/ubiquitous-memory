package com.example.kafkaapp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

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
    private final Gson gson = new Gson();

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

    // We intentionally removed @Async here because returning a CompletableFuture from an @Async
    // method causes Spring to implicitly wait/block the caller thread on the returned future,
    // destroying the separate thread pool separation.
    // Instead, the first part runs on whatever thread calls it (Kafka thread or primary pool),
    // and the SOAP call is explicitly handed off to the soapApiExecutor.
    public CompletableFuture<Void> processMessage(String transactionId, String rawMessagePayload) {
        log.info("Processing message for TxId: {}", transactionId);

        try {
            // Ensure payload is valid JSON (this will throw if it's completely malformed)
            JsonObject jsonObject = gson.fromJson(rawMessagePayload, JsonObject.class);
            // Optionally, you can extract the transactionId from the JSON here if it exists,
            // e.g. transactionId = jsonObject.has("txId") ? jsonObject.get("txId").getAsString() : transactionId;

            // 1. Async insert raw data into db
            databaseService.insertInitialState(transactionId, rawMessagePayload);

            // 2. Populate an instanceobject
            InstanceObject instance = new InstanceObject();
            instance.setTransactionId(transactionId);
            instance.setRawData(rawMessagePayload);

            // 3. Call external api
            log.info("Calling external API for TxId: {}", transactionId);
            String restResponse = restApiClientService.callExternalApi(rawMessagePayload);

            // 4. Set this data to this instanceobject
            instance.setExternalApiResponse(restResponse);

            // 5. Finally call soap api (async on separate thread pool)
            log.info("Submitting SOAP call for TxId: {} to soapApiExecutor", transactionId);
            return CompletableFuture.supplyAsync(() -> {
                    try {
                        return soapClientService.callExternalSystem(instance.toString());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, soapApiExecutor)
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
            return CompletableFuture.completedFuture(null);
        }
    }
}
