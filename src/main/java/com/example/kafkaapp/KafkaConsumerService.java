package com.example.kafkaapp;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Qualifier;

@Service
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final MessageProcessorAsync messageProcessorAsync;
    private final Executor messageProcessingExecutor;

    public KafkaConsumerService(
            MessageProcessorAsync messageProcessorAsync,
            @Qualifier("messageProcessingExecutor") Executor messageProcessingExecutor) {
        this.messageProcessorAsync = messageProcessorAsync;
        this.messageProcessingExecutor = messageProcessingExecutor;
    }

    /**
     * Consumes a batch of JSON messages (up to 50) from the Kafka topic.
     * We hand off the messages to an Async thread pool to process them concurrently.
     * We then block the main Kafka thread to wait for the entire batch to finish processing.
     * This prevents message loss in case of a pod crash.
     */
    @KafkaListener(topics = "transactions-topic", containerFactory = "kafkaListenerContainerFactory")
    public void consumeBatch(List<ConsumerRecord<String, String>> records) {
        log.info("Received batch of {} messages from Kafka. Processing asynchronously...", records.size());

        // Handoff each message in the batch to the primary async executor and collect the Futures
        List<CompletableFuture<Void>> futures = records.stream().map(record -> {
            log.info("Processing record - Partition: {}, Offset: {}", record.partition(), record.offset());

            String transactionId = UUID.randomUUID().toString();
            // Since we removed @Async from processMessage, we manually submit the initial processing
            // to the messageProcessingExecutor, which then chains to the soapApiExecutor.
            return CompletableFuture.supplyAsync(() -> {
                return messageProcessorAsync.processMessage(transactionId, record.value());
            }, messageProcessingExecutor).thenCompose(f -> f);
        }).collect(Collectors.toList());

        // Wait for all messages in the batch to finish processing
        CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        allOf.join(); // Blocks the Kafka thread until the batch is complete

        log.info("Successfully processed batch of {} messages.", messages.size());
    }
}
