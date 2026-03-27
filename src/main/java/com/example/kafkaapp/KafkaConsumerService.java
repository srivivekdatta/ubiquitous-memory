package com.example.kafkaapp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final MessageProcessorAsync messageProcessorAsync;

    public KafkaConsumerService(MessageProcessorAsync messageProcessorAsync) {
        this.messageProcessorAsync = messageProcessorAsync;
    }

    /**
     * Consumes a batch of JSON messages (up to 50) from the Kafka topic.
     * We hand off the messages to an Async thread pool to process them concurrently.
     * We then block the main Kafka thread to wait for the entire batch to finish processing.
     * This prevents message loss in case of a pod crash.
     */
    @KafkaListener(topics = "transactions-topic", containerFactory = "kafkaListenerContainerFactory")
    public void consumeBatch(List<Map<String, Object>> messages) {
        log.info("Received batch of {} messages from Kafka. Processing asynchronously...", messages.size());

        // Handoff each message in the batch to the async executor and collect the Futures
        List<CompletableFuture<Void>> futures = messages.stream().map(messagePayload -> {
            String transactionId = UUID.randomUUID().toString();
            // With CallerRunsPolicy configured in the Async pool, if the queue is full,
            // the map operation running on the main Kafka thread will simply execute
            // the method synchronously, providing perfect natural backpressure.
            return messageProcessorAsync.processMessage(transactionId, messagePayload);
        }).collect(Collectors.toList());

        // Wait for all messages in the batch to finish processing
        CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        allOf.join(); // Blocks the Kafka thread until the batch is complete

        log.info("Successfully processed batch of {} messages.", messages.size());
    }
}
