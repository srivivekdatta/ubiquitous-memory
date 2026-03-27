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

    private final MessageProcessorAsync messageProcessorAsync;

    public KafkaConsumerService(MessageProcessorAsync messageProcessorAsync) {
        this.messageProcessorAsync = messageProcessorAsync;
    }

    /**
     * Consumes JSON messages from the Kafka topic. Since there are only 15-20 partitions
     * across 30 pods, Kafka concurrency is 1. We hand off the message to an Async thread pool
     * immediately so the Kafka consumer is unblocked.
     */
    @KafkaListener(topics = "transactions-topic", containerFactory = "kafkaListenerContainerFactory")
    public void consume(Map<String, Object> messagePayload) {
        String transactionId = UUID.randomUUID().toString();
        log.info("Received message from Kafka. Handing off to Async Executor. TxId: {}", transactionId);

        // Handoff to async processing queue.
        // With CallerRunsPolicy, if the internal thread pool is saturated,
        // the Kafka thread itself will run processMessage synchronously, throttling Kafka naturally.
        messageProcessorAsync.processMessage(transactionId, messagePayload);
    }
}
