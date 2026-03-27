package com.example.kafkaapp;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.beans.factory.annotation.Value;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(ConsumerFactory<String, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        // Use the autoconfigured consumer factory from application.yml
        factory.setConsumerFactory(consumerFactory);

        // Since there are only 15-20 partitions and 30-32 pods, most pods will handle 0 or 1 partition.
        // Concurrency is set to 1 per pod. High throughput is achieved by handing off the batch
        // to an async ThreadPoolTaskExecutor with CallerRunsPolicy.
        factory.setConcurrency(1);

        // Enable batch listening explicitly for safety
        factory.setBatchListener(true);

        return factory;
    }
}
