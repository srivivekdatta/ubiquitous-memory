package com.example.kafkaapp;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableAsync
public class AsyncConfig {

    @Bean(name = "messageProcessingExecutor")
    public Executor messageProcessingExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // Thread pool sized to guarantee immediate processing of a full Kafka batch (max.poll.records=50)
        // Set to 60 to provide headroom and align with Hikari DB max-pool-size of 70.
        executor.setCorePoolSize(60);
        executor.setMaxPoolSize(60);
        // Queue capacity defines how many messages queue up before CallerRunsPolicy kicks in.
        // Set to 50 to accommodate batch sizes comfortably.
        executor.setQueueCapacity(50);
        executor.setThreadNamePrefix("KafkaProcessor-");

        // The user explicitly requested CallerRunsPolicy. If the thread pool and queue are full,
        // the main Kafka consumer thread will execute the task synchronously, essentially throttling the consumer.
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        executor.initialize();
        return executor;
    }

    @Bean(name = "soapApiExecutor")
    public Executor soapApiExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // Separate pool for the async SOAP API calls to prevent blocking the initial processing threads.
        // Sized identically to the main pool to ensure a 1:1 throughput capability.
        executor.setCorePoolSize(60);
        executor.setMaxPoolSize(60);
        executor.setQueueCapacity(50);
        executor.setThreadNamePrefix("SoapExecutor-");

        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }
}
