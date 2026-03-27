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
        // Base thread pool size. With 30 pods, each needs ~10 threads to handle 300 total concurrent processes.
        // We set it to 20 to fully utilize the DB connection pool (max 20) in extreme load scenarios.
        executor.setCorePoolSize(20);
        executor.setMaxPoolSize(20);
        // Queue capacity defines how many messages queue up before CallerRunsPolicy kicks in.
        // A smaller queue prevents too many messages from being stuck in-memory during a pod crash.
        executor.setQueueCapacity(50);
        executor.setThreadNamePrefix("KafkaProcessor-");

        // The user explicitly requested CallerRunsPolicy. If the thread pool and queue are full,
        // the main Kafka consumer thread will execute the task synchronously, essentially throttling the consumer.
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        executor.initialize();
        return executor;
    }
}
