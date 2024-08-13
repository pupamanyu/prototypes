/*
 * Copyright 2024

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.producer;

import com.example.config.KafkaConfig;
import com.example.generator.EventGenerator;
import com.google.common.util.concurrent.RateLimiter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;

public class EventProducer {

  private final KafkaProducer<byte[], byte[]> producer;
  private final ExecutorService executorService;
  private final RateLimiter rateLimiter;
  private volatile boolean running = true;

  public EventProducer() {
    this.producer = KafkaConfig.getProducer();
    this.executorService = Executors.newFixedThreadPool(KafkaConfig.KAFKA_PRODUCER_THREADS);
    this.rateLimiter =
        RateLimiter.create(KafkaConfig.PRODUCED_EVENTS_PER_SECOND); // Limit the message rate
  }

  // Run method to start the Kafka producer
  public static void run() {
    // Instantiate the Kafka producer
    EventProducer kafkaProducer = new EventProducer();

    // Add a shutdown hook to gracefully shut down the producer when the application is terminated
    Runtime.getRuntime().addShutdownHook(new Thread(kafkaProducer::shutdown));

    // Start the Kafka producer
    kafkaProducer.start();
  }

  // Start the producer to send messages in a multithreaded fashion
  public void start() {
    for (int i = 0; i < KafkaConfig.KAFKA_PRODUCER_THREADS; i++) {
      executorService.submit(
          () -> {
            while (running) {
              rateLimiter.acquire(); // Block until we are allowed to proceed
              ProducerRecord<byte[], byte[]> record = EventGenerator.getEvent();
              sendMessage(record);
            }
          });
    }
  }

  // Method to send a message asynchronously
  private void sendMessage(ProducerRecord<byte[], byte[]> record) {
    producer.send(
        record,
        (metadata, exception) -> {
          if (exception != null) {
            System.err.println("Error sending message. Error: " + exception.getMessage());
            if (exception instanceof TimeoutException) {
              System.err.println("Timeout occurred while sending message.");
            }
          } else {
            System.out.printf(
                "Sent record with meta(partition=%d, offset=%d)%n",
                metadata.partition(), metadata.offset());
          }
        });
  }

  // Gracefully shut down the producer
  public void shutdown() {
    running = false; // Stop all producer threads
    executorService.shutdown(); // Shutdown executor service
    try {
      if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
    } finally {
      closeProducer(); // Close Kafka producer
    }
  }

  // Close the Kafka producer
  private void closeProducer() {
    try {
      producer.flush(); // Ensure all messages are sent before closing
      producer.close(); // Close the producer to release resources
    } catch (Exception e) {
      System.err.println("Error closing Kafka producer: " + e.getMessage());
    }
  }
}
