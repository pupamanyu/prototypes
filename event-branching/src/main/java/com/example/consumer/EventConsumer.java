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
package com.example.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.example.processor.EventTypeAProcessor;
import com.example.processor.EventTypeBProcessor;
import com.example.config.KafkaConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class EventConsumer {

  private final KafkaConsumer<byte[], byte[]> consumer;
  private final String topic;
  private final Map<TopicPartition, OffsetAndMetadata> currentOffsets =
      new HashMap<>(); // Store current offsets for manual commits
  EventTypeAProcessor eventTypeAProcessor = new EventTypeAProcessor();
  EventTypeBProcessor eventTypeBProcessor = new EventTypeBProcessor();
  private volatile boolean running = true;

  public EventConsumer() {
    this.topic = KafkaConfig.KAFKA_TOPIC;

    // Configuration properties for the Kafka consumer
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.KAFKA_BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConfig.KAFKA_GROUP_ID);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        KafkaConfig.KAFKA_CONSUMER_AUTO_OFFSET_RESET_CONFIG);

    this.consumer = KafkaConfig.getConsumer();
  }

  public EventConsumer(
      KafkaConsumer<byte[], byte[]> consumer,
      EventTypeAProcessor eventTypeAProcessor,
      EventTypeBProcessor eventTypeBProcessor) {
    this.consumer = consumer;
    this.eventTypeAProcessor = eventTypeAProcessor;
    this.eventTypeBProcessor = eventTypeBProcessor;
    this.topic = KafkaConfig.KAFKA_TOPIC;
  }

  // Run method to start the Kafka consumer
  public static void run() {
    // Instantiate the Kafka consumer
    EventConsumer kafkaConsumer = new EventConsumer();

    // Add a shutdown hook to gracefully shut down the consumer when the application is terminated
    Runtime.getRuntime().addShutdownHook(new Thread(kafkaConsumer::shutdown));

    // Start the Kafka consumer
    kafkaConsumer.start();
  }

  public void start() {
    try {
      // Subscribe to the topic with a custom ConsumerRebalanceListener
      consumer.subscribe(Collections.singletonList(topic), new HandleRebalanced());

      while (running) {
        // Poll for records (messages) from the Kafka topic with a timeout of 1000ms
        ConsumerRecords<byte[], byte[]> records = null;
        try {
          records = consumer.poll(Duration.ofMillis(1000));
        } catch (WakeupException e) {
          if (!running) {
            break; // Exit the loop if shutdown has been initiated
          }
          throw e;
        } catch (Exception e) {
          System.err.println("Error during polling: " + e.getMessage());
        }

        // Process the records if any are returned by the poll operation
        if (records != null && !records.isEmpty()) {
          for (ConsumerRecord<byte[], byte[]> record : records) {
            try {
              processRecord(record);
              // Update the current offset for the partition after processing the record
              currentOffsets.put(
                  new TopicPartition(record.topic(), record.partition()),
                  new OffsetAndMetadata(record.offset() + 1));
            } catch (Exception e) {
              System.err.println("Error processing record. " + "Error: " + e.getMessage());
            }
          }

          try {
            // Commit the offsets synchronously after processing the batch of records
            consumer.commitSync(currentOffsets);
          } catch (Exception e) {
            System.err.println("Error committing offsets: " + e.getMessage());
          }
        }
      }
    } catch (WakeupException e) {
      if (running) {
        System.err.println("Unexpected WakeupException: " + e.getMessage());
      }
    } catch (Exception e) {
      System.err.println("Unexpected error: " + e.getMessage());
    } finally {
      closeConsumer();
    }
  }

  // Method to process each record (message) from Kafka
  void processRecord(ConsumerRecord<byte[], byte[]> record) {
    // Implement your processing logic here; this is where you handle the message
    Headers headers = record.headers();
    String eventType = new String(headers.lastHeader("type").value());
    String source = new String(headers.lastHeader("source").value());
    if ("eventTypeA".equals(eventType) && "serviceA".equals(source)) {
      // Route to processor A
      eventTypeAProcessor.processRecord(record);
    } else if ("eventTypeB".equals(eventType) && "serviceB".equals(source)) {
      // Route to processor B
      eventTypeBProcessor.processRecord(record);
    }
    // Add additional branching logic as needed
  }

  // Gracefully shut down the consumer
  public void shutdown() {
    running = false; // Set the running flag to false for exiting the loop
    consumer.wakeup(); // Wake up the consumer to exit the blocking poll operation
  }

  // Close the Kafka consumer and release resources
  private void closeConsumer() {
    try {
      consumer.commitSync(currentOffsets); // Commit offsets before closing
      consumer.close(); // Close the consumer to release resources
    } catch (Exception e) {
      System.err.println("Error closing Kafka consumer: " + e.getMessage());
    }
  }

  // Custom ConsumerRebalancedListener to handle partition rebalancing
  private class HandleRebalanced implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      // Before re-balancing, commit the offsets for the partitions being revoked
      System.out.println("Partitions revoked: " + partitions);
      try {
        consumer.commitSync(currentOffsets);
      } catch (Exception e) {
        System.err.println("Error committing offsets during re-balance: " + e.getMessage());
      }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
      // After re-balancing, reset any state if needed or initialize resources
      System.out.println("Partitions assigned: " + partitions);
      // You might reset offsets or reinitialize the state here if necessary
    }
  }
}
