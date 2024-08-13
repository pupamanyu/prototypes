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
package com.example.config;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public interface KafkaConfig {
  String KAFKA_BOOTSTRAP_SERVERS = "kafka1:9092";
  String KAFKA_TOPIC = "test-topic";
  String KAFKA_GROUP_ID = "some-kafka-group";
  Boolean PRODUCER_ENABLE_ACKS = true;
  Integer KAFKA_PRODUCER_THREADS = 4;
  Integer PRODUCED_EVENTS_PER_SECOND = 10_000;
  String KAFKA_CONSUMER_AUTO_OFFSET_RESET_CONFIG = "earliest";

  static KafkaProducer<byte[], byte[]> getProducer() {
    KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(getProducerProperties());
    return producer;
  }

  static KafkaConsumer<byte[], byte[]> getConsumer() {
    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(getConsumerProperties());
    return consumer;
  }

  private static Properties getProducerProperties() {
    // Configuration properties for the Kafka producer
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.RETRIES_CONFIG, 3);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    if (PRODUCER_ENABLE_ACKS) {
      props.put(ProducerConfig.ACKS_CONFIG, "all");
    } else {
      props.put(ProducerConfig.ACKS_CONFIG, "0");
    }
    return props;
  }

  private static Properties getConsumerProperties() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP_ID);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KAFKA_CONSUMER_AUTO_OFFSET_RESET_CONFIG);
    return props;
  }
}
