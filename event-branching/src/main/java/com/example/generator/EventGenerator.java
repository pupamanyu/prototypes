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
package com.example.generator;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.example.config.KafkaConfig;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

public interface EventGenerator {

  List<String> EVENT_TYPES = List.of("eventTypeA", "eventTypeB");
  List<String> SERVICE_TYPES = List.of("serviceA", "serviceB");

  /**
   * Generates a random string of the specified length and returns it as a byte array.
   *
   * @param length The length of the random string to generate.
   * @return A byte array containing the random string data.
   */
  static byte[] generateRandomData(int length) {
    // Generate a random alphanumeric string using Apache Commons Lang
    String randomString = RandomStringUtils.randomAlphanumeric(length);

    // Convert the generated string to a byte array using UTF-8 encoding
    return randomString.getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Randomly selects one element from an immutable list and returns it.
   *
   * @param <T> The type of elements in the list.
   * @param list The immutable list to select an element from.
   * @return A randomly selected element from the list.
   */
  static <T> T getRandomElement(List<T> list) {

    // Get a random index within the bounds of the list
    int randomIndex = ThreadLocalRandom.current().nextInt(0, list.size());

    // Return the element at the random index
    return list.get(randomIndex);
  }

  static ProducerRecord<byte[], byte[]> getEvent() {
    List<Header> headers =
        List.of(
            new RecordHeader(
                "type", getRandomElement(EVENT_TYPES).getBytes(StandardCharsets.UTF_8)),
            new RecordHeader(
                "source", getRandomElement(SERVICE_TYPES).getBytes(StandardCharsets.UTF_8)),
            new RecordHeader("timestamp", Instant.now().toString().getBytes()));
    ProducerRecord<byte[], byte[]> record =
        new ProducerRecord<>(
            KafkaConfig.KAFKA_TOPIC, null, generateRandomData(9), generateRandomData(9), headers);
    return record;
  }
}
