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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EventGeneratorTest {

  @Test
  void testGenerateRandomData() {
    int length = 10;
    byte[] data = EventGenerator.generateRandomData(length);

    assertNotNull(data);
    assertEquals(length, data.length);
  }

  @Test
  void testGetRandomElement() {
    List<String> list = List.of("A", "B", "C");
    String randomElement = EventGenerator.getRandomElement(list);

    assertNotNull(randomElement);
    assertTrue(list.contains(randomElement));
  }

  @Test
  void testGetEvent() {
    ProducerRecord<byte[], byte[]> event = EventGenerator.getEvent();

    assertNotNull(event);
    assertNotNull(event.key());
    assertNotNull(event.value());
    assertNotNull(event.headers());

    List<Header> headers = List.of(event.headers().toArray());
    assertEquals(3, headers.size());

    for (Header header : headers) {
      switch (header.key()) {
        case "type":
          assertTrue(
              EventGenerator.EVENT_TYPES.contains(
                  new String(header.value(), StandardCharsets.UTF_8)));
          break;
        case "source":
          assertTrue(
              EventGenerator.SERVICE_TYPES.contains(
                  new String(header.value(), StandardCharsets.UTF_8)));
          break;
        case "timestamp":
          Instant timestamp = Instant.parse(new String(header.value(), StandardCharsets.UTF_8));
          assertNotNull(timestamp);
          break;
        default:
          fail("Unexpected header key: " + header.key());
      }
    }
  }
}
