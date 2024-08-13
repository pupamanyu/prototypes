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
package com.example.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EventTypeAProcessor implements EventProcessor {
  @Override
  public void processRecord(ConsumerRecord<byte[], byte[]> record) {
    // TODO: implement logic for processing Event Type A here
    printRecordMeta(record);
  }

  private void printRecordMeta(ConsumerRecord<byte[], byte[]> record) {
    String eventSource = new String(record.headers().lastHeader("source").value());
    String eventType = new String(record.headers().lastHeader("type").value());
    System.out.printf(
        "Consumed record with meta(source=%s, type=%s, partition=%d, offset=%d)%n",
        eventSource, eventType, record.partition(), record.offset());
  }
}
