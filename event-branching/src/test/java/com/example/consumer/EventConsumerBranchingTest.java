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

import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;

import com.example.processor.EventTypeAProcessor;
import com.example.processor.EventTypeBProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class EventConsumerBranchingTest {

    @Mock
    private EventTypeAProcessor eventTypeAProcessor;

    @Mock
    private EventTypeBProcessor eventTypeBProcessor;

    private EventConsumer eventConsumer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        eventConsumer = new EventConsumer(mock(KafkaConsumer.class), eventTypeAProcessor, eventTypeBProcessor);
    }

    @Test
    void testProcessRecordEventTypeA() {

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test-topic", 0, 0L, null, null);
        record.headers().add(new RecordHeader("type", "eventTypeA".getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("source", "serviceA".getBytes(StandardCharsets.UTF_8)));

        eventConsumer.processRecord(record);

        verify(eventTypeAProcessor).processRecord(record);
        verify(eventTypeBProcessor, never()).processRecord(any());
    }

    @Test
    void testProcessRecordEventTypeB() {

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test-topic", 0, 0L, null, null);
        record.headers().add(new RecordHeader("type", "eventTypeB".getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("source", "serviceB".getBytes(StandardCharsets.UTF_8)));

        eventConsumer.processRecord(record);

        verify(eventTypeBProcessor).processRecord(record);
        verify(eventTypeAProcessor, never()).processRecord(any());
    }

    @Test
    void testProcessRecordUnknownType() {

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test-topic", 0, 0L, null, null);
        record.headers().add(new RecordHeader("type", "unknownType".getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("source", "unknownService".getBytes(StandardCharsets.UTF_8)));

        eventConsumer.processRecord(record);

        verify(eventTypeAProcessor, never()).processRecord(any());
        verify(eventTypeBProcessor, never()).processRecord(any());
    }
}


