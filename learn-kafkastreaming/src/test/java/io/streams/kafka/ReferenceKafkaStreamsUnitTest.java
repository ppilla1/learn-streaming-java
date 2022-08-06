package io.streams.kafka;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@Log4j2
public class ReferenceKafkaStreamsUnitTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Long> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;
    private KeyValueStore<String, Long> store;

    private Serde<String> stringSerde = new Serdes.StringSerde();
    private Serde<Long> longSerde = new Serdes.LongSerde();

    @BeforeEach
    public void setup() {
        Topology topology = new Topology();
        topology.addSource("sourceProcessor", "input-topic");
        topology.addProcessor("aggregator", new CustomMaxAggregatorSupplier(), "sourceProcessor");
        topology.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("aggStore"),
                        Serdes.String(),
                        Serdes.Long()).withLoggingDisabled(), // need to disable logging to allow store pre-populating
                "aggregator");
        topology.addSink("sinkProcessor", "result-topic", "aggregator");

        // setup test driver
        Properties props = new Properties();
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        testDriver = new TopologyTestDriver(topology, props);

        // setup test topics
        inputTopic = testDriver.createInputTopic("input-topic", stringSerde.serializer(), longSerde.serializer());
        outputTopic = testDriver.createOutputTopic("result-topic", stringSerde.deserializer(), longSerde.deserializer());

        // pre-populate store
        store = testDriver.getKeyValueStore("aggStore");
        store.put("a", 21L);
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void shouldFlushStoreForFirstInput() {
        inputTopic.pipeInput("a", 1L);
        KeyValue<String, Long> output = outputTopic.readKeyValue();
        assertEquals(output, new KeyValue<>("a", 21L));
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void shouldNotUpdateStoreForSmallerValue() {
        inputTopic.pipeInput("a", 1L);
        assertEquals(store.get("a"), 21L);
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("a", 21L));
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void shouldNotUpdateStoreForLargerValue() {
        inputTopic.pipeInput("a", 42L);
        assertEquals(store.get("a"), 42L);
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("a", 42L));
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void shouldUpdateStoreForNewKey() {
        inputTopic.pipeInput("b", 21L);
        assertEquals(store.get("b"), 21L);
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("a", 21L));
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("b", 21L));
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void shouldPunctuateIfEvenTimeAdvances() {
        final Instant recordTime = Instant.now();
        inputTopic.pipeInput("a", 1L,  recordTime);
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("a", 21L));

        inputTopic.pipeInput("a", 1L,  recordTime);
        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("a", 1L, recordTime.plusSeconds(10L));
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("a", 21L));
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void shouldPunctuateIfWallClockTimeAdvances() {
        testDriver.advanceWallClockTime(Duration.ofSeconds(60));
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("a", 21L));
        assertTrue(outputTopic.isEmpty());
    }

    public class CustomMaxAggregatorSupplier implements ProcessorSupplier<String, Long, String, Long> {

        @Override
        public Processor<String, Long, String, Long> get() {
            return new CustomMaxAggregator();
        }

    }

    public class CustomMaxAggregator implements Processor<String, Long, String, Long> {
        ProcessorContext<String, Long> context;
        private KeyValueStore<String, Long> store;

        private void flushStore() {
            KeyValueIterator<String, Long> it = store.all();
            while (it.hasNext()) {
                KeyValue<String, Long> next = it.next();
                Record<String, Long> record = new Record<>(next.key, next.value, System.currentTimeMillis());
                context.forward(record);
            }
        }

        @Override
        public void init(ProcessorContext<String, Long> context) {
            this.context = context;
            context.schedule(Duration.ofSeconds(60), PunctuationType.WALL_CLOCK_TIME, time -> flushStore());
            context.schedule(Duration.ofSeconds(10), PunctuationType.STREAM_TIME, time -> flushStore());
            store = (KeyValueStore<String, Long>) context.getStateStore("aggStore");
        }

        @Override
        public void process(Record<String, Long> record) {
            Long oldValue = store.get(record.key());
            if (oldValue == null || record.value() > oldValue) {
                store.put(record.key(), record.value());
            }
        }

        @Override
        public void close() {}
    }
}
