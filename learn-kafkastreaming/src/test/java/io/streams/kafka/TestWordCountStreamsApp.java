package io.streams.kafka;

import io.streams.kafka.stream.WordCountStreamsApp;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

@Log4j2
public class TestWordCountStreamsApp {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;
    private final String STORENAME = "Counts";

    @BeforeEach
    public void setup() {

        Serde<String> stringSerde = new Serdes.StringSerde();
        Serde<Long> longSerde = new Serdes.LongSerde();

        Properties config = new Properties();
        try (InputStream ins = this.getClass().getResourceAsStream("/application.properties")){
            config.load(ins);
        } catch (IOException e) {
            log.error("{}\n", e.getMessage(), e);
        }
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,stringSerde.getClass().getName());

        String inboudtopic = "wordcount-input";
        String outboundtopic = "wordcount-output";

        WordCountStreamsApp streamApp = WordCountStreamsApp.builder()
                .inboundTopic(inboudtopic)
                .outboundTopic(outboundtopic)
                .storeName(STORENAME)
                .build();

        Topology topology = streamApp.countStreamTopology();

        testDriver = new TopologyTestDriver(topology, config);

        inputTopic = testDriver.createInputTopic(inboudtopic, stringSerde.serializer(), stringSerde.serializer());
        outputTopic = testDriver.createOutputTopic(outboundtopic, stringSerde.deserializer(), longSerde.deserializer());

        log.info("Stream app topology -> {}", topology.toString());
    }

    @AfterEach
    public void teardown() {
        testDriver.close();
    }

    @Test
    public void testCountStreamTopology() {
        inputTopic.pipeInput(null, "Hello World");

        KeyValueStore<String, Long> store = testDriver.getKeyValueStore(STORENAME);
        assertEquals(store.get("hello"), 1L);
        assertEquals(store.get("world"), 1L);

        List<KeyValue<String, Long>> output = outputTopic.readKeyValuesToList();
        assertEquals(2, output.size());
        assertEquals(output.get(0).key, "hello");
        assertEquals(output.get(0).value, 1L);
        assertEquals(output.get(0).key, "hello");
        assertEquals(output.get(0).value, 1L);
    }
}
