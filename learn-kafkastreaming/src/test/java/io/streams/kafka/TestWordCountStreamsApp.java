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
    private String storename;

    @BeforeEach
    public void setup() {

        Serde<String> stringSerde = new Serdes.StringSerde();
        Serde<Long> longSerde = new Serdes.LongSerde();

        Properties config = new Properties();
        try (InputStream ins = this.getClass().getResourceAsStream("/application.properties")){
            config.load(ins);
            storename = config.getProperty("application.storename");
        } catch (IOException e) {
            log.error("{}\n", e.getMessage(), e);
        }
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,stringSerde.getClass().getName());

        WordCountStreamsApp streamApp = WordCountStreamsApp.builder()
                .inboundTopic(config.getProperty("topic.inbound"))
                .outboundTopic(config.getProperty("topic.outbound"))
                .storeName(storename)
                .build();

        Topology topology = streamApp.countStreamTopology();

        testDriver = new TopologyTestDriver(topology, config);

        inputTopic = testDriver.createInputTopic(config.getProperty("topic.inbound"), stringSerde.serializer(), stringSerde.serializer());
        outputTopic = testDriver.createOutputTopic(config.getProperty("topic.outbound"), stringSerde.deserializer(), longSerde.deserializer());

        log.info("Stream app topology -> {}", topology.toString());
    }

    @AfterEach
    public void teardown() {
        testDriver.close();
    }

    @Test
    public void testCountStreamTopology() {
        inputTopic.pipeInput(null, "Hello World");
        inputTopic.pipeInput(null, "world");

        KeyValueStore<String, Long> store = testDriver.getKeyValueStore(storename);
        assertEquals(1L, store.get("hello"));
        assertEquals(2L, store.get("world"));

        List<KeyValue<String, Long>> output = outputTopic.readKeyValuesToList();
        assertEquals(3, output.size());
        assertEquals( "hello", output.get(0).key);
        assertEquals( 1L, output.get(0).value);
        assertEquals( "world", output.get(1).key);
        assertEquals(1L, output.get(1).value);
        assertEquals( "world", output.get(2).key);
        assertEquals(2L, output.get(2).value);

    }
}
