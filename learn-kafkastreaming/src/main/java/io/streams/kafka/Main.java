package io.streams.kafka;

import io.streams.kafka.stream.WordCountStreamsApp;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;

@Log4j2
public class Main {

    public static void main(String[] args) {


        try(InputStream ins = Main.class.getResourceAsStream("/application.properties")) {

            Properties config = new Properties();
            config.load(ins);
            config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
            config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

            WordCountStreamsApp app = WordCountStreamsApp.builder()
                                        .inboundTopic(config.getProperty("topic.inbound"))
                                        .outboundTopic(config.getProperty("topic.outbound"))
                                        .storeName(config.getProperty("application.storename"))
                                        .build();

            KafkaStreams streams = new KafkaStreams(app.countStreamTopology(), config);

            log.info("Stream topology loaded -> {}", streams.toString());
            Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close(Duration.ofSeconds(10l))));

        } catch (IOException e) {
            log.error("{}\n", e.getMessage(), e);
        }


    }
}
