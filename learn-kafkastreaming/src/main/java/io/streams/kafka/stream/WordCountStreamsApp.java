package io.streams.kafka.stream;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;

@Builder
@Log4j2
@Getter
public class WordCountStreamsApp
{
    private final String inboundTopic;
    private final String outboundTopic;
    private final String storeName;

    public Topology countStreamTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        // Create materialized state store with key of type "String" and value of type "Long"
        Materialized<String, Long, KeyValueStore<Bytes, byte[]>> store = Materialized
                                                                        .<String, Long, KeyValueStore<Bytes, byte[]>>as(getStoreName());

        // 1 - Fetch value stream from Kafka inboud topic
        KStream<String, String> sentences = builder.stream(getInboundTopic());

        // 2 - Transform each values to lowercase
        KTable<String, Long> wordCounts = sentences
                                        .mapValues(textLine -> textLine.toLowerCase())
                                        // 3 - Split each values by space into multiple values
                                        .flatMapValues(lowercaseTextLine -> Arrays.asList(lowercaseTextLine.split(" ")))
                                        // 4 - Replace old key of values by value itself
                                        .selectKey((ignoredKey, word) -> word)
                                        // 5 - Group by key before aggregation
                                        .groupByKey()
                                        // 6 - Aggregate count for each values and store in state
                                        .count(store);

        // 7 - Write count stream to Kafka outbound topic
        wordCounts.toStream().to(getOutboundTopic(), Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }
}
