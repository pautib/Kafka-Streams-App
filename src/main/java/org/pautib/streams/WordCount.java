package org.pautib.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.pautib.KafkaUtils;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

public class WordCount {

    public static void main(String[] args) {

        Properties wordProps = KafkaUtils.getKafkaDefaultProps("streams-wordcount", "localhost:9092");

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("streams-plaintext-input");
        //stream.print(Printed.toSysOut());

        KStream<String, String> lowerCaseWords = stream.flatMapValues( value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")) );
        //lowerCaseWords.print(Printed.toSysOut());

        KTable<String,Long> counts = lowerCaseWords.groupBy((key, value) -> value ).count(Materialized.<String,Long, KeyValueStore<Bytes,byte[]>>as("count-store"));
        KStream<String, Long> outPutWordCountStream = counts.toStream();
        //outPutWordCountStream.print(Printed.toSysOut());
        outPutWordCountStream.to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        final Topology wordCountTopology = builder.build();
        System.out.println(wordCountTopology.describe());

        final KafkaStreams streams = new KafkaStreams(wordCountTopology, wordProps);
        KafkaUtils.runKafkaStream(streams);

        System.exit(0);
    }
}
