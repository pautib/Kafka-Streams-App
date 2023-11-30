package org.pautib.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.pautib.KafkaUtils;

import java.util.Arrays;
import java.util.Properties;

public class LineSplit {

    public static void main(String[] args) {
        // We assume that the Kafka broker this application is talking to runs on local machine with port 9092
        Properties lineProps = KafkaUtils.getKafkaDefaultProps("streams-linesplit", "localhost:9092");

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("streams-plaintext-input");
        stream.print(Printed.toSysOut());

        KStream<String, String> words = stream.flatMapValues( value -> Arrays.asList(value.split("\\W+")) );
        words.print(Printed.toSysOut());
        words.to("streams-line-output");

        final Topology lineSplitTopology = builder.build();
        System.out.println(lineSplitTopology.describe());

        final KafkaStreams streams = new KafkaStreams(lineSplitTopology, lineProps);
        KafkaUtils.runKafkaStream(streams);

        System.exit(0);
    }
}
