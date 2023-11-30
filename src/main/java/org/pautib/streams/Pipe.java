package org.pautib.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.pautib.KafkaUtils;

import java.util.Properties;

public class Pipe {
    public static void main(String[] args) {
        // we assume that the Kafka broker this application is talking to runs on local machine with port 9092
        Properties pipeProps = KafkaUtils.getKafkaDefaultProps("streams-pipe-example", "localhost:9092");

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("streams-plaintext-input");
        stream.print(Printed.toSysOut());
        stream.to("streams-pipe-output");

        final Topology plainTextStreamTopology = builder.build();
        System.out.println(plainTextStreamTopology.describe());

        final KafkaStreams streams = new KafkaStreams(plainTextStreamTopology, pipeProps);
        KafkaUtils.runKafkaStream(streams);

        System.exit(0);

    }
}
