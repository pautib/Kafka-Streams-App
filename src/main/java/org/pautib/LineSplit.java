package org.pautib;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class LineSplit {

    public static void main(String[] args) {

        Properties lineProps = new Properties();
        lineProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit");
        lineProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // we assume that the Kafka broker this application is talking to runs on local machine with port 9092
        lineProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        lineProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("streams-plaintext-input");
        stream.print(Printed.toSysOut());

        KStream<String, String> words = stream.flatMapValues( value -> Arrays.asList(value.split("\\W+")) );
        words.print(Printed.toSysOut());
        words.to("streams-line-output");

        final Topology lineSplitTopology = builder.build();
        System.out.println(lineSplitTopology.describe());

        final KafkaStreams streams = new KafkaStreams(lineSplitTopology, lineProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }

        System.exit(0);
    }
}
