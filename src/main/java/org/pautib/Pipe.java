package org.pautib;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Pipe {
    public static void main(String[] args) {

        Properties pipeProps = new Properties();
        pipeProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe-example");
        pipeProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // we assume that the Kafka broker this application is talking to runs on local machine with port 9092
        pipeProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        pipeProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("streams-plaintext-input");
        stream.print(Printed.toSysOut());
        stream.to("streams-pipe-output");

        final Topology plainTextStreamTopology = builder.build();
        System.out.println(plainTextStreamTopology.describe());

        final KafkaStreams streams = new KafkaStreams(plainTextStreamTopology, pipeProps);
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
