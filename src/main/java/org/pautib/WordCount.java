package org.pautib;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCount {

    public static void main(String[] args) {

        Properties wordProps = new Properties();
        wordProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        wordProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // we assume that the Kafka broker this application is talking to runs on local machine with port 9092
        wordProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        wordProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

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
