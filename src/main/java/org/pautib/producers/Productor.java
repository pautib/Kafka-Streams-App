package org.pautib.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Productor {

    public static void main(String[] args) {
        Properties producerProps = getProducerProps();

        String topic = "test_pautib_topic";
        int partition = 0;
        String key = "testKey2";
        String value = "testValue2";
        sendRecordAsync(topic, key, value, partition, producerProps);
    }

    private static void sendRecordAsync(String topic, String recordKey, String recordValue, Integer partition, Properties producerProps) {
        KafkaProducer<String,String> producer = new KafkaProducer<>(producerProps);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, recordKey, recordValue);
        producer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                System.out.println("Record sent failed");
            }
        });

        producer.close();
    }

    private static void sendRecord(String topic, String recordKey, String recordValue, Integer partition, Properties producerProps) {
        KafkaProducer<String,String> producer = new KafkaProducer<>(producerProps);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, recordKey, recordValue);

        try {
            producer.send(record).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        producer.close();
    }

    private static Properties getProducerProps() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        return producerProps;
    }

}
