package com.niazi.kafka.course;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        log.info("I am a Kafka producer");

        // Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // send the data
        for (int i = 0; i < 10; i++) {
            // create a producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("demo_java", "hello world " + i);

            producer.send(record, ((metadata, exception) -> {
                // everytime a message successfully sends, or an exception is thrown
                if (exception == null) {
                    log.info("Received new metadata\n\n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp());
                } else {
                    log.error("Error while producing", exception);
                }
            })); // asynchronous call

            try {
                Thread.sleep(1_000);
            } catch (Exception e) {  }
        }

        // flush and close producer
        producer.flush(); // synchronous call

        // close the producer
    }
}
