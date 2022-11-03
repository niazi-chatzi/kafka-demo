package com.niazi.kafka.course;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class);

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");

        // Property variables
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-third-application";
        String topic = "demo_java";

        // Create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Get a reference to the current thread
        final Thread mainThread = Thread.currentThread();;

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup(); // This will throw an exception to finis hthe while loop

                // Join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    log.error("Interrupt error", e);
                }
            }
        });

        try {

            // Subscribe consumer to our topics
            consumer.subscribe(Arrays.asList(topic));

            while (true) {
                // Log
                log.info("Polling...");

                // Poll the topic / Listen to the topic. If the topic has no data wait for "100" milliseconds
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                // Iterate over the records. "records" will be null if no data found
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException _ignore) {
            log.info("Wakeup exception!");
            // We intentionally ingore this because we made the code to throw the exception to stop the loop
        } catch (Exception e) {
            log.error("Unxceptected exception", e);
        } finally {
            consumer.close(); // This will also commit offsets if need be
            log.info("Consumer is gracefully closed");
        }
    }
}