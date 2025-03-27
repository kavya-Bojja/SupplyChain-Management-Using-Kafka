package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithCsvfile {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithCsvfile.class.getSimpleName());

    public static void main(String args[]) {

        log.info("I am a kafka consumer");

        String topic = "second_demo";
        String groupId = "my_project";

        // Set up consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:[::1]:9092");
        //Create Consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest"); //none/earliest/latest

        //Create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to the topic
        //consumer.subscribe(Arrays.asList(topic));

        //get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                //join the main thread to allow the execution of the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        try(FileWriter writer = new FileWriter("D:\\Spring 2024\\Adv_db\\kafka_output.csv", true)) {
            //Subscribe to topic
            consumer.subscribe(Arrays.asList(topic));

            //poll for data
            while (true) {
                log.info("Polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    log.info("No records found");
                }

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        log.info("Order status:" + record.key() + "  " + "Estimated Delivery date:" + record.value());
                        //log.info("Partition:" + record.partition() + "Offset:" + record.offset());
                        writer.append(record.key())
                                .append(',')
                                .append(record.value())
                                .append(',')
                                //.append(String.valueOf(record.partition()))
                                //.append(',')
                                //.append(String.valueOf(record.offset()))
                                .append('\n');
                        writer.flush();
                    }
                    catch (IOException e) {
                        log.error("Failed to write to CSV", e);
                    }
                }

            }


        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            log.info("Unexpected Error");
        } finally {
            consumer.close();  //close the consumer, this will also commit the offset
            log.info("Consumer shutdown successfully");
        }

    }
}
