package io.conduktor.demos.kafka;

import org.apache.commons.csv.CSVParser;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.util.Properties;

public class ProducerDemoWithCsvfile {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCsvfile.class.getSimpleName());

    public static void main(String args[]) {
        log.info("I am a kafka producer");

        String csvFile = "D:\\Spring 2024\\Adv_db\\olist_orders_dataset.csv";
        String topicName = "second_demo";

        //Create Producer Properties
        Properties properties = new Properties();

        //Connect to Localhost
        properties.setProperty("bootstrap.servers", "localhost:[::1]:9092");

        //Set Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        //
        // properties.setProperty("batch.size", "400");

        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //Create the producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
             CSVParser parser = CSVFormat.DEFAULT
                     .withFirstRecordAsHeader()
                     .withIgnoreHeaderCase()
                     .withTrim()
                     .parse(new FileReader(csvFile))) {
            //Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);
            for (CSVRecord record : parser) {
                // Assuming you have columns named 'id' and 'data'
                String key = record.get("order_status");
                String value = record.get("order_estimated_delivery_date");
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);

                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        //executes everytime a record is sent successfully or exception is thrown
                        if (e == null) {
                            //record was successfully sent
                            log.info("Order Status:" + key + " | Partition: " + metadata.partition() + "| Estimated Delivery:" + value
                                    //"Topic: " + metadata.topic() + "\n"
                                    //"Offset: " + metadata.offset() + "\n" +
                                    // "Timestamp: " + metadata.timestamp() + "\n"
                            );
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
                //Tell the producer to send all data and block until done ---synchronous
                //producer.flush();

                //Flush and Close the producer
                //producer.close();
            }
        } catch (FileNotFoundException e) {
            System.err.println("The specified CSV file was not found: " + e.getMessage());
            // Consider whether to rethrow, halt, or allow the application to continue
        } catch (IOException e) {
            System.err.println("An error occurred when processing the CSV file: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("An unexpected error occurred: " + e.getMessage());
            // This could catch other exceptions like Kafka's SerializationException, InterruptException, etc.
        }

        //Create a Producer Record
        //ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_demo", "Hello World" + i);
        //ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);




    }
}