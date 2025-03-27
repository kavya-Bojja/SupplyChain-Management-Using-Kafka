1. Navigate to the path of Zookeeper and Kafka. Since I insalled kafka in ubuntu, open Ubuntu and use the cd command to navigate to the directory where Kafka and Zookeeper are installed, likely in the home directory.

2. Command to Start Zookeeper
zookeeper-server-start.sh ~/kafka_2.13-3.1.0/config/zookeeper.properties
~/kafka_2.13-3.0.0/bin/zookeeper-server-start.sh ~/kafka_2.13-3.0.0/config/zookeeper.properties

3. Command to Start Kafka
kafka-server-start.sh ~/kafka_2.13-3.1.0/config/server.properties

4. Command to Run time log of Topic:
bin/kafka-console-consumer.sh bootstrap-server 'localhost:[::1]:9092 --topic Sample --from-beginning 

5. Following Kafka's setup. The project setup dependencies are listed below.
   - Java 8
   -  implementation 'org.apache.kafka:kafka-clients:3.1.0'

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
    implementation 'org.slf4j:slf4j-api:1.7.36'

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
    implementation 'org.slf4j:slf4j-simple:1.7.36'
    implementation 'org.apache.commons:commons-csv:1.9.0'

6. To run the code.
   A. First start the Zookeeper session.
   B. Second start the Kafka Session.
   C. Create the topics in java application.
   D. Initially run the Consumer class. As it is running on the behind now run the Producer class. 
   E. Now you can see the output


6. To execute the code.
   A. Begin the Zookeeper session first.
   B. Next, let's begin the Kafka session.
   C. Create the topics in java application.
   D. Run the Consumer class first. Now that the behind is operating, run the Producer class. 
   E. You can read the messages in consumer console.

7. To visualize the output from consumer, I have derived output in a csv file(Kafka-output) and in that I have visualized the result.