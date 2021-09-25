package com.github.nethra.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignandSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignandSeek.class.getName());
        Properties properties = new Properties();

        String topic = "first_topic";
        String bootstrap_server = "localhost:9092";
        // create consumer properties
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign and seek are used to mostly used to replay data or fetch a specific meesage

        //assign
        TopicPartition PartitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(PartitionToReadFrom));

        //seek
        consumer.seek(PartitionToReadFrom, offsetToReadFrom);
        int numOfmeesagesToRead = 5;
        boolean keepOnREading = true;
        int numberOfmeesagesReadSoFar = 0;


        //poll for new data
        while(keepOnREading){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records){
                numberOfmeesagesReadSoFar+=1;
                logger.info("Key: "+ record.key() +"Value: " + record.value());
                logger.info("partition: "+ record.partition() + "offset: "+ record.offset());
                if (numberOfmeesagesReadSoFar>=numOfmeesagesToRead){
                    keepOnREading=false;//exit the while loop
                    break;//exit the for loop
                }
            }

        }
    }
}
