package com.github.nethra.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for(int i=0; i<10; i++){

            //create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic","I love marvel movies - "+Integer.toString(i));

        //send data - asynchronous
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //executes everytime a record is successfully produced
                if (e==null){
                    //the record was successfully sent
                    logger.info("Record produced successfully \n"+
                            "topic: "+ recordMetadata.topic() + "\n"+
                            "partition: " + recordMetadata.partition() +"\n"+
                            "offset: "+ recordMetadata.offset()+ "\n" +
                            "timestamp: "+ recordMetadata.timestamp());
                }
                else{
                    logger.error("Error while producing", e);
                }
            }
        });

        }
        //flush data
        producer.flush();
        //flush and close
        producer.close();

        System.out.println("data produced complete");

    }
}
