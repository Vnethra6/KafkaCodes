package com.github.nethra.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for(int i=0; i<10; i++){
            String topic = "first_topic";
            String value = "I love marvel movies - "+Integer.toString(i);
            String key = "ID_" + Integer.toString(i);

            //create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<>(topic,key,value);
        logger.info("key "+ key);
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
        }).get();

        }
        //flush data
        producer.flush();
        //flush and close
        producer.close();

        System.out.println("data produced complete");

    }
}


// ID_0 to partition 0
// ID_1 to partition 1
// ID_2 to partition 2
// ID_3 to partition 0
// ID_4 to partition 0
// ID_5 to partition 2
