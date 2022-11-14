package com.mycompany.app;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class: 44-517 Big Data
 * Author: Niharica Myla
 * Description: The project is about finding out the smokers and checking their temperature to find if they need attention or not as well as planning their particular meal temperature.
 * Due: 11/11/2022
 * I pledge that I have completed the programming assignment independently.
   I have not copied the code from a student or any source.
   I have not given my code to any other student.
   I have not given my code to any other student and will not share this code
   with anyone under any circumstances.
*/

public class Consumer {

    public static void main(String[] args)
    {
        String bootstrapServers = "localhost:9092";

        Properties properties= new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "console-consumer-myapp");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("quickstart"));

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: records)
            {
                String a11 = record.value().substring(record.value().lastIndexOf(bootstrapServers));
                String b11 = a11.substring(10);
                double c11 = Double.parseDouble(b11);
                System.out.println(c11);
                System.out.println(record.topic() + ", " + record.key() + ", " + record.value());
                ArrayList<Double> now = new ArrayList<Double>();
                for (int i= 0; i< now.size(); i++) {
                    now.add(c11);
                    System.out.println(now.get(i));

                    if(record.key().equals("ch001")) {
                        for(i= 0; i<now.size();i++){
                            System.out.println(record.key()+now.get(now.size()-1));
                            if(now.size()>5){
                                if((now.get(i+5)-now.get(i))<10){
                                    System.out.println("alert!");
                                }
                            }
                        }
                    }

                }
            }
 
    }
    
}}
    /*-
%sql
select Channel2 from smoker_temps where Channel1 < 65 and Time(UTC) < 2;


 */