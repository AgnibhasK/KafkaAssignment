package com.knoldus.producer;

import com.knoldus.model.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class Producer {
    public static void main(String[] args){
        
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.knoldus.serializer.UserSerializer");
        KafkaProducer<String, User> producer = new KafkaProducer<>(properties);
        try {
            Random random = new Random();
            for (int i = 1; i <= 100; i++) {

                User user = new User(i, "Rex", random.nextInt(10)+20, "B.tech");
                producer.send(new ProducerRecord<String, User>("userobj", String.valueOf(i), user));
                System.out.println("Message " + user.toString() + " sent !!");
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.close();
        }
    }
}
