package com.harold.spark.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class kafkaProducer extends Thread {
    private String topic;
    private KafkaProducer<Integer, String> producer;

    public kafkaProducer(String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "1");
        producer = new KafkaProducer<Integer, String> (props);
    }

    @Override
    public void run() {
        int messageNum = 1;
        while (true) {
            String message = "message" + messageNum;
            producer.send(new ProducerRecord<Integer, String>(KafkaProperties.TOPIC, message));
            messageNum ++ ;
            System.out.println("sent message: " + message);
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
