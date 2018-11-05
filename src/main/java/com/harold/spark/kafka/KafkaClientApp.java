package com.harold.spark.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;

// producer 测试
public class KafkaClientApp {

    public static void main(String[] args) {
        new Consumer(KafkaProperties.TOPIC).start();
    }
}
