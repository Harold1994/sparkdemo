package com.harold.spark.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer extends Thread {
    private String topic;
    KafkaConsumer<String, String> consumer;
    public Consumer(String topic) {
        this.topic = topic;
        Properties prop = new Properties();
        prop.put("group.id", KafkaProperties.GROUP_ID);
        prop.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("enable.auto.commit", "true");
        prop.put("auto.commit.interval.ms", "1000");
        consumer = new KafkaConsumer<String, String>(prop);
        consumer.subscribe(Arrays.asList(KafkaProperties.TOPIC));
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord record : records) {
                System.out.println("received offset: " + record.offset() + " , received value" + record.value());
            }
        }
    }
}
