package com.harold.spark.kafka;

import org.apache.kafka.common.protocol.types.Field;

/**
 * Kafka配置文件
 */
public class KafkaProperties {
    public static final String ZK = "localhost:2181";
    public static final String TOPIC = "test";
    public static final String BROKER_LIST = "localhost:9092";
    public static final String GROUP_ID = "grp";
}
