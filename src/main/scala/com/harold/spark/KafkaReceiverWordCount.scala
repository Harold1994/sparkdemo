package com.harold.spark

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferBrokers
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object KafkaDirectWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaReceiverWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaParams = Map[String, Object] (
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("streamingtopic")

    val stream =  KafkaUtils.createDirectStream [String, String](ssc,
      PreferBrokers,
      Subscribe[String, String](topics, kafkaParams))

    // stream.map(record => record.value()).flatMap(x=>x.split(" ")).map((_,1)).reduceByKey(_+_).print()

    stream.map(record => record.value()).count().print()
    ssc.start()
    ssc.awaitTermination()


  }
}
