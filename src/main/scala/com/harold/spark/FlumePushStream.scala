package com.harold.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FlumePushStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("FlumePushStream")
    val ssc = new StreamingContext(conf, Seconds(5))

    FlumeUtils.createStream(ssc, "localhost", 44444)
      .map(x => new String(x.event.getBody.array()).trim)
      .flatMap(x => x.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()
    ssc.start()
    ssc.awaitTermination()
  }
}
