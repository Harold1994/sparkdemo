package com.harold.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FlumePullStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("FlumePullStream")
    val ssc = new StreamingContext(conf, Seconds(5))

    FlumeUtils.createPollingStream(ssc, "localhost", 41414)
      .map(x => new String(x.event.getBody.array()).trim)
      .flatMap(x => x.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()
    ssc.start()
    ssc.awaitTermination()
  }
}
