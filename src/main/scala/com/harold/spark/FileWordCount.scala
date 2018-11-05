package com.harold.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("FileWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.textFileStream("file:///Users/harold/Documents/ss/")
    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
