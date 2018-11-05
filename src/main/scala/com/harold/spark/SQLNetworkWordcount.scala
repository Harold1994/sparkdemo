package com.harold.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object SQLNetworkWordcount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("TransformApp")
    val ssc = new StreamingContext(conf, Seconds(2))

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    words.foreachRDD {(rdd:RDD[String], time:Time) =>
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._
      val wordsDataFrame = rdd.map(w => Record(w)).toDF()
      wordsDataFrame.createOrReplaceTempView("words")

      // Do word count on table using SQL and print it
      val wordCountsDataFrame =
        spark.sql("select word, count(*) as total from words group by word")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
    }

    ssc.start()
    ssc.awaitTermination()

    }
}

/** Case class for converting RDD to DataFrame */
case class Record(word: String)


object SparkSessionSingleton {
  @transient private var instance:SparkSession = _
  def getInstance(sparkConf: SparkConf):SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}