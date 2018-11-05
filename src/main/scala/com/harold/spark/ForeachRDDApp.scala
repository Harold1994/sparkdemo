package com.harold.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ForeachRDDApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    wordCounts.foreachRDD {
      rdd => {
        rdd.foreachPartition { partitionOfrecords =>
          val connection = createNewConnection()
          partitionOfrecords.foreach { record =>
            val sql = "insert into wordcount(word,wordcount) values ('" + record._1 + "'," + record._2 + ")"
            connection.createStatement().execute(sql)
          }
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
//    createNewConnection
  }

  def createNewConnection() = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_db", "root", "lh1994114")

  }
}
