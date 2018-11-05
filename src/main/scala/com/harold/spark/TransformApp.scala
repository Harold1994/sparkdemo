package com.harold.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("TransformApp")
    val ssc = new StreamingContext(conf, Seconds(2))

    val black = List("zs","ls")
    val blackRDD = ssc.sparkContext.parallelize(black).map(x=>(x,true))

    val lines = ssc.socketTextStream("localhost", 9999)
    val result = lines.map(x => (x.split(",")(1),x))
    val ans = result.transform { rdd =>
        rdd.leftOuterJoin(blackRDD).filter(x => x._2._2 != true)
        .map(x=>x._2._1)
    }
    ans.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
