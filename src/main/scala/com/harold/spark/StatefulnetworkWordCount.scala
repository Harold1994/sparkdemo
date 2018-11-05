package com.harold.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object StatefulnetworkWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("StatefulnetworkWordCount")

    val ssc = new StreamingContext(conf, Seconds(2))

    ssc.checkpoint(".")
    val initialRDD = ssc.sparkContext.parallelize(List(("hello",1), ("workd",1)))

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordDStream = words.map(x => (x,1))

    val mapFunc = (word:String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption().getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    val statedStream = wordDStream.mapWithState(
      StateSpec.function(mapFunc).initialState(initialRDD).timeout(Seconds(30))
    )
    statedStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
