package com.harold.project

import com.harold.dao.{CourseClickCountDAO, CourseSearchClickDAO}
import com.harold.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.harold.util.TimeUtil
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferBrokers
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.collection.mutable.ListBuffer

object ImookStatStreamingApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ImookStatStreamingApp")
    val ssc = new StreamingContext(conf, Seconds(60))

    val kafkaParams = Map[String, Object] (
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
      )
    val topics =Array("streamingtopic")

    val stream = KafkaUtils.createDirectStream[String, String](ssc,
      PreferBrokers,
      Subscribe[String, String](topics, kafkaParams)
    )

    val logs = stream.map(record => record.value())

    val cleanData = logs.map(line => {
      val infos = line.split("\t")
      var course_id = 0
      val url = infos(1)

      if (url.startsWith("class")) {
        val course_html =  url.split("/")(1)
        course_id = course_html.substring(0, course_html.lastIndexOf(".")).toInt
      }
      ClickLog(infos(2),course_id,TimeUtil.parseToMunite(infos(0)), infos(3),infos(4).toInt)
    }
    ).filter(clicklog => clicklog.course_id != 0)

    cleanData.map(x =>
      (x.date.substring(0,8) + "_" + x.course_id,1)
    ).reduceByKey(_+_).foreachRDD(rdd => {
      rdd.foreachPartition(partitionrecords => {
        val list = new ListBuffer[CourseClickCount]
        partitionrecords.foreach(pair => {
          list.append(CourseClickCount(pair._1, pair._2))
        })
        CourseClickCountDAO.save(list)
      })
    })

    cleanData.map(x => {
      val refer = x.refer.replaceAll("//","/")
      val split = refer.split("/")
      var host = ""
      if (split.length > 2) {
        host = split(1)
      }
      (host, x.course_id, x.date)
    }).filter(_._1 != "").map(x => {
        (x._3.substring(0,8) + "_" + x._1 + "_" + x._2 , 1)}).reduceByKey(_+_)
        .foreachRDD(rdd => {
          rdd.foreachPartition(partitionRecord => {
            val list = new ListBuffer[CourseSearchClickCount]
            partitionRecord.foreach(pair => {
              list.append(CourseSearchClickCount(pair._1, pair._2))
            })
            CourseSearchClickDAO.save(list)
          })
        })
    ssc.start()
    ssc.awaitTermination()

  }
}
