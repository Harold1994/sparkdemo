package com.harold.dao

import com.harold.domain.CourseClickCount
import com.harold.util.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer


object CourseClickCountDAO {
  val table_name = "imooc_course_clickcount"
  val cf = "info"
  val qualifer = "click_count"

  def save(list : ListBuffer[CourseClickCount]) : Unit = {
    val table = HBaseUtils.getInstance().getTable("imooc_course_clickcount")
    for(eve <- list) {
      table.incrementColumnValue(Bytes.toBytes(eve.day_course), Bytes.toBytes(cf),
        Bytes.toBytes(qualifer), eve.click_count)
    }

  }

  def get(day_course : String) : Long = {
    val table = HBaseUtils.getInstance().getTable("imooc_course_clickcount")
    val value = table.get(new Get(Bytes.toBytes(day_course))).getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifer))
    if (null == value)
      0L
    else
      Bytes.toLong(value)
  }

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20181010_6", 6))
    list.append(CourseClickCount("20181010_7", 7))
    list.append(CourseClickCount("20181010_8", 8))
    save(list)
    print(get("20181010_8"))

  }
}
