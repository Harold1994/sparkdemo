package com.harold.dao

import com.harold.domain.CourseSearchClickCount
import com.harold.util.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object CourseSearchClickDAO {
  val table_name = "imooc_course_search_clickcount"
  val cf = "info"
  val qualifer = "click_count"

  def save(list : ListBuffer[CourseSearchClickCount]) : Unit = {
    val table = HBaseUtils.getInstance().getTable(table_name)
    for(eve <- list) {
      table.incrementColumnValue(Bytes.toBytes(eve.DaySearchCourse), Bytes.toBytes(cf),
        Bytes.toBytes(qualifer), eve.clickCount)
    }
  }

  def get(day_course_search : String) : Long = {
    val table = HBaseUtils.getInstance().getTable("imooc_course_search_clickcount")
    val value = table.get(new Get(Bytes.toBytes(day_course_search))).getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifer))
    if (null == value)
      0L
    else
      Bytes.toLong(value)
  }
}
