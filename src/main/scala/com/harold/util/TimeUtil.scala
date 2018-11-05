package com.harold.util

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat


object TimeUtil {
  val YYYYMMDDHHMMSS = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:SS")
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmDD")

  def getTime(time:String) = {
    YYYYMMDDHHMMSS.parse(time).getTime
  }
  def parseToMunite(time:String) =  {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

}
