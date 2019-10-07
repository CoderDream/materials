package com.qf.gameanalyze

import org.apache.commons.lang3.time.FastDateFormat


object FilterUtils {
  private val fdf = FastDateFormat.getInstance("yyyy年MM月dd日,E,HH:mm:ss")

  // 以时间作为过滤条件
  def filterByTime (fields: Array[String], startTime: Long, endTime: Long): Boolean = {
    val time = fields(1)
    val time_long = fdf.parse(time).getTime
    time_long >= startTime && time_long < endTime
  }

  // 以事件作为过滤条件
  def filterByType (fields: Array[String], eventType: String): Boolean = {
    val _type = fields(0)
    _type.equals(eventType)
  }

  // 以时间和类型作为过滤条件
  def filterByTimeAndType(fields: Array[String], eventType: String,
                          startTime: Long, endTime: Long): Boolean = {
    val time = fields(1)
    val time_long = fdf.parse(time).getTime
    val _type = fields(0)
    time_long >= startTime && time_long < endTime && _type.equals(eventType)
  }

  // 以多个事件类型作为过滤条件
  def filterByTypes (fields: Array[String], eventTypes: String*): Boolean = {
    val _type = fields(0)
    for (et <- eventTypes) {
      if (_type.equals(et))
        return true
    }
    false
  }

}
