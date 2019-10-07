package com.qf.gameanalyze

import java.util.Calendar

import org.apache.commons.lang3.time.FastDateFormat

object TimeUtils {
  private val fdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  private val calendar = Calendar.getInstance()

  // 将String类型的日期转换为Long类型
  def apply (time: String): Long = {
    calendar.setTime(fdf.parse(time))
    calendar.getTimeInMillis
  }

  // 改变日期的方法
  def updateCalendar(a: Int): Long = {
    calendar.add(Calendar.DATE, a)
    val time = calendar.getTimeInMillis
    calendar.add(Calendar.DATE, -a)
    time
  }

}
