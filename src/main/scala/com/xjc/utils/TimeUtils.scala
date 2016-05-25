package com.xjc.utils

import java.text.SimpleDateFormat
import java.util.{ Calendar, Date }

/**
 * Created by xjc on 16-5-10.
 */
object TimeUtils {
  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def getToday(): String = {
    var now: Date = new Date()
    var hehe = dateFormat.format(now)
    hehe
  }

  def getYesterday(): String = {
    getNdaysAgo(1)
  }

  def getNdaysAgo(n: Int): String = {
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -n)
    dateFormat.format(cal.getTime)
  }

  def getNmonthsAgo(n: Int): String = {
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.MONTH, -n)
    dateFormat.format(cal.getTime).substring(0, 7)
  }

  def getNowWeekStart(): String = {
    var period: String = ""
    var cal: Calendar = Calendar.getInstance();
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    //获取本周一的日期
    period = dateFormat.format(cal.getTime())
    period
  }

  def getNowWeekEnd(): String = {
    var period: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY); //这种输出的是上个星期周日的日期，因为老外把周日当成第一天
    cal.add(Calendar.WEEK_OF_YEAR, 1) // 增加一个星期，才是我们中国人的本周日的日期
    period = df.format(cal.getTime())
    period
  }

  def getNowMonthStart(): String = {
    var period: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DATE, 1)
    period = df.format(cal.getTime()) //本月第一天
    period
  }

  def getNowMonthEnd(): String = {
    var period: String = ""
    var cal: Calendar = Calendar.getInstance();
    cal.set(Calendar.DATE, 1)
    cal.roll(Calendar.DATE, -1)
    period = dateFormat.format(cal.getTime()) //本月最后一天
    period
  }

  //将时间戳转化成日期
  def DateFormat(time: String): String = {
    var date: String = dateFormat.format(new Date((time.toLong * 1000l)))
    date
  }

  def getSeconds(time: String): Long = {
    //implicit def long2str(num:Long):String = String.valueOf(num)
    var cal: Calendar = Calendar.getInstance()
    cal.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time.trim))
    cal.getTimeInMillis / 1000
  }

}
