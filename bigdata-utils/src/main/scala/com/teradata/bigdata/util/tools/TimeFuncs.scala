package com.teradata.bigdata.util.tools

import java.text.{ParseException, SimpleDateFormat}
import java.util.Date

/**
  * @Project:
  * @Description: 时间函数工具类
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/8/27/027 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
trait TimeFuncs extends Serializable {

  //  var dateFormat = new SimpleDateFormat("yyyyMMdd")
  //  var monthFormat = new SimpleDateFormat("yyyyMM")
  //  var dataTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  //  val Y_M_D = "yyyy-MM-dd"
  //  val Y_M_D_HM = "yyyy-MM-dd HH:mm"
  //  val Y_M_D_HMS = "yyyy-MM-dd HH:mm:ss"
  //  val YMD = "yyyyMMdd"
  //  val YMDHM = "yyyyMMddHHmm"
  //  val YMDHMS = "yyyyMMddHHmmss"
  //  val ymd = "yyyy/MM/dd"
  //  val ymd_HM = "yyyy/MM/dd HH:mm"
  //  val ymd_HMS = "yyyy/MM/dd HH:mm:ss"
  def nowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    return date
  }

  def setDateFormat(format: String): SimpleDateFormat = {
    new SimpleDateFormat(format)
  }

  def getCurrentDate(format: String): String = {
    setDateFormat(format).format(toDate)
  }

  def getCurrentTimeMillis: Long = System.currentTimeMillis()

  def timeMillsToDate(timeMills: Long, format: String = "yyyy-MM-dd"): String = {
    val sdf: SimpleDateFormat = try {
      new SimpleDateFormat(format)
    } catch {
      case e: IllegalArgumentException => throw e.fillInStackTrace()
      case e: NullPointerException => throw e.fillInStackTrace()
    }
    val date: String = sdf.format(new Date(timeMills))
    date
  }

  def date2TimeStamp(dateStr: String, format: String): String = {
    try {
      val sdf = new SimpleDateFormat(format)
      return String.valueOf(sdf.parse(dateStr).getTime )
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    ""
  }

  def strToDate(str: String, format: String = "yyyy-MM-dd"): Date = {
    val sdFormat = new SimpleDateFormat(format)
    var date: Date = null
    try {
      date = sdFormat.parse(str)
    } catch {
      case e: ParseException => e.printStackTrace()
    }
    date
  }

  def toDate(timeMills: Long): Date = {
    val date = new Date(timeMills)
    date
  }

  def toDate: Date = {
    new Date(getCurrentTimeMillis)
  }

  //HDFS存储的数据文件load_time_m以5分钟一个
  def getCurrentLoadTime(): (String, String,String) = {
    val loadTimeTmp = timeMillsToDate(getCurrentTimeMillis, "yyyyMMddHHmm")
    val currentDate = loadTimeTmp.substring(0, 8)
    val currentHour = loadTimeTmp.substring(8, 10)
    val currentMinute = loadTimeTmp.substring(10, 12)
    if (currentMinute.tail.toInt < 5) {
      (currentDate, currentHour, currentMinute.head + "0")
    } else {
      (currentDate, currentHour, currentMinute.head + "5")
    }
  }

  def getTimeByHour(givenDate:String,hour: String): Date = {
    var newHour = "00"
    if (hour.matches("(?m)[0-9]{1}")) newHour = "0" + hour
    else if (hour.matches("(?m)[0-9]{2}")) newHour = hour
    strToDate(givenDate + newHour + "0000", "yyyyMMddHHmmss")
  }

  def getCurrentTimeByHour(hour: String): Date = {
    var newHour = "00"
    if (hour.matches("(?m)[0-9]{1}")) newHour = "0" + hour
    else if (hour.matches("(?m)[0-9]{2}")) newHour = hour
    strToDate(getCurrentDate("yyyyMMdd") + newHour + "0000", "yyyyMMddHHmmss")
  }

  def getIopSalesTimeFlag: Boolean = {
    val currentTimestamp = getCurrentTimeMillis
    val currTime = toDate(currentTimestamp)
    val currentDate = timeMillsToDate(currentTimestamp, "yyyy-MM-dd")
    val currentDate8oclock = strToDate(currentDate + " 08:00:00", format = "yyyy-MM-dd HH:mm:ss")
    val currentDate18oclock = strToDate(currentDate + " 18:00:00", format = "yyyy-MM-dd HH:mm:ss")
    if (currTime.after(currentDate8oclock) && currTime.before(currentDate18oclock)) true else false
  }

}
