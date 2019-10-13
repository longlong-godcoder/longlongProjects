package utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * 日期时间工具类
  * @author longlong
  */
object DateUtils {
  private val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
  private val DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd")

  /**
    * 判断一个时间是否在另一个时间之前
    * @param time1 时间1
    * @param time2 时间2
    * @return boolean
    */
  def before(time1: String, time2: String): Boolean ={
    try {
      val dateTime1: Date = TIME_FORMAT.parse(time1)
      val dateTime2: Date = TIME_FORMAT.parse(time2)
      if (dateTime1.before(dateTime2)) {
        return true
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    false
  }

  /**
    * 判断一个时间是否在另一个时间之后
    * @param time1 时间1
    * @param time2 时间2
    * @return boolean
    */
  def after(time1: String, time2: String): Boolean ={
    try {
      val dateTime1: Date = TIME_FORMAT.parse(time1)
      val dateTime2: Date = TIME_FORMAT.parse(time2)
      if (dateTime1.after(dateTime2)) {
        return true
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    false
  }

  /**
    * 计算时间差值（单位为秒）
    * @param time1 时间1
    * @param time2 时间2
    * @return 差值
    */
  def minus(time1: String, time2: String): Int ={
    try {
      val dateTime1: Date = TIME_FORMAT.parse(time1)
      val dateTime2: Date = TIME_FORMAT.parse(time2)
      val millisecond: Long = dateTime1.getTime - dateTime2.getTime
      return (millisecond / 1000).toInt
    } catch {
      case e: Exception => e.printStackTrace()
    }
    0
  }

  /**
    * 获取年月日和小时
    * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
    * @return 结果（yyyy-MM-dd_HH）
    */
  def getDateHour(datetime: String): String ={
    val date: String = datetime.split(" ")(0)
    val hourMinuteSecond: String = datetime.split(" ")(1)
    val hour: String = hourMinuteSecond.split(":")(0)
    date + "_" + hour
  }

  /**
    * 获取当天日期（yyyy-MM-dd）
    * @return 当天日期
    */
  def getTodayDate: String ={
    DATE_FORMAT.format(new Date())
  }

  /**
    *  获取昨天的日期（yyyy-MM-dd）
    * @return 昨天的日期
    */
  def getYesterdayDate: String ={
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.DAY_OF_YEAR, -1)
    val date: Date = cal.getTime
    DATE_FORMAT.format(date)
  }

  /**
    * 格式化时间（yyyy-MM-dd HH:mm:ss）
    * @param date Date对象
    * @return 日期String
    */
  def formatDate(date: Date): String ={
    DATE_FORMAT.format(date)
  }

  def formatTime(date: Date): String ={
    TIME_FORMAT.format(date)
  }
  /**
    * 解析时间字符串
    * @param time 时间字符串
    * @return Date对象
    */
  def parseTime(time: String): Date ={
    try {
      return TIME_FORMAT.parse(time)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    null
  }

  /**
    * 格式化日期key
    * @param date date
    * @return 生成的日期key
    */
  def formatDateKey(date: Date): String ={
    DATEKEY_FORMAT.format(date)
  }

  /**
    * 格式化日期key
    * @param datekey 日期key
    * @return Date对象
    */
  def parseDateKey(datekey: String): Date ={
    try {
      return DATEKEY_FORMAT.parse(datekey)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    null
  }

  /**
    * 格式化时间，保留到分钟级别yyyyMMddHHmm
    * @param date Date对象
    * @return String
    */
  def formatTimeMinute(date: Date): String ={
    val sdf = new SimpleDateFormat("yyyyMMddHHmm")
    sdf.format(date)
  }
}
