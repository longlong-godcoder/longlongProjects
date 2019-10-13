package conf

import java.io.InputStream
import java.util.Properties

/**
  * 配置管理组件
  * @author longlong
  */
object ConfigurationManager {

  private val properties: Properties = new Properties()

  try {
    val inputStream: InputStream = ConfigurationManager.getClass.getClassLoader.getResourceAsStream("my.properties")
    properties.load(inputStream)
  } catch {
    case e: Exception => e.printStackTrace()
  }

  def getProperty(key: String): String ={
    properties.getProperty(key)
  }

  def getInt(key: String): Int ={
    //这里中华石杉返回了Integer，return Integer.valueOf(value)
    // 为什么使用包装类Integer呢？
    val value: String = getProperty(key)
    try {
      return value.toInt
    } catch {
      case e: Exception => e.printStackTrace()
    }
    0
  }

  def getBoolean(key: String): Boolean ={
    val value: String = getProperty(key)
    try {
      return value.toBoolean
    } catch {
      case e: Exception => e.printStackTrace()
    }
    false
  }

  def getLong(key: String): Long ={
    val value: String = getProperty(key)
    try {
      value.toLong
    } catch {
      case e: Exception => e.printStackTrace()
    }
    0
  }
}
