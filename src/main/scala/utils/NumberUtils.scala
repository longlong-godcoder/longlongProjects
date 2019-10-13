package utils

object NumberUtils {
  /**
    * 格式化小数
    * @param num 小数
    * @param scale 四舍五入位数
    * @return 格式化后小数
    */
  def formatDouble(num: Double, scale: Int): Double ={
    val str: String = num.formatted("%." + scale + "f")
    str.toDouble
  }
}
