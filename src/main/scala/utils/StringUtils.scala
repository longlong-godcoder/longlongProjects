package utils
import scala.collection.mutable
/**
  * 字符串工具类
  * @author longlong
  */
object StringUtils {

  /**
    * 判断字符串是否为空
    * @param str 字符串
    * @return boolean
    */
  def isEmpty(str: String): Boolean ={
    str == null || "".equals(str)
  }

  /**
    * 判断字符串是否不为空
    * @param str 字符串
    * @return boolean
    */
  def isNotEmpty(str: String): Boolean ={
    str != null || !"".equals(str)
  }

  /**
    * 截断字符串两侧的逗号
    * @param str 字符串
    * @return 字符串
    */
  def trimComma(str: String): String ={
    var string = str
    if (string.startsWith(","))
      string = string.substring(1)
    if (string.endsWith(","))
      string = string.substring(0, string.length)
    string
  }

  /**
    * 补全两位数字
    * @param str 数字字符串
    * @return 补全后两位数字字符串
    */
  def fullfill(str: String): String ={
    if (str.length == 2) str else "0" + str
  }

  def getFieldFromConcatString(str: String, delimiter: String, field: String): String ={
    //这里为什么要try catch？
    try {
       val fields: Array[String] = str.split(delimiter)
      for (concateField <- fields) {
        if (concateField.split("=").length == 2) {
          val fieldName: String = concateField.split("=")(0)
          val fieldValue = concateField.split("=")(1)
          if (fieldName.equals(field)) return fieldValue
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    null
  }

  def setFieldInConcatString(str: String, delimiter: String, field: String, newFieldValue: String): String ={
    val fields: Array[String] = str.split(delimiter)

    for (i <- fields.indices){
      val fieldName = fields(i).split("=")(0)
      if (fieldName.equals(field)){
        val concatField: String = fieldName + "=" + newFieldValue
        fields(i) = concatField
      }
    }

    val stringBuilder: StringBuilder = new mutable.StringBuilder("")
    for (i <- fields.indices){
      stringBuilder.append(fields(i))
      if (i < fields.length - 1) stringBuilder.append("|")
    }

    stringBuilder.toString()
  }
}
