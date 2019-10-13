package utils

/**
  * 校验工具类
  */
object ValidUtils {

  /**
    * 校验数据中的指定字段，是否在指定范围内
    * @param data 数据
    * @param dataField 数据字段
    * @param parameter 参数
    * @param startParamField  参数
    * @param endParamField 结束参数字段
    * @return boolean
    */
  def between(data: String, dataField: String, parameter: String, startParamField: String,endParamField: String): Boolean ={
    val startParamFieldStr: String = StringUtils.getFieldFromConcatString(parameter, "\\|", startParamField)
    val endParamFieldStr: String = StringUtils.getFieldFromConcatString(parameter, "\\|", endParamField)
    if (startParamFieldStr == null || endParamFieldStr == null)
      return true
    val dataFieldStr: String = StringUtils.getFieldFromConcatString(data, "\\|", dataField)
    if (dataFieldStr != null){
      if (dataFieldStr.toInt >= startParamFieldStr.toInt && dataFieldStr.toInt <= endParamFieldStr.toInt)
        return true
      else
        return false
    }
    false
  }

  /**
    * 校验数据中的指定字段，是否有值与参数字段的值相同
    * @param data 数据
    * @param dataField 数据字段
    * @param parameter 参数
    * @param paramField 参数字段
    * @return 校验结果
    */
  def in(data: String, dataField: String, parameter: String, paramField: String): Boolean ={
    val paramFieldStr: String = StringUtils.getFieldFromConcatString(parameter, "\\|", paramField)
    if (paramFieldStr == null)
      return true
    val paramFieldSplited: Array[String] = paramFieldStr.split(",")
    val dataFieldStr = StringUtils.getFieldFromConcatString(data, "\\|", dataField)
    if (dataFieldStr != null){
      val dataFieldSplited: Array[String] = dataFieldStr.split(",")
      for (singleDataField <- dataFieldSplited){
        for (singleParamField <- paramFieldSplited){
          if (singleDataField.equals(singleParamField))
            return true
        }
      }
    }
    false
  }

  def equal(data: String, dataField: String, parameter: String, paramField: String): Boolean ={
    val paramFieldValue: String = StringUtils.getFieldFromConcatString(parameter, "\\|", paramField)
    if (paramFieldValue == null)
      return true
    val dataFieldValue: String = StringUtils.getFieldFromConcatString(data, "\\|", dataField)
    if (dataFieldValue != null)
      if (dataFieldValue.equals(paramFieldValue))
        return true
    false
  }
}
