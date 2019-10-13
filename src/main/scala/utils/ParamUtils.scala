package utils

import com.alibaba.fastjson.{JSONArray, JSONObject}
import conf.ConfigurationManager
import constant.Constants


object ParamUtils {

  /**
    *  从命令行参数中提取任务id
    * @param args args 命令行参数
    * @param taskType 任务id
    * @return
    */
  def getTaskIdFromArgs(args: Array[String], taskType: String): Long ={
    val local: Boolean = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)

    if (local){
      return ConfigurationManager.getLong(taskType)
    }else{
      try {
        if (args != null && args.length > 0) {
          return args(0).toLong
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    0
    //源代码返回的是null，因为使用的Long分装类，这里不支持null，看以后业务逻辑是否符合
  }

  /**
    * 从JSON对象中提取参数
    * @param jSONObject jsonObject JSON对象
    * @param field  参数
    * @return
    */
  def getParam(jSONObject: JSONObject, field: String): String ={
    //fastJson貌似不同版本API有区别，可以再进行研究确认
    val jsonArray: JSONArray = jSONObject.getJSONArray(field)
    if (jsonArray != null && jsonArray.size() > 0){
      return jsonArray.getString(0)
    }
    null
  }

}
