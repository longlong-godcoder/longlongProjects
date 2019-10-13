package spark.product

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.api.java.UDF2

class GetFieldFromJSONStrUDF extends UDF2[String, String, String]{

  override def call(json: String, field: String): String = {
    try {
      val jSONObject: JSONObject = JSON.parseObject(json)
      jSONObject.getString(field)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    null
  }
}
