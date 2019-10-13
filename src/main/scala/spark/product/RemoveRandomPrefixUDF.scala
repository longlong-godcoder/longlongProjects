package spark.product

import org.apache.spark.sql.api.java.UDF1

/**
  * 注册为remove_random_prefix
  */
class RemoveRandomPrefixUDF extends UDF1[String, String]{

  override def call(value: String): String = {
    val valSplited: Array[String] = value.split("_")
    valSplited(1)
  }
}
