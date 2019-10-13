package spark.product

import org.apache.spark.sql.api.java.UDF3

/**
  * 注册为concat_long_string
  */
class ConcatLongStringUDF extends UDF3[Long, String, String, String]{

  override def call(t1: Long, t2: String, t3: String): String = {
    t1 + t2 + t3
  }
}
