package spark.product

import org.apache.spark.sql.api.java.UDF2

import scala.util.Random


/**
  * 注册为remove_random_prefix
  */
class RandomPrefixUDF extends UDF2[String, Int, String]{

  override def call(value: String, num: Int): String = {
    val random = new Random()
    val prefix: Int = random.nextInt(num)
    s"${prefix}_$value"
  }
}
