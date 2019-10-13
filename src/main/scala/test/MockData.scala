package test

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import utils.{DateUtils, StringUtils}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * 模拟离线数据类
  * 说明;
  * 一天的数据，最多10个页面
  * 100个可能重复的userid
  * 每个userid对应10个sessionid，点击10个可能重复的品类
  * 每个sessionid对应0到100次不确定的action操作
  * 最后，click,order,pay没有包含关系，但显而易见，拥有clickproductid必定用clickcategoryId
  * @author longlong
  */
object MockData {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("testMockData").getOrCreate()
    mock(spark)
  }

  /**
    * 模拟数据
    * @param sc SparkSession
    */
  def mock(sc: SparkSession): Unit ={

    val rows: ListBuffer[Row] = ListBuffer[Row]()
    val date: String = DateUtils.getTodayDate
    val searchKeywords: Array[String] = Array[String]("火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
      "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉")
    val actions: Array[String] = Array[String]("search", "click", "order", "pay")
    val random = new Random()

    for (i <- 0 until 100) {

      val userid: Long = random.nextInt(100).toLong

      for (i <- 0 until 10) {

        val sessionid: String = UUID.randomUUID().toString.replace("-", "")
        val baseActionTime: String = s"$date ${random.nextInt(24)}"
        var clickCategoryIdOption: Option[Long] = None

        for (k <- 0 to random.nextInt(100)) {

          val pageid: Long = random.nextInt(10).toLong
          val actionTime: String = s"$baseActionTime:${StringUtils.fullfill(random.nextInt(60).toString)}:${StringUtils.fullfill(random.nextInt(60).toString)}"
          var searchKeyword: String = null
          var clickProductIdOption: Option[Long] = None
          var orderCategoryIds: String = null
          var orderProductIds: String = null
          var payCategoryIds: String = null
          var payProductIds: String = null
          var cityId: Long = random.nextInt(10).toLong

          val action: String = actions(random.nextInt(4))
          if ("search".equals(action)) searchKeyword = searchKeywords(random.nextInt(10))
          else if ("click".equals(action)) {
            if (clickCategoryIdOption.isEmpty) clickCategoryIdOption = Some(random.nextInt(100).toLong)
            clickProductIdOption = Some(random.nextInt(100).toLong)
          } else if ("order".equals(action)) {
            orderCategoryIds = random.nextInt(100).toString
            orderProductIds = random.nextInt(100).toString
          } else if ("pay".equals(action)) {
            payCategoryIds = random.nextInt(100).toString
            payProductIds = random.nextInt(100).toString
          }

          val row = Row(
            date, userid, sessionid, pageid, actionTime, searchKeyword,
            if (clickCategoryIdOption.isEmpty) null else clickCategoryIdOption.get,
            if (clickProductIdOption.isEmpty) null else clickProductIdOption.get,
            orderCategoryIds, orderProductIds,
            payCategoryIds, payProductIds,
            cityId
          )
          rows += row
        }
      }
    }
    var rowsRDD: RDD[Row] = sc.sparkContext.parallelize(rows)

    val schema = StructType(Array[StructField](
      StructField("date", StringType, nullable = true),
      StructField("user_id", LongType, nullable = true),
      StructField("session_id", StringType, nullable = true),
      StructField("page_id", LongType, nullable = true),
      StructField("action_time", StringType, nullable = true),
      StructField("search_keyword", StringType, nullable = true),
      StructField("click_category_id", LongType, nullable = true),
      StructField("click_product_id", LongType, nullable = true),
      StructField("order_category_ids", StringType, nullable = true),
      StructField("order_product_ids", StringType, nullable = true),
      StructField("pay_category_ids", StringType, nullable = true),
      StructField("pay_product_ids", StringType, nullable = true),
      StructField("city_id", LongType, nullable = true)
    ))
    val df: DataFrame = sc.createDataFrame(rowsRDD, schema)
    df.createOrReplaceGlobalTempView("user_visit_action")
    df.take(10).foreach(_row => {
      println("=========================")
      println(_row)
    })
    //========================================================================//
    rows.clear()
    val sexes: Array[String] = Array[String]("male", "female")

    for (i <- 0 until 100){
      val userid: Long= i
      val username: String = s"user$i"
      val name: String = s"name$i"
      val age: Long = random.nextInt(60)
      val professional: String = s"professional${random.nextInt(100)}"
      val city: String = s"city${random.nextInt(100)}"
      val sex: String = sexes(random.nextInt(2))
      val row = Row(
        userid, username, name, age, professional, city, sex
      )
      rows += row
    }

    rowsRDD = sc.sparkContext.parallelize(rows)

    val schema2 = StructType(Array[StructField](
      StructField("user_id", LongType, nullable = true),
      StructField("username", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("age", LongType, nullable = true),
      StructField("professional", StringType, nullable = true),
      StructField("city", StringType, nullable = true),
      StructField("sex", StringType, nullable = true)
    ))

    val df2: DataFrame = sc.createDataFrame(rowsRDD, schema2)
    df2.take(10).foreach(_row => {
      println("=========================")
      println(_row)
    })
    df2.createOrReplaceGlobalTempView("user_info")
    val testdf: DataFrame = sc.sql("select * from view user_info")
    testdf.show()

    //===================================================================//
    rows.clear()
    val productStatus: Array[Int] = Array[Int](0, 1)
    for (i <- 0 until 100){
      val productId: Long = i
      val productName: String = s"product$i"
      val extendInfo: String = "{\"product_status\":" + productStatus(random.nextInt(2)) + "}"

      val row = Row(productId, productName, extendInfo)
      rows += row
    }
    rowsRDD = sc.sparkContext.parallelize(rows)

    val schema3 = StructType(Array[StructField](
      StructField("product_id", LongType, nullable = true),
      StructField("product_name", StringType, nullable = true),
      StructField("extend_info", StringType, nullable = true)
    ))

    val df3: DataFrame = sc.createDataFrame(rowsRDD, schema3)
    df3.take(10).foreach(_row => {
      println("=========================")
      println(_row)
    })
    df3.createOrReplaceGlobalTempView("product_info")

  }
}
