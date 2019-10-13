package spark.product

import dao.IAreaTop3ProductDAO
import domain.AreaTop3Product
import factory.DAOFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

object AreaTop3ProductSpark {

  /**
    * 通过时间范围获取RDD[(cityid, Row)]
    * @param sc SparkConSesion
    * @param startDate Stirng: startDate
    * @param endDate String: endDate
    * @return
    */
  def getCityid2ClickActionRDDByDate(sc: SparkSession, startDate: String, endDate: String): RDD[(Long, Row)] ={

    val sql: String =
      s"""
        |SELECT city_id, click_product_id FROM user_visit_action
        |WHERE click_product_id IS NOT NULL AND date>='$startDate' AND date>='$endDate'
      """.stripMargin

    val clickActionDF: DataFrame = sc.sql(sql)

    val clickActionRDD: RDD[Row] = clickActionDF.rdd

    val cityid2clickActionRDD: RDD[(Long, Row)] = clickActionRDD.map(row => {
      val cityid: Long = row.get(0).toString.toLong
      (cityid, row)
    })

    cityid2clickActionRDD
  }


  /**
    * 选定日期的ClickProduct，cityidInfo 明细表, 注册在SparkSession
    * @param sc parkConSesion
    * @param cityid2AcitonRDD RDD[(cityid, Row)]
    * @param cityid2cityInfoRDD RDD[(cityid, cityInfoRow)]
    */
  def generateTempClickProductBasicTable(sc: SparkSession, cityid2AcitonRDD: RDD[(Long, Row)], cityid2cityInfoRDD: RDD[(Long, Row)]): Unit ={

    val joinedRDD: RDD[(Long, (Row, Row))] = cityid2AcitonRDD.join(cityid2cityInfoRDD)

    val mappedRDD: RDD[Row] = joinedRDD.map(tuple => {
      val cityid: Long = tuple._1
      val clickAction: Row = tuple._2._1
      val cityInfo: Row = tuple._2._2

      val productid: Long = clickAction.getLong(1)
      val cityName: String = cityInfo.getString(1)
      val area: String = cityInfo.getString(2)

      Row(cityid, cityName, area, productid)
    })

    val schema = StructType(Array[StructField](
      StructField("city_id", LongType, nullable = true),
      StructField("city_name", StringType, nullable = true),
      StructField("area", StringType, nullable = true),
      StructField("product_id", LongType, nullable = true)
    ))

    val df: DataFrame = sc.createDataFrame(mappedRDD, schema)

    df.createOrReplaceGlobalTempView("tmp_click_product_basic")

  }

  /**
    * 各区域商品点击次数统计临时表
    * @param sc SparkContext
    */
  def generateTempAreaProductClickCountTable(sc: SparkSession): Unit ={

    val sqlv1: String =
      """
        |SELECT area, product_id, count(*) click_count, group_concat_distinct(concat_long_string(city_id, city_name, ':')) city_infos
        |FROM tmp_click_product_basic
        |GROUP BY area, product_id
      """.stripMargin

    //为避免数据倾斜采用的二次聚合方案
    val sqlv2: String =
      """
        |SELECT product_id_area, count(click_count) click_count, group_concat_distinct(city_infos) city_infos
        |FROM (
        |  SELECT remove_random_prefix(product_id_area) product_id_area, click_count, city_infos
        |  FROM (
        |    SELECT product_id_area, count(*) click_count, group_concat_distinct(city_id, city_name, ':')) city_infos
        |    FROM (
        |      SELECT random_prefix(concat_long_string(product_id,area,':'), 10) product_id_area, city_id, city_name
        |      FROM tmp_click_product_basic
        |    ) t1
        |    GROUP BY product_id_area
        |  ) t2
        |) t3
        |GROUP BY product_id_area
      """.stripMargin

    val df: DataFrame = sc.sql(sqlv2)

    df.createOrReplaceGlobalTempView("tmp_area_product_click_count")
  }

  /**
    *
    * @param sc
    */
  def generateTempAreaFullProductClickCountTable(sc: SparkSession): Unit ={

    val sqlv1: String =
      """
        |SELECT
        |t1.area, t1.click_count, t1.city_infos, t2.product_name,
        |if(get_json_object(t2.extend_info, 'product_status'=0, '自营商品', '第三方商品')) product_status
        |FROM tmp_area_product_click_count t1
        |JOIN product_info t2 ON t1.product_id = t2.product_id
      """.stripMargin

    val df: DataFrame = sc.sql(sqlv1)

    df.createOrReplaceGlobalTempView("tmp_area_fullprod_click_count")
  }

  def getAreaTop3ProductRDD(sc: SparkSession): RDD[Row] ={

    val sql: String =
        """
          |SELECT area,
          |CASE
          |  WHEN area = 'China North' OR area = 'China East' THEN 'A Level'
          |  WHEN area = 'China South' OR area = 'China Middle' THEN 'B Level'
          |  WHEN area = 'West North' OR area = 'West South' THEN 'C Level'
          |  ElSE 'D Level' END area_level,
          |product_id, click_count, city_infos, product_name, product_status
          |FROM (
          |  SELECT area, product_id, click_count, city_infos, product_name, product_status,
          |  row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank
          |  FROM tmp_area_fullprod_click_count
          |) t
          |WHERE rank <= 3
        """.stripMargin
    val df: DataFrame = sc.sql(sql)

    df.rdd
  }

  def persistAreaTop3ProductRDD(taskid: Long, rows: List[Row]): Unit ={

    val areaTop3products: ListBuffer[AreaTop3Product] = ListBuffer[AreaTop3Product]()
    rows.foreach(row => {
      val areaTop3Product = AreaTop3Product()
      areaTop3Product.taskid = taskid
      areaTop3Product.area = row.getString(0)
      areaTop3Product.areaLevel = row.getString(1)
      areaTop3Product.productid = row.getLong(2)
      areaTop3Product.clickCount = row.get(3).toString.toLong
      areaTop3Product.cityInfos = row.getString(4)
      areaTop3Product.productName = row.getString(5)
      areaTop3Product.productStatus = row.getString(6)
      areaTop3products += areaTop3Product
    })

    val areaTop3ProductDAO: IAreaTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO
    areaTop3ProductDAO.insertBatch(areaTop3products.toList)
  }
}
