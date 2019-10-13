package test

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object GenerateMockData {
  def main(args: Array[String]): Unit = {
    val path: String = new File("spark-warehouse").getAbsolutePath
    print(path)
    val sparkSession: SparkSession = SparkSession.builder().master("local[4]").appName("GenerateMockData").getOrCreate()
//    MockData.mock(sparkSession)
    val arr: Array[Row] = Array[Row](Row(1, "yaoming"), Row(2, "leo"))
    val arrRDD: RDD[Row] = sparkSession.sparkContext.parallelize(arr)
    val structType = StructType(Array[StructField](StructField("id", IntegerType, nullable = true), StructField("name", StringType, nullable = true)))

    val df: DataFrame = sparkSession.createDataFrame(arrRDD, structType)
    df.createOrReplaceGlobalTempView("testTable")

    val testSQL1: String = "select * from testTable"
    val dataFrame: DataFrame = sparkSession.sql(testSQL1)
    dataFrame.show()
    sparkSession.close()
  }
}
