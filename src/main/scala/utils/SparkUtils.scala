package utils

import conf.ConfigurationManager
import constant.Constants
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark工具类
  * 石杉大神使用的API比较老，这里需要使用SparkSession做为程序的入口
  * @author longlong
  */
object SparkUtils {

  def mockDate(sc: SparkContext, sQLContext: SQLContext): Unit ={

  }

  def setMaster(conf: SparkConf): Unit ={
    val local: Boolean = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if (local) conf.setMaster("local")
  }

  def getSQLContext(sc: SparkContext): SQLContext={
    //这里SQLContext这种入口已经过时
    val local: Boolean = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if (local) new SQLContext(sc) else new HiveContext(sc)
  }

  def getActionRDDByDateRange: Unit ={
  }


}
