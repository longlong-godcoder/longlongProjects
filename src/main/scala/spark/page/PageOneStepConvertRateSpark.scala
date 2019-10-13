package spark.page

import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import constant.Constants
import dao.{IPageSplitConvertRateDAO, ITaskDao}
import domain.{PageSplitConvertRate, Task}
import factory.DAOFactory
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j
import org.slf4j.LoggerFactory
import utils.{DateUtils, NumberUtils, ParamUtils}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object PageOneStepConvertRateSpark {

  def main(args: Array[String]): Unit = {

    val logger: slf4j.Logger = LoggerFactory.getLogger("PageOneStepConvertRateLogger")
    val ssc: SparkSession = SparkSession.builder()
      .appName(Constants.SPARK_APP_NAME_PAGE)
      .master("local[4]")
      .getOrCreate()

    val taskid: Long = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE)
    val taskDAO: ITaskDao = DAOFactory.getTaskDAO
    val task: Task = taskDAO.findById(taskid)
    if (task == null){
      logger.info(s"${new Date}: cannot find this task with id [$taskid]")
      return
    }
    val taskParam: JSONObject = JSON.parseObject(task.taskParam)


  }

  def getSessionid2actionRDD(actionRDD: RDD[Row]): RDD[(String, Row)] ={
    actionRDD.map(row => {
      val sessionid: String = row.getString(2)
      (sessionid, row)
    })
  }

  def generateAndMatchPageSplit(sparkContext: SparkContext,
                                sessionid2actionsRDD: RDD[(String, Iterable[Row])],
                                taskParam: JSONObject): RDD[(String, Int)] ={
    //获取指定分析的pageid参数，将参数做成广播变量
    val stargetPageFlow: String = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    val targetPageFlowBroadcast: Broadcast[String] = sparkContext.broadcast(stargetPageFlow)

    sessionid2actionsRDD.flatMap(tuple => {
      val tuplesList: ListBuffer[(String, Int)] = ListBuffer[(String, Int)]()
      val targetPages: Array[String] = targetPageFlowBroadcast.value.split(",")
      //将一个sessionid的所有pageview按照时间顺序排序
      val rows: ListBuffer[Row] = ListBuffer[Row]()
      tuple._2.foreach(row => {
        rows += row
      })
      rows.sortWith((row1, row2) => {
        var flag: Boolean = false
        val actionTime1: String = row1.getString(4)
        val actionTime2: String = row2.getString(4)
        val date1: Date = DateUtils.parseTime(actionTime1)
        val date2: Date = DateUtils.parseTime(actionTime2)
        if (date1.getTime - date2.getTime < 0) flag = true
        flag
      })

      var lastPageidOption: Option[Long] = None
      for (row <- rows){
        val pageid: Long = row.getLong(3)

        if (lastPageidOption.isEmpty){
          lastPageidOption = Some(pageid)
        }else{
          val pageSlited: String = s"${lastPageidOption.get.toString}_$pageid"
          var flag: Boolean = true
          for (i <- targetPages.indices if flag){
            val targetPageSplit: String = s"${targetPages(i)}_${targetPages(i + 1)}"
            if (pageSlited.equals(pageSlited)){
              tuplesList.+= ((pageSlited, 1))
              flag = false
            }
          }
          lastPageidOption = Option(pageid)
        }
      }
      tuplesList
    })
  }

  /**
    * 获取startPage的Pv
    * @param taskParam 欲求转化率的页面参数
    * @param sessionid2actionsRDD sessionid聚合后RDD
    * @return
    */
  def getStartPagePv(taskParam: JSONObject, sessionid2actionsRDD: RDD[(String, Iterable[Row])]): Long ={

    val targetPageFlow: String = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    val startPageId: Long = targetPageFlow.split(",")(0).toLong

    val startPageRDD: RDD[Long] = sessionid2actionsRDD.flatMap(tuple => {
      val list: ListBuffer[Long] = ListBuffer[Long]()
      tuple._2.foreach(row => {
        val pageid: Long = row.getLong(3)
        if (pageid == startPageId) list += pageid
      })
      list
    })
    startPageRDD.count()
  }

  def computePageSplitConvertRate(taskParam: JSONObject,
                                  pageSplitPvMap: Map[String, Any],
                                  startPagePv: Long): Map[String, Double] ={

    val convertRateMap: mutable.Map[String, Double] = mutable.Map[String, Double]()

    val targetPages: Array[String] = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",")

    var lastPageSplitPv: Long = 0L

    for (i <- targetPages.indices){
      val targetPageSplit: String = s"${targetPages(i)}_${targetPages(i + 1)}"
      val targetPageSplitPv: Long = pageSplitPvMap.get(targetPageSplit).toString.toLong

      var convertRate: Double = 0.0

      if (i == 0)
        convertRate = NumberUtils.formatDouble(targetPageSplitPv.toDouble / startPagePv.toDouble, 2)
      else
        convertRate = NumberUtils.formatDouble(targetPageSplit.toDouble / lastPageSplitPv.toDouble, 2)

      convertRateMap += ((targetPageSplit, convertRate))

      lastPageSplitPv = targetPageSplitPv
    }
    convertRateMap.toMap
  }

  def persistConvertRate(taskid: Long, convertRateMap: Map[String, Double]): Unit ={
    val buffer: mutable.Buffer[String] = mutable.Buffer[String]()
    convertRateMap.foreach(tuple => {
      val pageSplit: String = tuple._1
      val convertRate: Double = tuple._2
      buffer += s"$pageSplit = ${convertRate.toString}|"
    })
    var convertRate: String = buffer.toString()

    convertRate = convertRate.substring(0, convertRate.length - 1)
    //最后做个DAO即可
    val pageSplitConvertRate = PageSplitConvertRate()
    pageSplitConvertRate.taskid = taskid
    pageSplitConvertRate.convertRate = convertRate
    val pageSplitConvertRateDAO: IPageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO
    pageSplitConvertRateDAO.insert(pageSplitConvertRate)

  }
}
