package spark.session

import java.util.{Date, OptionalLong}

import com.alibaba.fastjson.JSONObject
import constant.Constants
import dao.{ISessionDetailDAO, ISessionRandomExtractDAO}
import domain.{SessionDetail, SessionRandomExtract}
import factory.DAOFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.AccumulatorV2
import utils.{DateUtils, ParamUtils, StringUtils, ValidUtils}

import scala.collection.immutable.StringOps
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object UserVisitSessionAnalyzeSpark {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().getOrCreate()
    import session.implicits._
  }

  def getActionRDDByDateRange(sparkSession: SparkSession, taskParam: JSONObject): DataFrame ={
    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    var sql: String =
      s"""
        |SELECT * FROM user_visit_action
        |WHERE date>='$startDate' AND date<='$endDate'
      """.stripMargin

    sparkSession.sql(sql)
  }

  def getSessionid2ActionRDD(actionRDD: RDD[Row]): RDD[(String, Row)] ={
    //这里为什么要使用mappartions，不会内存溢出么
    actionRDD.mapPartitions(iterator => {
      val tupleList: ListBuffer[(String, Row)] = ListBuffer[(String, Row)]()
      iterator.foreach(row => {
        val sessionid: String = row.getString(2)
        tupleList += ((sessionid, row))
      })
      tupleList.toIterator
    })
  }

  def aggregateBySession(sc: SparkSession, session2actionRDD: RDD[(String, Row)]): Unit ={

    val session2actionRDDs: RDD[(String, Iterable[Row])] = session2actionRDD.groupByKey()

    val userid2PartAggrInfoRDD: RDD[(Long, String)]= session2actionRDDs.map(tuple => {
      val sessionid: String = tuple._1
      val searchKeywordsBuffer: mutable.Buffer[String] = mutable.Buffer[String]()
      val clickCategoryIdsBuffer: mutable.Buffer[String] = mutable.Buffer[String]()
      var userid: Option[Long] = None
      var startTime: Date = null
      var endTime: Date = null
      var stepLength: Int = 0
      tuple._2.foreach(row => {
        if (userid.isEmpty) userid = Some(row.getLong(1))
        val searchKeyword: Option[String] = Some(row.getString(5))
        val clickCategoryId: Option[Long] = Some(row.getLong(6))

        if (StringUtils.isNotEmpty(searchKeyword.get))
          if (!searchKeywordsBuffer.toString().contains(searchKeyword))
            searchKeywordsBuffer += s"$searchKeyword,"
        if (clickCategoryId.nonEmpty)
          if (clickCategoryIdsBuffer.toString().contains(clickCategoryId.get.toString))
            clickCategoryIdsBuffer += s"${clickCategoryId.get},"
        val actionTime: Date = DateUtils.parseTime(row.getString(4))

        if (startTime == null)
          startTime = actionTime
        if (endTime == null)
          endTime = actionTime
        if (actionTime.before(startTime))
          startTime = actionTime
        if (actionTime.after(endTime))
          endTime = actionTime
        stepLength += 1
      })
      val searchKeywords: String = StringUtils.trimComma(searchKeywordsBuffer.toString())
      val clickCategoryIds: String = StringUtils.trimComma(clickCategoryIdsBuffer.toString())
      val visitLength: Long = (endTime.getTime - startTime.getTime) / 1000

      val partAggInfo: String =
        s"""
          |${Constants.FIELD_SESSION_ID}=$sessionid|
          |${Constants.FIELD_SEARCH_KEYWORDS}=$searchKeywords|
          |${Constants.FIELD_CLICK_CATEGORY_IDS}=$clickCategoryIds|
          |${Constants.FIELD_VISIT_LENGTH}=$visitLength|
          |${Constants.FIELD_STEP_LENGTH}=$stepLength|
          |${Constants.FIELD_START_TIME}=${DateUtils.formatTime(startTime)}
        """.stripMargin
      (userid.get, partAggInfo)
    })

    val sql: String = "SELECT * FROM user_info"
    val userInfoRDD: RDD[Row] = sc.sql(sql).rdd
    val userid2InfoRDD: RDD[(Long, Row)] = userInfoRDD.map(row => {
      (row.getLong(0), row)
    })

    val userInfos: Array[(Long, Row)] = userid2InfoRDD.collect()
    val userInfosBroadcast: Broadcast[Array[(Long, Row)]] = sc.sparkContext.broadcast(userInfos)

    val userid2FullInfoRDD: RDD[(Long, (String, Row))] = userid2PartAggrInfoRDD.join(userid2InfoRDD)

    val sessionid2FullAggrInfoRDD: RDD[(String, String)]= userid2FullInfoRDD.map(tuple => {
      val partAggrInfo: String = tuple._2._1
      val userInfoRow: Row = tuple._2._2

      val sessionid: String = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID)
      val age: Int = userInfoRow.getInt(3)
      val professional: String = userInfoRow.getString(4)
      val city: String = userInfoRow.getString(5)
      val sex: String = userInfoRow.getString(6)

      val fullAggInfo: String =
        s"""
          |$partAggrInfo|
          |${Constants.FIELD_AGE}=$age|
          |${Constants.FIELD_PROFESSIONAL}=$professional|
          |${Constants.FIELD_CITY}=$city|
          |${Constants.FIELD_SEX}=$sex
        """.stripMargin
      (sessionid, fullAggInfo)
    })
  }

  def filterSessionAndAggrStat(session2AggrInfoRDD: RDD[(String, String)], taskParam: JSONObject, accumulator: AccumulatorV2[String, String]): RDD[(String, String)] ={
    //通过JSON参数生成参数拼接字符串
    val startAge: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals: String = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities: String = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex: String = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keyWords: String = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds: String = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)
    var _parameter: String =
      s"""
        |${if (startAge != null) s"${Constants.PARAM_START_AGE}=$startAge|" else ""}
        |${if (endAge != null) s"${Constants.PARAM_END_AGE}=$endAge|" else ""}
        |${if (professionals != null) s"${Constants.PARAM_PROFESSIONALS}=$professionals|" else ""}
        |${if (cities != null) s"${Constants.PARAM_CITIES}=$cities|" else ""}
        |${if (sex != null) s"${Constants.PARAM_SEX}=$sex|" else ""}
        |${if (keyWords != null) s"${Constants.PARAM_KEYWORDS}=$keyWords|" else ""}
        |${if (categoryIds != null) s"${Constants.PARAM_CATEGORY_IDS}=$categoryIds|" else ""}
      """.stripMargin
    if (_parameter.endsWith("\\|")) _parameter = _parameter.substring(0, _parameter.length - 1)
    val parameter: String = _parameter

    val filteredSessionid2AggrInfoRDD: RDD[(String, String)] = session2AggrInfoRDD.filter(tuple => {

      val aggrInfo: String = tuple._2

      def calculateVisitLength(visitLength: Long): Unit = {
        if (visitLength >= 1 && visitLength <= 3) accumulator.add(Constants.TIME_PERIOD_1s_3s)
        else if (visitLength >= 4 && visitLength <= 6) accumulator.add(Constants.TIME_PERIOD_4s_6s)
        else if (visitLength >= 7 && visitLength <= 9) accumulator.add(Constants.TIME_PERIOD_7s_9s)
        else if (visitLength >= 10 && visitLength <= 30) accumulator.add(Constants.TIME_PERIOD_10s_30s)
        else if (visitLength > 30 && visitLength <= 60) accumulator.add(Constants.TIME_PERIOD_30s_60s)
        else if (visitLength > 60 && visitLength <= 180) accumulator.add(Constants.TIME_PERIOD_1m_3m)
        else if (visitLength > 180 && visitLength <= 600) accumulator.add(Constants.TIME_PERIOD_3m_10m)
        else if (visitLength > 600 && visitLength <= 1800) accumulator.add(Constants.TIME_PERIOD_10m_30m)
        else if (visitLength > 1800) accumulator.add(Constants.TIME_PERIOD_30m)
      }

      def calculateStepLength(stepLength: Long): Unit = {
        if (stepLength >= 1 && stepLength <= 3) accumulator.add(Constants.STEP_PERIOD_1_3)
        else if (stepLength >= 4 && stepLength <= 6) accumulator.add(Constants.STEP_PERIOD_4_6)
        else if (stepLength >= 7 && stepLength <= 9) accumulator.add(Constants.STEP_PERIOD_7_9)
        else if (stepLength >= 10 && stepLength <= 30) accumulator.add(Constants.STEP_PERIOD_10_30)
        else if (stepLength > 30 && stepLength <= 60) accumulator.add(Constants.STEP_PERIOD_30_60)
        else if (stepLength > 60) accumulator.add(Constants.STEP_PERIOD_60)
      }

      def filter(): Boolean = {
        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) return false
        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) return false
        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) return false
        if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) return false
        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) return false
        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CATEGORY_ID, parameter, Constants.PARAM_CATEGORY_IDS)) return false

        accumulator.add(Constants.SESSION_COUNT)
        val visitLength: Long = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
        val stepLength: Long = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
        calculateVisitLength(visitLength)
        calculateStepLength(stepLength)
        true
      }

      filter()
    })
    filteredSessionid2AggrInfoRDD
  }

  def getSessionid2datailRDD(sessionid2aggrInfoRDD: RDD[(String, String)], sessionid2actionRDD: RDD[(String, Row)]): RDD[(String, (String, Row))]={

    val sessionid2detailRDD: RDD[(String, (String, Row))] = sessionid2aggrInfoRDD.join(sessionid2actionRDD)

    sessionid2detailRDD
  }

  def randomExtractSession(sc: SparkSession, taskid: Long, sessionid2AggrInfoRDD: RDD[(String, String)], sessionid2actionRDD: RDD[(String, Row)]): Unit ={

    val time2aggrInfoRDD: RDD[(String, String)] = sessionid2AggrInfoRDD.map(tuple => {
      val aggrInfo: String = tuple._2
      val startTime: String = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME)
      val dateHour: String = DateUtils.getDateHour(startTime)
      (dateHour, aggrInfo)
    })

    val countMap: collection.Map[String, Long] = time2aggrInfoRDD.countByKey()
    val dateHourCountMap: mutable.Map[String, mutable.Map[String, Long]] = mutable.Map[String, mutable.Map[String, Long]]()
    countMap.foreach(tuple => {
      val dateHour: String = tuple._1
      val date: String = dateHour.split("_")(0)
      val hour: String = dateHour.split("_")(1)
      val count: Long = tuple._2
       var hourCountMapOption: Option[mutable.Map[String, Long]] = dateHourCountMap.get(date)
      if (hourCountMapOption.isEmpty){
        hourCountMapOption = Some(mutable.Map[String, Long]())
        dateHourCountMap.put(date, hourCountMapOption.get)
      }
      hourCountMapOption.get.put(hour, count)
    })
    //100个session，按照天数平均分每天抽取多少个session
    val extactNumberPerDay: Int = 100 / dateHourCountMap.size

    val dateHourExtractMap: mutable.Map[String, mutable.Map[String, ListBuffer[Int]]] = mutable.Map[String, mutable.Map[String, ListBuffer[Int]]]()

    val random: Random = new Random()
    dateHourCountMap.foreach(tuple => {
      val date: String = tuple._1
      val hourCountMap: mutable.Map[String, Long] = tuple._2
      //统计一天一共有多少session
      var sessionCount: Long = 0L
      hourCountMap.foreach(tuple => {
        val hourCount: Long = tuple._2
        sessionCount += hourCount
      })

      var hourExtarctMapOption: Option[mutable.Map[String, ListBuffer[Int]]] = dateHourExtractMap.get(date)
      if (hourExtarctMapOption.isEmpty){
        hourExtarctMapOption = Some(mutable.Map[String, ListBuffer[Int]]())
        dateHourExtractMap.put(date, hourExtarctMapOption.get)
      }
      hourCountMap.foreach(tuple => {
        val hour: String = tuple._1
        val count: Long = tuple._2
        var hourExtractNumber: Int = ((count.toDouble / sessionCount.toDouble) * extactNumberPerDay).toInt
        if (hourExtractNumber > count) hourExtractNumber = count.toInt
        var extractIndexListOption: Option[ListBuffer[Int]] = hourExtarctMapOption.get.get(hour)
        if (extractIndexListOption.isEmpty){
          extractIndexListOption = Some(ListBuffer[Int]())
          hourExtarctMapOption.get.put(hour, extractIndexListOption.get)
        }
        for (i <- 0 until hourExtractNumber){
          var extractIndex: Int = random.nextInt(count.toInt)
          while (extractIndexListOption.get.contains(extractIndex)){
            extractIndex = random.nextInt(count.toInt)
          }
          extractIndexListOption.get += extractIndex
        }
      })
    })

    val dateHourExtractMapBroadcast: Broadcast[mutable.Map[String, mutable.Map[String, ListBuffer[Int]]]] = sc.sparkContext.broadcast(dateHourExtractMap)

    val time2AggrInfosRDD: RDD[(String, Iterable[String])] = time2aggrInfoRDD.groupByKey()

    val extractSessionidsRDD: RDD[(String, String)] = time2AggrInfosRDD.flatMap(tuple => {
      val extractSessionids: ListBuffer[(String, String)] = ListBuffer[(String, String)]()
      val dateHour: String = tuple._1
      val date: String = dateHour.split("_")(0)
      val hour: String = dateHour.split("_")(1)

      val dateHourExtractMap: mutable.Map[String, mutable.Map[String, ListBuffer[Int]]] = dateHourExtractMapBroadcast.value
      val extractIndexList: ListBuffer[Int] = dateHourExtractMap(date)(hour)
      val sessionRandomExtractDAO: ISessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO
      var index: Int = 0
      tuple._2.foreach(sessionAggrInfo => {
        if (extractIndexList.contains(index)) {
          val sessionid: String = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID)
          val sessionRandomExtract: SessionRandomExtract = SessionRandomExtract()
          sessionRandomExtract.taskid = taskid
          sessionRandomExtract.sessionid = sessionid
          sessionRandomExtract.startTime = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME)
          sessionRandomExtract.clickCategoryIds = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CATEGORY_ID)
          sessionRandomExtract.searchKeywords = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
          sessionRandomExtractDAO.insert(sessionRandomExtract)
          extractSessionids += ((sessionid, sessionid))
        }
        index += 1
      })
      extractSessionids
    })

    val extractSessionDetailRDD: RDD[(String, (String, Row))] = extractSessionidsRDD.join(sessionid2actionRDD)

    extractSessionDetailRDD.foreachPartition(iterator => {

      val sessionDetails: ListBuffer[SessionDetail] = ListBuffer[SessionDetail]()

      iterator.foreach(tuple => {
        val row: Row = tuple._2._2
        val sessionDetail = SessionDetail()
        sessionDetail.taskid = taskid
        sessionDetail.userid = row.getLong(1)
        sessionDetail.sessionid = row.getString(2)
        sessionDetail.pageid = row.getLong(3)
        sessionDetail.actionTIme = row.getString(4)
        sessionDetail.searchKeyword = row.getString(5)
        sessionDetail.clickCatgoryId = row.getLong(6)
        sessionDetail.clickProductIds = row.getLong(7)
        sessionDetail.orderCategoryIds = row.getString(8)
        sessionDetail.orderProductIds = row.getString(9)
        sessionDetail.payCategoryIds = row.getString(10)
        sessionDetail.payProductIds = row.getString(11)
        sessionDetails += sessionDetail
      })

      val sessionDetailDAO: ISessionDetailDAO = DAOFactory.getSessionDetailDAO
      sessionDetailDAO.insertBatch(sessionDetails.toList)
    })


  }

  def getTop10Category(taskid: Long, session2detailRDD: RDD[(String, Row)]): Unit ={

    var categoryidRDD: RDD[(Long, Long)] = session2detailRDD.flatMap(tuple => {
      val row: Row = tuple._2
      val list: ListBuffer[(Long, Long)] = ListBuffer[(Long, Long)]()

      val clickCategoryIdOption: Option[Long] = Option(row.getLong(6))
      if (clickCategoryIdOption.nonEmpty) list += ((clickCategoryIdOption.get, clickCategoryIdOption.get))

      val orderCategoryIdsOption: Option[String] = Option(row.getString(8))
      if (orderCategoryIdsOption.nonEmpty) {
        val orderCategoryIdsSplited: Array[String] = orderCategoryIdsOption.get.split("'")
        orderCategoryIdsSplited.foreach(orderCategoryId => {
          list += ((orderCategoryId.toLong, orderCategoryId.toLong))
        })
      }

      val payCategoryIdsOption: Option[String] = Option(row.getString(10))
      if (payCategoryIdsOption.nonEmpty) {
        val payCatgoryIdsSplited: Array[String] = payCategoryIdsOption.get.split(",")
        payCatgoryIdsSplited.foreach(payCategoryId => {
          list += ((payCategoryId.toLong, payCategoryId.toLong))
        })
      }
      list
    })

    categoryidRDD = categoryidRDD.distinct()
  }

  def getClickCategoryId2CountRDDVersion1(sessionid2detailRDD: RDD[(String, Row)]): RDD[(Long, Long)] ={

    //点击行为数据只是一部分数据，过滤完成后数据会出现不均匀的情况，所以需要coalesce击行数据重分区，本地执行请注释
    val clickActionRDD: RDD[(String, Row)] = sessionid2detailRDD.filter(tuple => {
      var flag: Boolean = true
      val row: Row = tuple._2
      if (Option(row.get(6)).isEmpty) flag = false
      flag
    }).coalesce(100)

    val clickCategoryIdRDD: RDD[(Long, Long)] = clickActionRDD.map(tuple => {
      val clickCategoryId: Long = tuple._2.getLong(6)
      (clickCategoryId, 1L)
    })

    val clickCategoryId2CountRDD: RDD[(Long, Long)] = clickCategoryIdRDD.reduceByKey(_ + _)

    clickCategoryId2CountRDD
  }

  def getClickCategoryId2CountRDDVersion2(sessionid2detailRDD: RDD[(String, Row)]): RDD[(Long, Long)]={

    val clickActionRDD: RDD[(String, Row)] = sessionid2detailRDD.filter(tuple => {
      var flag: Boolean = true
      val row: Row = tuple._2
      if (Option(row.get(6)).isEmpty) flag = false
      flag
    })

    //加入随机前缀
    val clickCategoryIdRDD: RDD[(String, Long)] = clickActionRDD.map(tuple => {
      val clickCategoryId: Long = tuple._2.getLong(6)
      val random = new Random()
      val prefix: Int = random.nextInt(10)
      (s"${prefix}_$clickCategoryId", 1L)
    })
    //第一次局部聚合
    val firstAggrRDD: RDD[(String, Long)] = clickCategoryIdRDD.reduceByKey(_ + _)
    //去除随机前缀
    val restoredRDD: RDD[(Long, Long)] = firstAggrRDD.map(tuple => {
      val categoryId: String = tuple._1.split("_")(1)
      (categoryId.toLong, tuple._2)
    })
    //二次聚合
    val clickCategoryCountRDD: RDD[(Long, Long)] = restoredRDD.reduceByKey(_ + _)
    clickCategoryCountRDD
  }

  def getOrderCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, Row)]): RDD[(Long, Long)] ={

    val orderActionRDD: RDD[(String, Row)] = sessionid2detailRDD.filter(tuple => {
      var flag: Boolean = true
      val row: Row = tuple._2
      if (Option(row.get(8)).isEmpty) flag = false
      flag
    })

    val orderCategoryIdRDD: RDD[(Long, Long)] = orderActionRDD.flatMap(tuple => {
      val row: Row = tuple._2
      val orderCategoryIds: String = row.getString(8)
      val orderCateIdsSplited: Array[String] = orderCategoryIds.split(",")
      val list: ListBuffer[(Long, Long)] = ListBuffer[(Long, Long)]()
      orderCateIdsSplited.foreach(orderCategoryId => {
        list += ((orderCategoryId.toLong, 1L))
      })
      list
    })
    val orderCategoryId2CountRDD: RDD[(Long, Long)] = orderCategoryIdRDD.reduceByKey(_ + _)
    orderCategoryId2CountRDD
  }

  def getPayCategoryId2CountRDD(sessionid2detailRDD:  RDD[(String, Row)]): RDD[(Long, Long)] ={

    val payActionRDD: RDD[(String, Row)] = sessionid2detailRDD.filter(tuple => {
      var flag: Boolean = true
      val row: Row = tuple._2
      if (Option(row.get(10)).isEmpty) flag = false
      flag
    })

    val payCategoryIdRDD: RDD[(Long, Long)] = payActionRDD.flatMap(tuple => {
      val row: Row = tuple._2
      val payCategoryIds: String = row.getString(8)
      val payCateIdsSplited: Array[String] = payCategoryIds.split(",")
      val list: ListBuffer[(Long, Long)] = ListBuffer[(Long, Long)]()
      payCateIdsSplited.foreach(payCategoryId => {
        list += ((payCategoryId.toLong, 1L))
      })
      list
    })
    val payCategoryId2CountRDD: RDD[(Long, Long)] = payCategoryIdRDD.reduceByKey(_ + _)
    payCategoryId2CountRDD
  }

  def joinCategoryAndDate(
                           categoryidRDD: RDD[(Long, Long)],
                           clickCategoryId2CountRDD: RDD[(Long, Long)],
                           orderCategoryId2CountRDD: RDD[(Long, Long)], 
                           payCategoryId2CountRDD: RDD[(Long, Long)]): RDD[(Long, String)] ={
    
    val tmpJoinRDD: RDD[(Long, (Long, Option[Long]))] = categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD)

    var tmpMappedRDD: RDD[(Long, String)] = tmpJoinRDD.map(tuple => {
      val categoryid: Long = tuple._1
      val clickCountOption: Option[Long] = tuple._2._2
      var clickCount: Long = 0L
      if (clickCountOption.nonEmpty) clickCount = clickCountOption.get
      var value: String =
        s"""
           |${Constants.FIELD_CATEGORY_ID}=$categoryid|
           |${Constants.FIELD_CLICK_COUNT}=$clickCount
           |""".stripMargin
      (categoryid, value)
    })

    tmpMappedRDD = tmpMappedRDD.leftOuterJoin(orderCategoryId2CountRDD).map(tuple => {
      val categoryid: Long = tuple._1
      var value: String = tuple._2._1
      val orderCountOption: Option[Long] = tuple._2._2
      var orderCount: Long = 0L
      if (orderCountOption.nonEmpty) orderCount = orderCountOption.get
      value = s"$value|${Constants.FIELD_ORDER_COUNT}=$orderCount"
      (categoryid, value)
    })

    tmpMappedRDD = tmpMappedRDD.leftOuterJoin(payCategoryId2CountRDD).map(tuple => {
      val categoryid: Long = tuple._1
      var value: String = tuple._2._1
      val payCountOption: Option[Long] = tuple._2._2
      var payCount: Long = 0L
      if (payCountOption.nonEmpty) payCount = payCountOption.get
      value = s"$value|${Constants.FIELD_PAY_COUNT}=$payCount"
      (categoryid, value)
    })
    tmpMappedRDD
  }
}
