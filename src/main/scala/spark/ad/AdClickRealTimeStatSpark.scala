package spark.ad

import java.util.Date

import conf.ConfigurationManager
import constant.Constants
import dao._
import domain._
import factory.DAOFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.DateUtils

import scala.collection.mutable.ListBuffer



/**
  * 广告点击流量实时统计spark作业
  *
  * @author longlong
  */
object AdClickRealTimeStatSpark {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("AdClickRealTimeStatSpark")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("hdfs目录")
    val adRealTimeLogStream: InputDStream[ConsumerRecord[String, String]] = getKafkaDirectStream(ssc)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  def getKafkaDirectStream(ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] ={
    val params: Map[String, Object] = getKafkaParams
    val topics: Array[String] = getKafkaTopics
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, params)
    )
    stream
  }

  def getKafkaParams: Map[String, Object] ={
    val kafkaParams: Map[String, Object] = Map[String, Object](
      //石杉大神这里使用的是brokerList参数，不知道是不是过时了
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    kafkaParams
  }

  def getKafkaTopics: Array[String] ={
    val kafkaTopicStr: String = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS)
    val kafkaTopicsSplited: Array[String] = kafkaTopicStr.split(",")
    kafkaTopicsSplited
  }

  /**
    * 根据动态黑名单进行数据过滤
    * @param adRealTimeLogStream KafkaStream
    * @return filteredStream (Long, String)
    */
  def filterByBlacklist(adRealTimeLogStream: InputDStream[ConsumerRecord[String, String]]): DStream[String] ={
    val filteredAdRealTimeLogDStream: DStream[String] = adRealTimeLogStream.transform(rdd => {
      val adBlacklistDAO: IAdBlacklistDAO = DAOFactory.getAdBlacklistDAO
      val adblacklists: List[AdBlacklist] = adBlacklistDAO.findAll()
      val tuples: ListBuffer[(Long, Boolean)] = ListBuffer[(Long, Boolean)]()
      for (adBlacklist <- adblacklists) {
        tuples.+=((adBlacklist.userid, true))
      }
      val sc: SparkContext = rdd.context
      val blacklistRDD: RDD[(Long, Boolean)] = sc.parallelize(tuples)
      val mappedRDD: RDD[(Long, String)] = rdd.map(stream => {
        val log: String = stream.value()
        val logSplited: Array[String] = log.split(" ")
        val userid: Long = logSplited(3).toLong
        (userid, log)
      })
      val joinedRDD: RDD[(Long, (String, Option[Boolean]))] = mappedRDD.leftOuterJoin(blacklistRDD)
      val filteredRDD: RDD[(Long, (String, Option[Boolean]))] = joinedRDD.filter(tuple => {
        val option: Option[Boolean] = tuple._2._2
        if (option.getOrElse(false)) {
          false
        }
        true
      })
      val resultRDD: RDD[String] = filteredRDD.map(tuple => {
        tuple._2._1
      })
      resultRDD
    })
    filteredAdRealTimeLogDStream
  }


  def generateDynamicBlackList(filteredAdRealTimeLogDstream: DStream[String]): Unit ={
    val dailyUserAdClickDstream: DStream[(String, Long)] = filteredAdRealTimeLogDstream.map(log => {
      val logSplited: Array[String] = log.split(" ")
      val timeStamp: String = logSplited(0)
      val date: Date = new Date(timeStamp.toLong)
      val daykey: String = DateUtils.formatDateKey(date)
      val userid: String = logSplited(3)
      val adid: String = logSplited(4)
      val key: String = s"${daykey}_${userid}_$adid"
      (key, 1L)
    })
    val dailyUserAdClickCountDStream: DStream[(String, Long)] = dailyUserAdClickDstream.reduceByKey(_ + _)
    dailyUserAdClickCountDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iterrator =>{
        val adUserClickCounts: ListBuffer[AdUserClickCount] = ListBuffer[AdUserClickCount]()
        iterrator.foreach(tuple => {
          val keySplited: Array[String] = tuple._1.split("_")
          val date: String = DateUtils.formatDate(DateUtils.parseDateKey(keySplited(0)))
          val userid: Long = keySplited(1).toLong
          val adid: Long = keySplited(2).toLong
          val clickCount: Long = tuple._2
          val adUserClickCount: AdUserClickCount = AdUserClickCount(date, userid, adid, clickCount)
          adUserClickCounts += adUserClickCount
        })
        val adUserClickCountDAO: IAdUserClickCountDAO = DAOFactory.getAdUserClickCountDAO
        adUserClickCountDAO.updateBatch(adUserClickCounts.toList)
      })
    })

    val blaclistDStream: DStream[(String, Long)] = dailyUserAdClickCountDStream.filter(tuple => {
      var flag: Boolean = false
      val key: String = tuple._1
      val keySplited: Array[String] = key.split("_")
      val date: String = DateUtils.formatDate(DateUtils.parseDateKey(keySplited(0)))
      val userid: Long = keySplited(1).toLong
      val adid: Long = keySplited(2).toLong

      val adUserClickCountDAO: IAdUserClickCountDAO = DAOFactory.getAdUserClickCountDAO
      val clickCount: Int = adUserClickCountDAO.fincClickCountByMutiKey(date, userid, adid)
      if (clickCount >= 100) flag = true
      flag
    })

    val blacklistUseridDStream: DStream[Long] = blaclistDStream.map(tuple => {
      val key: String = tuple._1
      val keysSplited: Array[String] = key.split("_")
      val userid: Long = keysSplited(1).toLong
      userid
    })

    val distinctBlacklistUserDStream: DStream[Long] = blacklistUseridDStream.transform(rdd => {
      rdd.distinct()
    })

    distinctBlacklistUserDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iterator => {
        val adBlacklists: ListBuffer[AdBlacklist] = ListBuffer[AdBlacklist]()
        iterator.foreach(userid => {
          val adBlacklist = AdBlacklist(userid)
          adBlacklists += adBlacklist
        })
        val adBlacklistDAO: IAdBlacklistDAO = DAOFactory.getAdBlacklistDAO
        adBlacklistDAO.insertBatch(adBlacklists.toList)
      })
    })
  }

  def calculateRealTimeStat(filteredAdRealTimeLogDstream: DStream[String]): DStream[(String, Long)] ={
    val mappedDStream: DStream[(String, Long)] = filteredAdRealTimeLogDstream.map(log => {
      val logSplited: Array[String] = log.split(" ")
      val timestamp: String = logSplited(0)
      val date: Date = new Date(timestamp.toLong)
      val datekey: String = DateUtils.formatDateKey(date)

      val province: String = logSplited(1)
      val city: String = logSplited(2)
      val adid: String = logSplited(4)

      val key: String = s"${datekey}_${province}_${city}_$adid"
      (key, 1L)
    })

    val aggregateDStream: DStream[(String, Long)] = mappedDStream.updateStateByKey((values: Seq[Long], option: Option[Long]) => {
      var clickCount: Long = 0L
      if (option.nonEmpty) clickCount = option.get
      for (value <- values) {
        clickCount += value
      }
      Option(clickCount)
    })
    aggregateDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iterator => {
        val adStats: ListBuffer[AdStat] = ListBuffer[AdStat]()
        iterator.foreach(tuple => {
          val keySplited: Array[String] = tuple._1.split("_")
          val datekey: String = keySplited(0)
          val province: String = keySplited(1)
          val city: String = keySplited(2)
          val adid: Long = keySplited(3).toLong

          val clickCount: Long = tuple._2
          val adstat = AdStat()
          adstat.date = datekey
          adstat.provice = province
          adstat.city = city
          adstat.adid = adid
          adstat.clickCount = clickCount

          adStats += adstat
        })

        val adStatDAO: IAdStatDAO = DAOFactory.getAdStatDAO
        adStatDAO.updateBatch(adStats.toList)
      })
    })
    aggregateDStream
  }
  
  def calculateProvinceTop3Ad(adRealTimeStatDstream: DStream[(String, Long)]): Unit ={

    val rowsDStream: DStream[Row] = adRealTimeStatDstream.transform(rdd => {
      val mappedRDD: RDD[(String, Long)] = rdd.map(tuple => {
        val keySplited: Array[String] = tuple._1.split("_")
        val datekey: String = keySplited(0)
        val province: String = keySplited(1)
        val adid: Long = keySplited(3).toLong
        val clickCount: Long = tuple._2
        val key: String = s"${datekey}_${province}_$adid"
        (key, clickCount)
      })

      val dailyAdClickCountByProvinceRDD: RDD[(String, Long)] = mappedRDD.reduceByKey(_ + _)

      val rowsRDD: RDD[Row] = dailyAdClickCountByProvinceRDD.map(tuple => {
        val keySplited: Array[String] = tuple._1.split("_")
        val datekey: String = keySplited(0)
        val province: String = keySplited(1)
        val city: String = keySplited(2)
        val adid: Long = keySplited(3).toLong
        val clickCount: Long = tuple._2

        val date: String = DateUtils.formatDate(DateUtils.parseDateKey(datekey))
        Row(date, province, adid, clickCount)
      })

      val schema: StructType = StructType(Array[StructField](
        StructField("date", StringType, nullable = true),
        StructField("province", StringType, nullable = true),
        StructField("ad_id", LongType, nullable = true),
        StructField("click_count", LongType, nullable = true)
      ))
      //这里我不能再创建一个sparksession而是应该继续沿用根据rdd所在的context
      val sparkSessionOption: Option[SparkSession] = SparkSession.getActiveSession
      val sparkSession: SparkSession = sparkSessionOption.get
      val dailyAdClickCountByProvinceDF: DataFrame = sparkSession.createDataFrame(rowsRDD, schema)
      dailyAdClickCountByProvinceDF.createOrReplaceGlobalTempView("tmp_daily_ad_click_count_by_prov")
      val provinceTop3AdD: DataFrame= sparkSession.sql(
        """
          |SELECT
          |date,
          |province,
          |ad_id,
          |click_count
          |FROM (
          | SELECT
          | date,
          | province,
          | ad_id,
          | click_count,
          | ROW_NUMBER() OVER(PARTITION BY province ORDER BY click_count DESC) rank
          | FROM tmp_daily_ad_click_count_by_prov ) t
          |WHERE rank>=3
        """.stripMargin)
      provinceTop3AdD.rdd
    })

    //更新到mysql
    rowsDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iterator => {
        val adProvinceTops3s: ListBuffer[AdProvinceTop3] = ListBuffer[AdProvinceTop3]()
        iterator.foreach(row => {
          val date: String = row.getString(0)
          val province: String = row.getString(1)
          val adid: Long = row.getLong(2)
          val clickCount: Long = row.getLong(3)

          val adProvinceTop3: AdProvinceTop3 = AdProvinceTop3()
          adProvinceTop3.date = date
          adProvinceTop3.province = province
          adProvinceTop3.adid = adid
          adProvinceTop3.clickCount = clickCount
          adProvinceTops3s += adProvinceTop3
        })
        val adProvinceTop3DAO: IAdProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO
        adProvinceTop3DAO.updateBatch(adProvinceTops3s.toList)
      })
    })
  }

  def calculateAdClickCountByWindow(adRealTimeLogDStream: DStream[String]): Unit ={

    val mappedDStream: DStream[(String, Long)] = adRealTimeLogDStream.map { log => {
      val logSplited: Array[String] = log.split(" ")
      val timeMinute: String = DateUtils.formatTimeMinute(new Date(logSplited(0).toLong))
      val adid: Long = logSplited(4).toLong
      (s"${timeMinute}_$adid", 1L)
    }
    }

    val aggrWindowDStream: DStream[(String, Long)] = mappedDStream.reduceByKeyAndWindow((v1: Long, v2: Long) => v1 + v2, Minutes(60), Seconds(10))

    aggrWindowDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iterator => {
        val adClickTrends: ListBuffer[AdClickTrend] = ListBuffer[AdClickTrend]()
        iterator.foreach(tuple => {
          val keySplited: Array[String] = tuple._1.split("_")
          val dateMinute: String = keySplited(0)
          val adid: Long = keySplited(1).toLong
          val clickCount: Long = tuple._2
          val date: String = DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0, 8)))
          val hour: String = dateMinute.substring(8, 10)
          val mimute: String = dateMinute.substring(10)
          val adClickTrend: AdClickTrend = AdClickTrend()
          adClickTrend.date = date
          adClickTrend.hour = hour
          adClickTrend.minute = mimute
          adClickTrend.adid = adid
          adClickTrend.clickCount = clickCount
          adClickTrends += adClickTrend
        })

        val adClickTrendDAO: IAdClickTrendDAO = DAOFactory.getAdClickTrendDAO
        adClickTrendDAO.updateBatch(adClickTrends.toList)
      })
    })
  }
}
