package dao.impl

import dao.IAdClickTrendDAO
import domain.AdClickTrend
import jdbc.JDBCHelper
import model.AdClickTrendQueryResult

import scala.collection.mutable.ListBuffer

class AdClickTrendDAOImpl extends IAdClickTrendDAO{

  override def updateBatch(adClickTrends: List[AdClickTrend]): Unit = {
    val updateAdClickTrends: ListBuffer[AdClickTrend] = new ListBuffer[AdClickTrend]()
    val insertAdClickTrends: ListBuffer[AdClickTrend] = ListBuffer[AdClickTrend]()

    val selectSQL: String =
      """
        |SELECT count(*) FROM ad_click_trend
        |WHERE date=? AND hour=? AND minute=? AND ad_id=?
      """.stripMargin

    for (adClickTrend <- adClickTrends){
      val queryResult: AdClickTrendQueryResult = AdClickTrendQueryResult()
      val params: Array[Any] = Array[Any](
        adClickTrend.date,
        adClickTrend.hour,
        adClickTrend.minute,
        adClickTrend.adid
      )
      JDBCHelper.executeQuery(selectSQL, params, rs => {
        if (rs.next()){
          val count: Int = rs.getInt(1)
          queryResult.count = count
        }
      })
      val count: Int = queryResult.count
      if (count > 0) updateAdClickTrends += adClickTrend else insertAdClickTrends += adClickTrend

      //执行更新操作
      val updateSQL: String =
        """
          |UPDATE ad_click_trend SET click_count=?
          |WHERE AND hour=? AND minute=? AND ad_id=?
        """.stripMargin

      val updateParamsList: ListBuffer[Array[Any]] = ListBuffer[Array[Any]]()
      for (adClickTrend <- updateAdClickTrends){
        val params: Array[Any] = Array[Any](
          adClickTrend.clickCount,
          adClickTrend.date,
          adClickTrend.hour,
          adClickTrend.minute,
          adClickTrend.adid
        )
        updateAdClickTrends += adClickTrend
      }
      JDBCHelper.executeBatch(updateSQL, updateParamsList.toArray)

      //执行批量
      val insertSQL: String =
        """
          |INSERT INTO ad_click_trend VALUES(?,?,?,?,?)
        """.stripMargin

      val insertParamsList: ListBuffer[Array[Any]] = ListBuffer[Array[Any]]()
      for (adClickTrend <- insertAdClickTrends){
        val params: Array[Any] = Array[Any](
          adClickTrend.date,
          adClickTrend.hour,
          adClickTrend.minute,
          adClickTrend.adid,
          adClickTrend.clickCount
        )
        insertParamsList += params
      }
      JDBCHelper.executeBatch(insertSQL, insertParamsList.toArray)
    }
  }
}
