package dao.impl

import dao.IAdUserClickCountDAO
import domain.AdUserClickCount
import jdbc.JDBCHelper
import model.AdUserClickCountQueryResult
import scala.collection.mutable.ListBuffer

class AdUserClickCountDAOImpl extends IAdUserClickCountDAO{

  override def updateBatch(adUserClickCounts: List[AdUserClickCount]): Unit = {
    val insertAdUserClickCounts: ListBuffer[AdUserClickCount] = ListBuffer[AdUserClickCount]()
    val updateAdUserClickCounts: ListBuffer[AdUserClickCount] = ListBuffer[AdUserClickCount]()
    val selectSQL: String =
      """
        |SELECT count(*) FROM ad_user_click_count
        |WHERE date=? AND user_id=? AND ad_id=?
      """.stripMargin
    var selectParams: Array[Any] = null
    for (adUserClickCount <- adUserClickCounts){
      val queryResult: AdUserClickCountQueryResult = AdUserClickCountQueryResult()
      selectParams = Array[Any](adUserClickCount.date, adUserClickCount.userid, adUserClickCount.adid)
      JDBCHelper.executeQuery(selectSQL, selectParams, rs => {
        if (rs.next()){
          val count: Int = rs.getInt(1)
          queryResult.count = count
        }
      })
      val count: Int = queryResult.count
      if (count > 0) updateAdUserClickCounts += adUserClickCount
      else insertAdUserClickCounts += adUserClickCount
    }

    //执行批量插入
    val insertSQL: String =
      """
        |INSERT INTO ad_user_click_count VALUES(?,?,?,?)
      """.stripMargin
    val insertParamsList: ListBuffer[Array[Any]] = ListBuffer[Array[Any]]()
    for (adUserClickCount <- insertAdUserClickCounts){
      val insertParams: Array[Any] = Array[Any](
        adUserClickCount.date,
        adUserClickCount.userid,
        adUserClickCount.adid,
        adUserClickCount.clickCount
      )
      insertParamsList += insertParams
    }
    JDBCHelper.executeBatch(insertSQL, insertParamsList.toArray)

    //执行批量更新
    val updateSQL: String =
      """
        |UPDATE ad_user_click_count SET click_count=click_count+?
        |WHERE date=? AND user_id=? AND ad+id=?
      """.stripMargin
    val updateParamsList: ListBuffer[Array[Any]] = ListBuffer[Array[Any]]()
    for (adUserClickCount <- updateAdUserClickCounts){
      val updateParams: Array[Any] = Array[Any](
        adUserClickCount.clickCount,
        adUserClickCount.date,
        adUserClickCount.userid,
        adUserClickCount.adid
      )
      updateParamsList += updateParams
    }
    JDBCHelper.executeBatch(updateSQL, updateParamsList.toArray)
  }

  override def fincClickCountByMutiKey(date: String, userid: Long, adid: Long): Int = {
    val sql: String =
      """
        |SELECT click_count FROM ad_user_click_count
        |WHERE date=? AND user_id=? AND ad_id=?
      """.stripMargin
    val params: Array[Any] = Array[Any](date, userid, adid)
    val queryResult: AdUserClickCountQueryResult = AdUserClickCountQueryResult()
    JDBCHelper.executeQuery(sql, params, rs => {
      if (rs.next()){
        val clickCount: Int = rs.getInt(1)
        queryResult.count = clickCount
      }
    })
    queryResult.count
  }
}
