package dao.impl

import dao.IAdStatDAO
import domain.AdStat
import jdbc.JDBCHelper
import model.AdStatQueryResult

import scala.collection.mutable.ListBuffer

class AdStatDAOImpl extends IAdStatDAO{
  override def updateBatch(adStats: List[AdStat]): Unit = {

    val insertAdStats: ListBuffer[AdStat] = ListBuffer[AdStat]()
    val updateAdStats: ListBuffer[AdStat] = ListBuffer[AdStat]()

    val selectSQL: String =
      """
        |SELECT count(*) FROM ad_stat
        |WHERE date=? AND province=? AND city=? AND ad_id=?
      """.stripMargin

    for (adStat <- adStats){
      val queryResult: AdStatQueryResult = AdStatQueryResult()

      val params: Array[Any] = Array[Any](adStat.date, adStat.provice, adStat. city, adStat.adid)
      JDBCHelper.executeQuery(selectSQL, params, rs => {
        val count: Int = rs.getInt(1)
        queryResult.count = count
      })
      val count: Int = queryResult.count
      if (count > 0) updateAdStats += adStat else insertAdStats += adStat
    }

    //对于需要插入的数据，执行批量插入操作
    val insertSQL: String = "INSERT INTO ad_stat VALUES(?,?,?,?,?)"
    val insertParamsList: ListBuffer[Array[Any]] = ListBuffer[Array[Any]]()
    for (adStat <- insertAdStats){
      val params: Array[Any] = Array[Any](adStat.date, adStat.provice, adStat. city, adStat.adid)
      insertParamsList += params
    }
    JDBCHelper.executeBatch(insertSQL, insertParamsList.toArray)

    //对于需要更新的数据，执行批量更新操作
    val updateSQL: String =
      """
        |UPDATE ad_stat SET click_count=? FROM ad_stat
        |WHERE date=? AND province=? AND city=? AND ad_id=?
      """.stripMargin

    val updateParamsList: ListBuffer[Array[Any]] = ListBuffer[Array[Any]]()
    for (adStat <- updateAdStats){
      val params: Array[Any] = Array[Any](adStat.date, adStat.provice, adStat. city, adStat.adid)
      updateParamsList += params
    }
    JDBCHelper.executeBatch(updateSQL, updateParamsList.toArray)
  }
}
