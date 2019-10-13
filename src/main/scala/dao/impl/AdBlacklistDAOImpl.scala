package dao.impl

import dao.IAdBlacklistDAO
import domain.AdBlacklist
import jdbc.JDBCHelper

import scala.collection.mutable.ListBuffer

class AdBlacklistDAOImpl extends IAdBlacklistDAO{

  override def insertBatch(adBlacklists: List[AdBlacklist]): Unit = {
    val sql: String = "INSERT INTO ad_blacklist VALUES(?)"
    val paramsList: ListBuffer[Array[Any]] = ListBuffer[Array[Any]]()
    for (adBlackList <- adBlacklists){
      val params = Array[Any](adBlackList)
      paramsList += params
    }
    JDBCHelper.executeBatch(sql, paramsList.toArray)
  }

  /**
    * 获取黑名单List
    * @return
    */
  override def findAll(): List[AdBlacklist] = {
    val sql: String = "SELECT * FROM ad_blacklist"
    //无法预测数据量，使用ListBuffer存比较合理
    val adblacklists: ListBuffer[AdBlacklist] = ListBuffer[AdBlacklist]()
    JDBCHelper.executeQuery(sql, null, rs => {
      while (rs.next()){
        val userid: Long = rs.getInt(1).toLong
        adblacklists += AdBlacklist(userid)
      }
    })
    adblacklists.toList
  }
}
