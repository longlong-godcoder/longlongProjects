package dao.impl

import dao.ISessionRandomExtractDAO
import domain.SessionRandomExtract
import jdbc.JDBCHelper

class SessionRandomExtractDAOImpl extends ISessionRandomExtractDAO{

  override def insert(sessionRandomExtract: SessionRandomExtract): Unit = {

    val sql: String = "insert into session_random_extract values(?,?,?,?,?)"
    val params: Array[Any] = Array[Any](
      sessionRandomExtract.taskid,
      sessionRandomExtract.sessionid,
      sessionRandomExtract.startTime,
      sessionRandomExtract.searchKeywords,
      sessionRandomExtract.clickCategoryIds
    )
    JDBCHelper.executeUpdate(sql, params)
  }
}
