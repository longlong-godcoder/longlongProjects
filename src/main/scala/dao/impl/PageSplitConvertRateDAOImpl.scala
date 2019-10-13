package dao.impl

import dao.IPageSplitConvertRateDAO
import domain.PageSplitConvertRate
import jdbc.JDBCHelper

class PageSplitConvertRateDAOImpl extends IPageSplitConvertRateDAO{

  override def insert(pageSplitConvertRate: PageSplitConvertRate): Unit = {
    var sql: String = "insert into page_split_convert_rate values(?, ?)"
    val params: Array[Any] = Array[Any](pageSplitConvertRate.taskid, pageSplitConvertRate.convertRate)
    JDBCHelper.executeUpdate(sql, params)
  }
}
