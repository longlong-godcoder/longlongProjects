package dao.impl

import dao.ISessionDetailDAO
import domain.SessionDetail
import jdbc.JDBCHelper

import scala.collection.mutable.ListBuffer

class SessionDetailDAOImpl extends ISessionDetailDAO{

  override def insert(sessionDetail: SessionDetail): Unit = {

    var sql: String = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)"

    val params: Array[Any] = Array[Any](
      sessionDetail.taskid,
      sessionDetail.userid,
      sessionDetail.sessionid,
      sessionDetail.pageid,
      sessionDetail.actionTIme,
      sessionDetail.searchKeyword,
      sessionDetail.clickCatgoryId,
      sessionDetail.clickProductIds,
      sessionDetail.orderCategoryIds,
      sessionDetail.orderProductIds,
      sessionDetail.payCategoryIds,
      sessionDetail.payProductIds
    )
    JDBCHelper.executeUpdate(sql, params)
  }

  override def insertBatch(sessionDetails: List[SessionDetail]): Unit = {

    var sql: String = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)"

    val paramsList: ListBuffer[Array[Any]] = ListBuffer[Array[Any]]()

    sessionDetails.foreach(sessionDetail => {
      val params: Array[Any] = Array[Any](
        sessionDetail.taskid,
        sessionDetail.userid,
        sessionDetail.sessionid,
        sessionDetail.pageid,
        sessionDetail.actionTIme,
        sessionDetail.searchKeyword,
        sessionDetail.clickCatgoryId,
        sessionDetail.clickProductIds,
        sessionDetail.orderCategoryIds,
        sessionDetail.orderProductIds,
        sessionDetail.payCategoryIds,
        sessionDetail.payProductIds
      )
      paramsList += params
    })

    JDBCHelper.executeBatch(sql, paramsList.toArray)
  }
}
