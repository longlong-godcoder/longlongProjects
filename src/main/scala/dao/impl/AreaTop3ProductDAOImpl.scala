package dao.impl

import dao.IAreaTop3ProductDAO
import domain.AreaTop3Product
import jdbc.JDBCHelper

import scala.collection.mutable.ListBuffer

class AreaTop3ProductDAOImpl extends IAreaTop3ProductDAO{

  override def insertBatch(areaTop3Products: List[AreaTop3Product]): Unit = {

    val sql: String = "INSERT INTO area_top3_product VALUES(?,?,?,?,?,?,?,?)"

    val paramsList: ListBuffer[Array[Any]] = ListBuffer[Array[Any]]()

    areaTop3Products.foreach(areaTop3Product => {
      val params: Array[Any] = Array[Any](
        areaTop3Product.taskid,
        areaTop3Product.area,
        areaTop3Product.areaLevel,
        areaTop3Product.productid,
        areaTop3Product.cityInfos,
        areaTop3Product.clickCount,
        areaTop3Product.productName,
        areaTop3Product.productStatus
      )
      paramsList += params
    })

    JDBCHelper.executeBatch(sql, paramsList.toArray)
  }
}
