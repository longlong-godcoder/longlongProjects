package dao.impl

import dao.IAdProvinceTop3DAO
import domain.AdProvinceTop3
import jdbc.JDBCHelper

import scala.collection.mutable.ListBuffer

class AdProvinceTop3Impl extends IAdProvinceTop3DAO{

  override def updateBatch(adProvinceTop3s: List[AdProvinceTop3]): Unit = {
    //先做去重
    val dateProvinces: ListBuffer[String] = ListBuffer[String]()
    for (adProvinceTop3 <- adProvinceTop3s){
      val date: String = adProvinceTop3.date
      val province: String = adProvinceTop3.province
      val key: String = s"${date}_$province"
      if (!dateProvinces.contains(key)) dateProvinces += key
    }

    //批量删除
    val deleteSQL: String = "DELETE FROM ad_province_top3 WHERE date=? AND province=?"

    val deleteParamsList: ListBuffer[Array[Any]] = ListBuffer[Array[Any]]()
    for (dateProvince <- dateProvinces){
      val dateProvinceSplited: Array[String] = dateProvince.split("_")
      val date: String = dateProvinceSplited(0)
      val province: String = dateProvinceSplited(1)
      val params: Array[Any] = Array[Any](date, province)
      deleteParamsList += params
    }
    JDBCHelper.executeBatch(deleteSQL, deleteParamsList.toArray)

    //批量插入
    val insertSQL: String = "INSERT INTO ad_province_top3 VALUES(?,?,?,?)"
    val insertParamsList: ListBuffer[Array[Any]] = ListBuffer[Array[Any]]()
    for (adPro3vinceTop3 <- adProvinceTop3s){
      val params: Array[Any] = Array[Any](
        adPro3vinceTop3.date,
        adPro3vinceTop3.province,
        adPro3vinceTop3.adid,
        adPro3vinceTop3.clickCount
      )
      insertParamsList += params
    }
    JDBCHelper.executeBatch(insertSQL, insertParamsList.toArray)
  }
}
