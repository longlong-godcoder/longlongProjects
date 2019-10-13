package dao
import domain.AdProvinceTop3
trait IAdProvinceTop3DAO {

  def updateBatch( adProvinceTop3s: List[AdProvinceTop3])
}
