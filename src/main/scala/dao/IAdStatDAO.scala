package dao
import domain.AdStat
trait IAdStatDAO {

  def updateBatch(adStats: List[AdStat])
}
