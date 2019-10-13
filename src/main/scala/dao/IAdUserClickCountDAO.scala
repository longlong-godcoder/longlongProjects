package dao
import domain.AdUserClickCount
trait IAdUserClickCountDAO {

  def updateBatch(adUserClickCounts: List[AdUserClickCount])

  def fincClickCountByMutiKey(date: String, userid: Long, adid: Long): Int
}
