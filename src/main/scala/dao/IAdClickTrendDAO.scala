package dao
import domain.AdClickTrend
trait IAdClickTrendDAO {

  def updateBatch(adClickTrends: List[AdClickTrend])
}
