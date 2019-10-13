package dao
import domain.SessionDetail
trait ISessionDetailDAO {

  def insert(sessionDetail: SessionDetail)

  def insertBatch(sessionDetails: List[SessionDetail])
}
