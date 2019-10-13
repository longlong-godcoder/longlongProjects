package dao
import  domain.AdBlacklist

trait IAdBlacklistDAO {

  def insertBatch(adBlacklist: List[AdBlacklist])

  def findAll(): List[AdBlacklist]
}
