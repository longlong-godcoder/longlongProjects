package dao

import domain.AreaTop3Product

trait IAreaTop3ProductDAO {

  def insertBatch(areaTop3Products: List[AreaTop3Product])
}
