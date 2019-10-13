package factory
import dao._
import dao.impl._

object DAOFactory {

  def getTaskDAO: ITaskDao ={
    new TaskDAOImpl
  }

  def getAdBlacklistDAO: IAdBlacklistDAO ={
    new AdBlacklistDAOImpl
  }

  def getAdUserClickCountDAO: IAdUserClickCountDAO ={
    new AdUserClickCountDAOImpl
  }

  def getAdStatDAO: IAdStatDAO ={
    new AdStatDAOImpl
  }

  def getAdProvinceTop3DAO: IAdProvinceTop3DAO ={
    new AdProvinceTop3Impl
  }

  def getAdClickTrendDAO: IAdClickTrendDAO ={
    new AdClickTrendDAOImpl
  }

  def getPageSplitConvertRateDAO: IPageSplitConvertRateDAO ={
    new PageSplitConvertRateDAOImpl
  }

  def getSessionRandomExtractDAO: ISessionRandomExtractDAO ={
    new SessionRandomExtractDAOImpl
  }

  def getSessionDetailDAO: ISessionDetailDAO ={
    new SessionDetailDAOImpl
  }

  def getAreaTop3ProductDAO:IAreaTop3ProductDAO ={
    new AreaTop3ProductDAOImpl
  }
}
