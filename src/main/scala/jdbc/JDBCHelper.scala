package jdbc

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import conf.ConfigurationManager
import constant.Constants
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * 使用JAVA原生的JDBC接口的JDBCHelper
  */
object JDBCHelper{
  private val logger: Logger = LoggerFactory.getLogger("JDBCHelperLogger")
  private val datasource: mutable.Queue[Connection] = mutable.Queue[Connection]()

  /**
    * 加载JDBC驱动
    */
  try {
    val driver: String = ConfigurationManager.getProperty(Constants.JDBC_DRIVER)
    Class.forName(driver)
    logger.trace("=====JDBC驱动加载成功=====")
  } catch {
    case e: Exception => e.printStackTrace()
  }

  /**
    * 初始化连接池
    */
  {
    val datasourceSize: Int = ConfigurationManager.getInt(Constants.JDBC_DATASOURCE_SIZE)

    for (i <- 1 to datasourceSize) {
      val local: Boolean = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
      var url: String = null
      var user: String = null
      var password: String = null

      if (local) {
        url = ConfigurationManager.getProperty(Constants.JDBC_URL)
        user = ConfigurationManager.getProperty(Constants.JDBC_USER)
        password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
      } else {
        url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD)
        user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD)
        password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD)
      }

      logger.info(
        s"""=====成功获取连接信息======
          |"url: " + $url
          |"user: " + $user
          |"password: " + $password
        """.stripMargin)

      try {
        val connection: Connection = DriverManager.getConnection(url, user, password)
        datasource.enqueue(connection)
        logger.info(s"=====加入第${i}一个Connection=====")
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }

  }

  /**
    * 获取JDBC连接
    * @return Connection
    */
  def getConnection: Connection ={
    JDBCHelper.synchronized{
      while (datasource.isEmpty){
        try {
          Thread.sleep(10)
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
      logger.info("=====获取一个Connection=====")
      datasource.dequeue()
    }
  }

  /**
    * 更新一条数据
    * @param sql SQL
    * @param params 占位符参数
    * @return
    */
  def executeUpdate(sql: String, params: Array[Any]): Int ={
    var rtn: Int = 0
    var conn: Connection = null
    var pstmt: PreparedStatement= null

    try {
      conn = getConnection
      conn.setAutoCommit(false)
      pstmt = conn.prepareStatement(sql)

      if (params != null && params.nonEmpty){
        for (i <- 1 to params.length){
          pstmt.setObject(i, params(i - 1))
        }
      }

      rtn = pstmt.executeUpdate()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    }finally {
      if (conn != null) datasource.enqueue(conn)
    }
    rtn
  }

  /**
    * 执行一条查询语句
    * @param sql SQL
    * @param params 占位符参数
    * @param callBack 查询回到逻辑函数
    */
  def executeQuery(sql: String, params: Array[Any], callBack: ResultSet => Unit): Unit ={
    var rs: ResultSet = null
    var conn: Connection = null
    var pstmt: PreparedStatement= null

    try {
      conn = getConnection
      pstmt = conn.prepareStatement(sql)

      if (params != null && params.nonEmpty){
        for (i <- 1 to params.length){
          pstmt.setObject(i, params(i - 1))
        }
      }
      rs = pstmt.executeQuery()
      callBack(rs)
    } catch {
      case e: Exception => e.printStackTrace()
    }finally {
      if (conn != null) datasource.enqueue(conn)
    }
  }

  /**
    * 批量执行SQL语句
    * @param sql SQL
    * @param paramsArray paramsArray
    * @return
    */
  def executeBatch(sql: String, paramsArray: Array[Array[Any]]): Array[Int] ={
    var rtn:Array[Int] = null
    var conn: Connection = null
    var pstmt: PreparedStatement= null

    try {
      conn = getConnection
      conn.setAutoCommit(false)
      pstmt = conn.prepareStatement(sql)

      if (paramsArray != null && paramsArray.nonEmpty) {
        for (params <- paramsArray) {
          for (i <- 1 to params.length) {
            pstmt.setObject(i, params(i - 1))
          }
          pstmt.addBatch()
        }
      }

      rtn = pstmt.executeBatch()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (conn != null) datasource.enqueue(conn)
    }
    rtn
  }

}
