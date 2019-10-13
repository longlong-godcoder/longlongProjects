package jdbc

import conf.ConfigurationManager
import constant.Constants
import scalikejdbc.{AutoSession, ConnectionPool, SQL}

object JDBChelper_Scalike {



  def main(args: Array[String]): Unit = {
    Class.forName(ConfigurationManager.getProperty(Constants.JDBC_DRIVER))
    ConnectionPool.singleton("jdbc:mysql://localhost:3306/spark_project", "root", "root")
    implicit val session: AutoSession.type = AutoSession
    //    val bool: Boolean = SQL("insert into test1 values(10,'scalike')").execute().apply()
        val maps: List[Map[String, Any]] = SQL("select * from test1").map(_.toMap()).list().apply()
    for (i <- maps){
      println(i)
    }



  }

}
