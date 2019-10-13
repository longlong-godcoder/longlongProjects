package dao.impl

import dao.ITaskDao
import domain.Task
import jdbc.JDBCHelper
class TaskDAOImpl extends ITaskDao{
  /**
    * 根据主键查询任务
    *
    * @param taskid 主键
    * @return 任务对象
    */
  override def findById(taskid: Long): Task = {
    val task: Task = Task()

    val sql: String = "select * from task where task_id=?"

    val params: Array[AnyVal] = Array[AnyVal](taskid)

    new Task
  }
}
