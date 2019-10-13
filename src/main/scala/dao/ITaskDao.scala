package dao

import domain.Task

/**
  * 任务管理DAO接口
 *
  * @author longlong
  */
trait ITaskDao {

  /**
    * 根据主键查询任务
    * @param taskid 主键
    * @return 任务对象
    */
  def findById(taskid: Long): Task
}
