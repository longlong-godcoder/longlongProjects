package domain

import scala.beans.BeanProperty


case class Task(){

  var taskid: Long = _
  var taskName: String = _
  var createTime: String = _
  var startTime: String =  _
  var finishTime: String = _
  var taskType: String = _
  var taskStatus: String = _
  var taskParam: String =  _

}
