package domain

case class SessionDetail() {

  var taskid: Long = _
  var userid: Long = _
  var sessionid: String = _
  var pageid: Long = _
  var actionTIme: String = _
  var searchKeyword: String = _
  var clickCatgoryId: Long = _
  var clickProductIds: Long= _
  var orderProductIds: String = _
  var orderCategoryIds: String = _
  var payCategoryIds: String = _
  var payProductIds: String = _
}
