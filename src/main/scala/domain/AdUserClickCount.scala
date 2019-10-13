package domain

case class AdUserClickCount(
                           var date: String,
                           var userid: Long,
                           var adid: Long,
                           var clickCount: Long
                           ) {

}
