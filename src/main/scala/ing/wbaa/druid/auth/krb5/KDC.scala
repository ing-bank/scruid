package ing.wbaa.druid.auth.krb5

import scala.concurrent.Future

object KDC {
  def getTicket(): Future[String] = Future.successful("fake-ticket")
}
