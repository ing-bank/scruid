package ing.wbaa.druid.client

import scala.concurrent._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import com.typesafe.config.{ Config, ConfigFactory, ConfigValueFactory }

trait RequestFlowExtension {
  def alterRequest(request: HttpRequest): HttpRequest
  def alterResponse(request: HttpRequest,
                    response: Future[HttpResponse],
                    requestExecutor: HttpRequest => Future[HttpResponse])(
      implicit ec: ExecutionContext
  ): Future[HttpResponse]

  def exportConfig: Config
}

trait RequestFlowExtensionBuilder {

  def apply(config: Config): RequestFlowExtension

}

class NoRequestFlowExtension extends RequestFlowExtension {
  def alterRequest(request: HttpRequest): HttpRequest = request
  def alterResponse(
      request: HttpRequest,
      response: Future[HttpResponse],
      requestExecutor: HttpRequest => Future[HttpResponse]
  )(implicit ec: ExecutionContext): Future[HttpResponse] =
    response

  override def exportConfig: Config = ConfigFactory.empty()
}

object NoRequestFlowExtension extends RequestFlowExtensionBuilder {
  private lazy val instance                                = new NoRequestFlowExtension
  override def apply(config: Config): RequestFlowExtension = instance
}

class BasicAuthenticationExtension(username: String, password: String)
    extends RequestFlowExtension {
  def alterRequest(request: HttpRequest): HttpRequest =
    request.withHeaders(Authorization(BasicHttpCredentials(username, password)))

  def alterResponse(
      request: HttpRequest,
      response: Future[HttpResponse],
      requestExecutor: HttpRequest => Future[HttpResponse]
  )(implicit ec: ExecutionContext): Future[HttpResponse] =
    response

  override def exportConfig: Config =
    ConfigFactory
      .empty()
      .withValue("username", ConfigValueFactory.fromAnyRef(username))
      .withValue("password", ConfigValueFactory.fromAnyRef(password))
}

object BasicAuthenticationExtension extends RequestFlowExtensionBuilder {

  override def apply(config: Config): RequestFlowExtension = {

    val username =
      Option(config.getString("username")).getOrElse {
        throw new IllegalStateException(
          "BasicAuthenticationExtension requires 'username' configuration parameter to be specified"
        )
      }

    val password =
      Option(config.getString("password")).getOrElse {
        throw new IllegalStateException(
          "BasicAuthenticationExtension requires 'password' configuration parameter to be specified"
        )
      }

    new BasicAuthenticationExtension(username, password)
  }
}

object KDC {
  def getTicket(): Future[String] = Future.successful("fake-ticket")
}

class KerberosAuthenticationExtension extends RequestFlowExtension {
  private var cachedTicket: Option[String] = None

  def alterRequest(request: HttpRequest): HttpRequest =
    cachedTicket
      .map { ticket =>
        request.withHeaders(Authorization(OAuth2BearerToken(ticket)))
      }
      .getOrElse(request)
  def alterResponse(
      request: HttpRequest,
      response: Future[HttpResponse],
      requestExecutor: HttpRequest => Future[HttpResponse]
  )(implicit ec: ExecutionContext): Future[HttpResponse] =
    response.map(_.status).flatMap {
      case StatusCodes.Unauthorized =>
        KDC.getTicket().flatMap { ticket =>
          cachedTicket = Some(ticket)
          requestExecutor(request)
        }
      case _ => response

    }

  override def exportConfig: Config = ConfigFactory.empty()
}

object KerberosAuthenticationExtension extends RequestFlowExtensionBuilder {

  override def apply(config: Config): RequestFlowExtension = new KerberosAuthenticationExtension

}
