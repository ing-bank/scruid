package ing.wbaa.druid.auth.krb5

import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken }
import akka.http.scaladsl.model.{ HttpRequest, HttpResponse, StatusCodes }
import com.typesafe.config.{ Config, ConfigFactory }
import ing.wbaa.druid.client.{ RequestFlowExtension, RequestFlowExtensionBuilder }

import scala.concurrent.{ ExecutionContext, Future }

/** Experimental code, do not use. */
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
