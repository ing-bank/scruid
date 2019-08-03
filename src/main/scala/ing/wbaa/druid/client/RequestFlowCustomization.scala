package ing.wbaa.druid.client

import scala.concurrent._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._

trait RequestFlowCustomization {
  def alterRequest(request: HttpRequest): HttpRequest
  def alterResponse(request: HttpRequest,
                    response: Future[HttpResponse],
                    requestExecutor: HttpRequest => Future[HttpResponse])(
      implicit ec: ExecutionContext
  ): Future[HttpResponse]
}

object NoSpecialRequestFlowCustomization extends RequestFlowCustomization {
  def alterRequest(request: HttpRequest) = request
  def alterResponse(
      request: HttpRequest,
      response: Future[HttpResponse],
      requestExecutor: HttpRequest => Future[HttpResponse]
  )(implicit ec: ExecutionContext) =
    response
}

class BasicAuthenticationAddition(username: String, password: String)
    extends RequestFlowCustomization {
  def alterRequest(request: HttpRequest) =
    request.withHeaders(Authorization(BasicHttpCredentials(username, password)))
  def alterResponse(
      request: HttpRequest,
      response: Future[HttpResponse],
      requestExecutor: HttpRequest => Future[HttpResponse]
  )(implicit ec: ExecutionContext) =
    response
}

object KDC {
  def getTicket(): Future[String] = Future.successful("fake-ticket")
}

object KerberosAuthenticationAddition extends RequestFlowCustomization {
  private var cachedTicket: Option[String] = None

  def alterRequest(request: HttpRequest) =
    cachedTicket
      .map { ticket =>
        request.withHeaders(Authorization(OAuth2BearerToken(ticket)))
      }
      .getOrElse(request)
  def alterResponse(
      request: HttpRequest,
      response: Future[HttpResponse],
      requestExecutor: HttpRequest => Future[HttpResponse]
  )(implicit ec: ExecutionContext) =
    response.map(_.status).flatMap {
      case StatusCodes.Success(_) => response
      case StatusCodes.Unauthorized =>
        KDC.getTicket().flatMap { ticket =>
          cachedTicket = Some(ticket)
          requestExecutor(request)
        }
    }
}
