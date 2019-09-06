package ing.wbaa.druid.client

import akka.http.scaladsl.model._
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent._

/**
  * Customization hook for altering the request flow in `DruidAdvancedHttpClient`. This is primarily
  * intended to handle authentication requirements.
  */
trait RequestFlowExtension {

  /**
    * Modify the outgoing request before it is transmitted.
    * @param request the outgoing request.
    * @return the request that's to be sent to Druid.
    */
  def alterRequest(request: HttpRequest): HttpRequest

  /**
    * Modify the incoming response before it's decoded. This method can be implemented to intercept
    * an incoming authentication challenge, reach out to an authentication system such as a token
    * issuer, and then re-transmit the original request to Druid with appropriate authentication
    * headers.
    *
    * Implementors should be aware that any request sent through the provided `druidEndpoint` will
    * itself pass through the request flow extension. Be careful not to infinitely recurse.
    *
    * @param request the original, outgoing request.
    * @param response the response that came back from the server.
    * @param druidEndpoint invoke to transmit a request to Druid.
    * @param ec asynchronous context.
    * @return the response future containing the answer to the original Druid query.
    */
  def alterResponse(request: HttpRequest,
                    response: Future[HttpResponse],
                    druidEndpoint: HttpRequest => Future[HttpResponse])(
      implicit ec: ExecutionContext
  ): Future[HttpResponse]

  /**
    * Returns the active configuration of this extension.
    */
  def exportConfig: Config
}

/**
  * Marker trait for objects that produce a `RequestFlowExtension`.
  */
trait RequestFlowExtensionBuilder {
  def apply(config: Config): RequestFlowExtension
}

/**
  * Forwards the request and response unmodified. Can be used as a null object, but is also suitable
  * for use as a base class.
  */
class NoopRequestFlowExtension extends RequestFlowExtension {

  def alterRequest(request: HttpRequest): HttpRequest = request

  def alterResponse(
      request: HttpRequest,
      response: Future[HttpResponse],
      requestExecutor: HttpRequest => Future[HttpResponse]
  )(implicit ec: ExecutionContext): Future[HttpResponse] =
    response

  override def exportConfig: Config = ConfigFactory.empty()
}

object NoopRequestFlowExtension extends RequestFlowExtensionBuilder {
  private lazy val instance                                = new NoopRequestFlowExtension
  override def apply(config: Config): RequestFlowExtension = instance
}
