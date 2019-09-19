/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ing.wbaa.druid.client

import akka.http.scaladsl.model._
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent._

/**
  * Customization hook for altering the request flow in `DruidAdvancedHttpClient`. This is primarily intended to handle
  * authentication requirements. The advanced client always uses a single interceptor, which defaults to a no-op
  * implementation. There is no support for chaining interceptors into a filter chain, but you could write a composite
  * interceptor to overcome this.
  */
trait RequestInterceptor {

  /**
    * Intercept the outgoing request before it is transmitted.
    * @param request the outgoing request.
    * @return the request that's to be sent to Druid.
    */
  def interceptRequest(request: HttpRequest): HttpRequest

  /**
    * Intercept the incoming response before it's decoded. This method can be implemented to intercept an incoming
    * authentication challenge, reach out to an authentication system such as a token issuer, and then re-transmit the
    * original request to Druid with appropriate authentication headers.
    *
    * Implementors should be aware that any request sent through the provided `sendToDruid` function will itself pass
    * through the request interception hooks. Be careful not to infinitely recurse.
    *
    * @param request the original, outgoing request.
    * @param response the response that came back from the server.
    * @param sendToDruid call this function to transmit a (modified) request to the Druid endpoint.
    * @param ec asynchronous context.
    * @return the response future containing the answer to the original Druid query.
    */
  def interceptResponse(request: HttpRequest,
                        response: Future[HttpResponse],
                        sendToDruid: HttpRequest => Future[HttpResponse])(
      implicit ec: ExecutionContext
  ): Future[HttpResponse]

  /**
    * Returns the active configuration of this interceptor.
    */
  def exportConfig: Config
}

/**
  * Marker trait for objects that produce a `RequestInterceptor`. One such object is specified in the scruid
  * configuration settings. The default is `NoopRequestInterceptor`.
  */
trait RequestInterceptorBuilder {

  /**
    * Construct the intended interceptor, using the given configuration object.
    * @param config the config object
    * @return a request interceptor. It is an error to return null.
    */
  def apply(config: Config): RequestInterceptor
}

/**
  * Forwards the request and response unmodified. Can be used as a default value, but is also suitable for use as a
  * base class.
  */
class NoopRequestInterceptor extends RequestInterceptor {

  def interceptRequest(request: HttpRequest): HttpRequest = request

  def interceptResponse(
      request: HttpRequest,
      response: Future[HttpResponse],
      requestExecutor: HttpRequest => Future[HttpResponse]
  )(implicit ec: ExecutionContext): Future[HttpResponse] =
    response

  override def exportConfig: Config = ConfigFactory.empty()
}

object NoopRequestInterceptor extends RequestInterceptorBuilder {
  private lazy val instance                              = new NoopRequestInterceptor
  override def apply(config: Config): RequestInterceptor = instance
}
