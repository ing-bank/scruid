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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import ing.wbaa.druid._

import scala.concurrent.{ ExecutionContextExecutor, Future }

class DruidHttpClient private (connectionFlow: DruidHttpClient.ConnectionFlowType,
                               queryHost: QueryHost)(implicit system: ActorSystem)
    extends DruidClient
    with DruidResponseHandler {

  private implicit val materializer: ActorMaterializer = ActorMaterializer()
  private implicit val ec: ExecutionContextExecutor    = system.dispatcher

  override def actorSystem: ActorSystem = system

  override def actorMaterializer: ActorMaterializer = materializer

  override def isHealthy()(implicit druidConfig: DruidConfig): Future[Boolean] = {
    val request = HttpRequest(HttpMethods.GET, uri = druidConfig.healthEndpoint)
    Source
      .single(request)
      .via(connectionFlow)
      .runWith(Sink.head)
      .map(_.status == StatusCodes.OK)
      .recover { case _ => false }
  }

  override def healthCheck(implicit druidConfig: DruidConfig): Future[Map[QueryHost, Boolean]] =
    isHealthy.map(outcome => Map(queryHost -> outcome))

  override def doQuery(
      query: DruidQuery
  )(implicit druidConfig: DruidConfig): Future[DruidResponse] =
    Marshal(query)
      .to[RequestEntity]
      .map { entity =>
        HttpRequest(HttpMethods.POST, uri = druidConfig.url)
          .withEntity(entity.withContentType(`application/json`))
      }
      .flatMap(request => executeRequest(query.queryType, request))

  override def doQueryAsStream(
      query: DruidQuery
  )(implicit druidConfig: DruidConfig): Source[DruidResult, NotUsed] =
    Source
      .fromFuture(createHttpRequest(query))
      .via(connectionFlow)
      .flatMapConcat(handleResponseAsStream)

  override def shutdown(): Future[Unit] = Future.successful(())

  private def executeRequest(
      queryType: QueryType,
      request: HttpRequest
  )(implicit druidConfig: DruidConfig): Future[DruidResponse] = {
    logger.debug(
      s"Executing api ${request.method} request to ${request.uri} with entity: ${request.entity}"
    )

    Source
      .single(request)
      .via(connectionFlow)
      .runWith(Sink.head)
      .flatMap(response => handleResponse(response, queryType, druidConfig.responseParsingTimeout))
  }

  private def createHttpRequest(
      q: DruidQuery
  )(implicit druidConfig: DruidConfig): Future[HttpRequest] =
    Marshal(q)
      .to[RequestEntity]
      .map { entity =>
        HttpRequest(HttpMethods.POST, uri = druidConfig.url)
          .withEntity(entity.withContentType(`application/json`))
      }
}

object DruidHttpClient extends DruidClientBuilder {

  type ConnectionFlowType = Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]]

  override val supportsMultipleBrokers: Boolean = false

  override def apply(druidConfig: DruidConfig): DruidClient = {
    implicit val system: ActorSystem = druidConfig.system
    val connectionFlow               = createConnectionFlow(druidConfig)

    new DruidHttpClient(connectionFlow, druidConfig.hosts.head)
  }

  private def createConnectionFlow(
      druidConfig: DruidConfig
  )(implicit actorSystem: ActorSystem): ConnectionFlowType = {
    val QueryHost(host, port) = druidConfig.hosts.head

    if (druidConfig.secure) Http().outgoingConnectionHttps(host, port)
    else Http().outgoingConnection(host, port)
  }

}
