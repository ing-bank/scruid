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

package ing.wbaa.druid

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.java8.time._
import io.circe.parser.decode
import org.slf4j.LoggerFactory

import scala.concurrent.Future

object DruidClient extends FailFastCirceSupport with TimeInstances {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit val system       = ActorSystem()
  implicit val materializer = ActorMaterializer()

  implicit val ec = system.dispatcher

  def connectionFlow(
      implicit druidConfig: DruidConfig
  ): Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]] =
    if (druidConfig.secure)
      Http().outgoingConnectionHttps(host = druidConfig.host, port = druidConfig.port)
    else Http().outgoingConnection(host = druidConfig.host, port = druidConfig.port)

  private def handleResponse(
      queryType: QueryType
  )(response: HttpResponse)(implicit druidConfig: DruidConfig): Future[DruidResponse] = {
    val body =
      response.entity
        .toStrict(druidConfig.responseParsingTimeout)
        .map(_.data.decodeString("UTF-8"))
    body.onComplete(b => logger.debug(s"Druid response: $b"))

    if (response.status != StatusCodes.OK) {
      body.flatMap { b =>
        Future.failed(new Exception(s"Got unexpected response from Druid: $b"))
      }
    } else {
      body
        .map(decode[List[DruidResult]])
        .map {
          case Left(error)  => throw new Exception(s"Unable to parse json response: $error")
          case Right(value) => DruidResponse(results = value, queryType = queryType)
        }
    }
  }

  private def executeRequest(
      queryType: QueryType
  )(request: HttpRequest)(implicit druidConfig: DruidConfig): Future[DruidResponse] = {
    logger.debug(
      s"Executing api ${request.method} request to ${request.uri} with entity: ${request.entity}"
    )

    Source
      .single(request)
      .via(connectionFlow)
      .runWith(Sink.head)
      .flatMap(handleResponse(queryType))
  }

  def doQuery(q: DruidQuery)(implicit druidConfig: DruidConfig): Future[DruidResponse] =
    Marshal(q)
      .to[RequestEntity]
      .map { entity =>
        HttpRequest(HttpMethods.POST, uri = druidConfig.url)
          .withEntity(entity.withContentType(`application/json`))
      }
      .flatMap(executeRequest(q.queryType))
}
