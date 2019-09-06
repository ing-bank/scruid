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
import akka.http.scaladsl.model.{ HttpEntity, HttpResponse, StatusCodes }
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import ing.wbaa.druid._
import io.circe.java8.time._
import org.mdedetrich.akka.http.support.CirceHttpSupport
import org.slf4j.{ Logger, LoggerFactory }
import io.circe.parser.decode
import org.mdedetrich.akka.stream.support.CirceStreamSupport
import org.typelevel.jawn.AsyncParser

import scala.concurrent.{ ExecutionContextExecutor, Future }
import scala.concurrent.duration.FiniteDuration
import scala.util.{ Failure, Success, Try }

trait DruidClient extends CirceHttpSupport with JavaTimeDecoders {

  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  logger.info(s"Using '${this.getClass.getName}' as http client")

  /**
    * @return the underlying [[akka.actor.ActorSystem]]
    */
  def actorSystem: ActorSystem

  /**
    * @return the underlying [[akka.stream.ActorMaterializer]]
    */
  def actorMaterializer: ActorMaterializer

  /**
    * Checks the health status of all Druid query hosts
    *
    * @param druidConfig the configuration options
    *
    * @return true when all Druid query hosts are healthy, otherwise false
    */
  def isHealthy()(implicit druidConfig: DruidConfig): Future[Boolean]

  /**
    *
    * @param druidConfig the configuration options
    *
    * @return a Map with the health status of each Druid query host,
    *         where each host is associated with a Boolean value indicating whether
    *         the host is healthy (true) or not (false)
    */
  def healthCheck(implicit druidConfig: DruidConfig): Future[Map[QueryHost, Boolean]]

  /**
    * Perform a query to Druid and get its resulting response
    *
    * @param query the query to perform
    * @param druidConfig the configuration options
    * @return a Future holding the resulting response
    */
  def doQuery(query: DruidQuery)(implicit druidConfig: DruidConfig): Future[DruidResponse]

  /**
    * Perform a query to Druid and get its result as an Akka Stream
    * [[akka.stream.scaladsl.Source]] of [[ing.wbaa.druid.DruidResult]].
    *
    * This is useful for queries that result to responses with large payloads
    * (Druid responds with streaming JSON)
    *
    * @param query the query to perform
    * @param druidConfig the Druid configuration options
    * @return the resulting Akka Streams Source of [[ing.wbaa.druid.DruidResult]]
    */
  def doQueryAsStream(query: DruidQuery)(
      implicit druidConfig: DruidConfig
  ): Source[DruidResult, NotUsed]

  /**
    * Shutdown the client and close the active connections to Druid
    *
    * @return a Future which completes successfully when all active connections have been closed
    */
  def shutdown(): Future[Unit]
}

trait DruidClientBuilder {

  /**
    * Should be set to true when the resulting implementation of [[ing.wbaa.druid.client.DruidClient]] supports
    * multiple Druid Query hosts (Brokers)
    */
  val supportsMultipleBrokers: Boolean

  /**
    * Build an instance of [[ing.wbaa.druid.client.DruidClient]] given the specified [[ing.wbaa.druid.DruidConfig]]
    * configuration options
    *
    * @param druidConfig the configuration options
    * @return an instance of DruidClient
    */
  def apply(druidConfig: DruidConfig): DruidClient

}

trait DruidResponseHandler {
  self: DruidClient =>

  protected def handleResponse(
      response: HttpResponse,
      queryType: QueryType,
      responseParsingTimeout: FiniteDuration
  )(implicit materializer: ActorMaterializer,
    ec: ExecutionContextExecutor): Future[DruidResponse] = {

    val body =
      response.entity
        .toStrict(responseParsingTimeout)
    body.onComplete(b => logger.debug(s"Druid response: $b"))

    if (response.status != StatusCodes.OK) {
      body
        .map(Success(_))
        .recover {
          case t: Throwable => Failure(t)
        }
        .flatMap { entity: Try[HttpEntity.Strict] =>
          Future.failed(
            new HttpStatusException(response.status, response.protocol, response.headers, entity)
          )
        }
    } else {
      body
        .map(_.data.decodeString("UTF-8"))
        .map(decode[List[DruidResult]])
        .map {
          case Left(error)  => throw new Exception(s"Unable to parse json response: $error")
          case Right(value) => DruidResponse(results = value, queryType = queryType)
        }
    }
  }

  protected def handleResponseAsStream(
      response: HttpResponse
  ): Source[DruidResult, NotUsed] =
    response.entity
      .withoutSizeLimit()
      .dataBytes
      .via(CirceStreamSupport.decode[DruidResult](AsyncParser.UnwrapArray))
      .mapMaterializedValue(_ => NotUsed)
}
