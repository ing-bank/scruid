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

package ing.wbaa.druid.common

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.util.ByteString
import ing.wbaa.druid.definitions.{Aggregation, Filter}
import ing.wbaa.druid.query.DruidQuery
import org.json4s.FieldSerializer._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.slf4j.LoggerFactory

import scala.concurrent.Future

object DruidClient {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val formats: Formats = Serialization.formats(NoTypeHints).preservingEmptyValues +
    FieldSerializer[Aggregation]() + FieldSerializer[Filter](
    renameTo("kind", "type"),
    renameFrom("type", "kind")
  ) + FieldSerializer[DruidQuery[_]]() + json.AggregationTypeSerializer

  /**
    * Execute the query by performing a POST request to the Druid broker
    *
    * @param q The query to be executed.
    * @return The response json
    */
  def doQuery(q: DruidQuery[_]): Future[String] = {
    val queryClass = Extraction.decompose(q)
    val queryString = compact(render(queryClass))

    logger.debug(s"Druid query: $queryString")

    val request = Http().singleRequest(
      HttpRequest(HttpMethods.POST, uri = DruidConfig.url)
        .withEntity(
          HttpEntity(ContentTypes.`application/json`, queryString)
        )
    )

    for {
      response <- request
      rawBytes <- response.entity.dataBytes.runFold(ByteString(""))(_ ++ _)
    } yield rawBytes.utf8String
  }
}


