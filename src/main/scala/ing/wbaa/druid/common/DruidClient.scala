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

import ing.wbaa.druid.definitions.{Aggregation, Filter}
import ing.wbaa.druid.query.DruidQuery
import org.json4s.FieldSerializer._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.slf4j.LoggerFactory

import scalaj.http.{Http, HttpOptions}

object DruidClient {

  private lazy val httpClient = Http(DruidConfig.url)
    .options(httpOptions)
    .header("Content-Type", "application/json")

  private val logger = LoggerFactory.getLogger(getClass)
  private val httpOptions = Seq(
    HttpOptions.connTimeout(DruidConfig.connectionTimeout),
    HttpOptions.readTimeout(DruidConfig.readTimeout),
    HttpOptions.followRedirects(false)
  )

  implicit val formats: Formats = Serialization.formats(NoTypeHints).preservingEmptyValues +
    FieldSerializer[Aggregation](// Some renaming because type is a reserved keyword in Scala
      renameTo("kind", "type"),
      renameFrom("type", "kind")
    ) + FieldSerializer[Filter](
    renameTo("kind", "type"),
    renameFrom("type", "kind")
  ) + FieldSerializer[DruidQuery[_]]()

  /**
    * Execute the query by performing a POST request to the Druid broker
    *
    * @param q The query to be executed.
    * @return The response json
    */
  def doQuery(q: DruidQuery[_]): String = {
    val res = Extraction.decompose(q)

    val query = compact(render(res))

    val response = httpClient.postData(query)

    logger.debug(s"Druid result: ${response.asString.body}")

    response.asString.body
  }
}


