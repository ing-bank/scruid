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

package ing.wbaa.druid.auth.basic

import akka.http.scaladsl.model.StatusCodes
import ing.wbaa.druid.client.{ DruidAdvancedHttpClient, HttpStatusException }
import ing.wbaa.druid.definitions._
import ing.wbaa.druid.{ DruidConfig, QueryHost, TimeSeriesQuery }
import io.circe.generic.auto._
import org.scalatest._
import org.scalatest.concurrent._

import scala.concurrent.duration._
import scala.language.postfixOps

class BasicAuthenticationSpec extends WordSpec with Matchers with ScalaFutures {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(5 minutes, 100 millis)
  private val totalNumberOfEntries                     = 39244
  private val basicAuthenticationAddition =
    new BasicAuthenticationExtension(username = "user", password = "aloha")

  case class TimeseriesCount(count: Int)

  "TimeSeriesQuery without Basic Auth" should {

    implicit val config = DruidConfig(
      clientBackend = classOf[DruidAdvancedHttpClient],
      clientConfig = DruidAdvancedHttpClient
        .ConfigBuilder()
        .build(),
      hosts = Seq(QueryHost("localhost", 8088))
    )

    "get 401 Auth Required when querying Druid without Authentication config" in {
      val request = TimeSeriesQuery(
        aggregations = List(
          CountAggregation(name = "count")
        ),
        granularity = GranularityType.Hour,
        intervals = List("2011-06-01/2017-06-01")
      ).execute

      whenReady(request.failed) { throwable =>
        throwable shouldBe a[HttpStatusException]
        throwable.asInstanceOf[HttpStatusException].status shouldBe StatusCodes.Unauthorized
      }
    }
  }

  "TimeSeriesQuery with Basic Auth" should {

    implicit val config = DruidConfig(
      clientBackend = classOf[DruidAdvancedHttpClient],
      clientConfig = DruidAdvancedHttpClient
        .ConfigBuilder()
        .withRequestInterceptor(basicAuthenticationAddition)
        .build(),
      hosts = Seq(QueryHost("localhost", 8088))
    )

    "successfully query Druid when an Authentication config is set" in {
      val request = TimeSeriesQuery(
        aggregations = List(
          CountAggregation(name = "count")
        ),
        granularity = GranularityType.Hour,
        intervals = List("2011-06-01/2017-06-01")
      ).execute

      whenReady(request) { response =>
        response.list[TimeseriesCount].map(_.count).sum shouldBe totalNumberOfEntries
      }
    }
  }

}
