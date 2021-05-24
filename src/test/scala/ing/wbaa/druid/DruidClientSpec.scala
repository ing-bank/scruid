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

import java.util.concurrent.TimeoutException

import scala.concurrent.duration._
import scala.language.postfixOps

import akka.http.scaladsl.model.{ HttpProtocols, StatusCodes }
import akka.http.scaladsl.model.headers.RawHeader
import ing.wbaa.druid.client.{ DruidHttpClient, HttpStatusException }
import ing.wbaa.druid.definitions.{ CountAggregation, GranularityType }
import ing.wbaa.druid.util._
import org.scalatest.concurrent._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DruidClientSpec extends AnyWordSpec with Matchers with ScalaFutures {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(10 seconds, 100 millis)

  "DruidClient" should {

    "indicate when Druid is healthy" in {
      val config = DruidConfig(clientBackend = classOf[DruidHttpClient])
      val client = config.client

      whenReady(client.isHealthy()) { result =>
        result shouldBe true
      }
    }

    "indicate when Druid is not healthy" in {
      val config = DruidConfig(clientBackend = classOf[DruidHttpClient],
                               hosts = Seq(QueryHost("localhost", 8187)))
      val client = config.client

      whenReady(client.isHealthy()) { result =>
        result shouldBe false
      }
    }

    "fail to load when having multiple query nodes" in {
      val config = DruidConfig(clientBackend = classOf[DruidHttpClient],
                               hosts =
                                 Seq(QueryHost("localhost", 8082), QueryHost("localhost", 8183)))

      assertThrows[IllegalStateException] {
        config.client
      }
    }

    "throw HttpStatusException for non-200 status codes" in {
      val config = DruidConfig(clientBackend = classOf[DruidHttpClient],
                               hosts = Seq(QueryHost("localhost", 8186))) // yields HTTP 500
      val client = config.client
      val responseFuture = client.doQuery(
        TimeSeriesQuery(
          aggregations = List(
            CountAggregation(name = "count")
          ),
          granularity = GranularityType.Hour,
          intervals = List("2011-06-01/2017-06-01")
        )
      )

      whenReady(responseFuture.failed) {
        case exception: HttpStatusException =>
          exception.status shouldBe StatusCodes.InternalServerError
          exception.protocol shouldBe HttpProtocols.`HTTP/1.1`
          exception.headers should contain(new RawHeader("x-clusterfk-status-code", "500"))
          exception.entity.get.isKnownEmpty() shouldBe true
        case response => fail(s"expected HttpStatusException, got $response")
      }

      client.shutdown().futureValue
    }

    "throw HttpStatusException for non-200 status codes where body fails to materialize" in {
      // the endpoint on 8087 returns HTTP 502 and takes 5 seconds to send the response body
      implicit val config =
        DruidConfig(
          clientBackend = classOf[DruidHttpClient],
          responseParsingTimeout = 1.seconds,
          hosts = Seq(QueryHost("localhost", 8187))
        )

      val client = config.client
      val responseFuture = client.doQuery(
        TimeSeriesQuery(
          aggregations = List(
            CountAggregation(name = "count")
          ),
          granularity = GranularityType.Hour,
          intervals = List("2011-06-01/2017-06-01")
        )
      )

      whenReady(responseFuture.failed) {
        case exception: HttpStatusException =>
          exception.status shouldBe StatusCodes.BadGateway
          exception.entity.isFailure shouldBe true
          exception.entity.failed.get shouldBe a[TimeoutException]
        case response => fail(s"expected HttpStatusException, got $response")
      }

      config.client.shutdown().futureValue
    }

    "throw HttpStatusException when pushing an invalid query" in {
      implicit val config: DruidConfig =
        DruidConfig(
          clientBackend = classOf[DruidHttpClient],
          hosts = Seq(QueryHost("localhost", 8082))
        )

      val client = config.client
      val responseFuture = client.doQuery(
        TimeSeriesQuery(
          aggregations = List(
            CountAggregation(name = "count")
          ),
          intervals = List("invalid interval")
        )
      )

      whenReady(responseFuture.failed) {
        case exception: HttpStatusException =>
          exception.status shouldBe StatusCodes.InternalServerError
          exception.entity.isFailure shouldBe false
          exception.entity.get.data.utf8String shouldBe
          """{
              |"error":"Unknown exception",
              |"errorMessage":"Cannot construct instance of `org.apache.druid.query.spec.LegacySegmentSpec`,
              | problem: Format requires a '/' separator: invalid interval\n
              | at [Source: (org.eclipse.jetty.server.HttpInputOverHTTP); line: 1, column: 186]
              | (through reference chain: org.apache.druid.query.timeseries.TimeseriesQuery[\"intervals\"])",
              |"errorClass":"com.fasterxml.jackson.databind.exc.ValueInstantiationException",
              |"host":null
              |}""".toOneLine
        case response => fail(s"expected HttpStatusException, got $response")
      }

      config.client.shutdown().futureValue
    }

  }
}
