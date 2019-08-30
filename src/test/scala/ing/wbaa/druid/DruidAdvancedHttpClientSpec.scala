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

import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ HttpProtocols, StatusCodes }
import ing.wbaa.druid.client.{ DruidAdvancedHttpClient, HttpStatusException }
import ing.wbaa.druid.definitions._
import org.scalatest._
import org.scalatest.concurrent._

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContextExecutor, Future }
import scala.language.postfixOps
import scala.util.Random

class DruidAdvancedHttpClientSpec extends WordSpec with Matchers with ScalaFutures with Inspectors {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(5 minutes, 100 millis)

  private val numberOfConcurrentQueriesLarge = 1024
  private val numberOfConcurrentQueriesSmall = 128

  case class TimeseriesCount(count: Int)
  case class GroupByIsAnonymous(isAnonymous: String, count: Int)
  case class TopCountry(count: Int, countryName: Option[String])

  "DruidAdvancedHttpClient" should {

    val timeSeries = TimeSeriesQuery(
      aggregations = List(
        CountAggregation(name = "count")
      ),
      granularity = GranularityType.Hour,
      intervals = List("2011-06-01/2017-06-01")
    )

    val groupBy = GroupByQuery(
      aggregations = List(
        CountAggregation(name = "count")
      ),
      dimensions = List(Dimension(dimension = "isAnonymous")),
      intervals = List("2011-06-01/2017-06-01")
    )

    val topFive = TopNQuery(
      dimension = Dimension(
        dimension = "countryName"
      ),
      threshold = 3,
      metric = "count",
      aggregations = List(
        CountAggregation(name = "count")
      ),
      intervals = List("2011-06-01/2017-06-01")
    )

    val queries = Array(timeSeries, groupBy, topFive)

    "indicate when Druid is healthy" in {
      val config = DruidConfig(clientBackend = classOf[DruidAdvancedHttpClient])
      val client = config.client

      whenReady(client.isHealthy()) { result =>
        result shouldBe true
      }

      config.client.shutdown().futureValue
    }

    "indicate when Druid is not healthy" in {
      val config = DruidConfig(clientBackend = classOf[DruidAdvancedHttpClient],
                               hosts = Seq(QueryHost("localhost", 8086)))
      val client = config.client

      whenReady(client.isHealthy()) { result =>
        result shouldBe false
      }

      config.client.shutdown().futureValue
    }

    "correctly report the health status of all Druid brokers" in {

      // A map with the expected QueryHost to health status.
      // When the status is true the corresponding Broker is healthy, otherwise is false
      val expectedHealthCheck = Map(
        QueryHost("localhost", 8082) -> true,
        QueryHost("localhost", 8083) -> true,
        QueryHost("localhost", 8084) -> true,
        QueryHost("localhost", 8085) -> true,
        // the following node always fails with Internal Server Error (HTTP code 500)
        QueryHost("localhost", 8086) -> false
      )

      val config = DruidConfig(clientBackend = classOf[DruidAdvancedHttpClient],
                               hosts = expectedHealthCheck.keys.toSeq)

      val client = config.client

      // since localhost:8086 is always failing the health status should be false
      whenReady(client.isHealthy()) { result =>
        result shouldBe false
      }

      whenReady(client.healthCheck) { outcome =>
        forAll(expectedHealthCheck) {
          case (broker, expectedResult) =>
            outcome(broker) shouldBe expectedResult
        }

      }

      config.client.shutdown().futureValue

    }

    "throw HttpStatusException for non-200 status codes" in {
      implicit val config =
        DruidConfig(clientBackend = classOf[DruidAdvancedHttpClient],
                    hosts = Seq(QueryHost("localhost", 8086))) // yields HTTP 500

      val responseFuture = queries.head.execute()

      whenReady(responseFuture.failed) {
        case exception: HttpStatusException =>
          exception.status shouldBe StatusCodes.InternalServerError
          exception.protocol shouldBe HttpProtocols.`HTTP/1.1`
          exception.headers should contain(new RawHeader("x-clusterfk-status-code", "500"))
          exception.entity.isKnownEmpty() shouldBe true

          exception.response.status shouldBe StatusCodes.InternalServerError
        case response => fail(s"expected HttpStatusException, got $response")
      }

      config.client.shutdown().futureValue
    }

    s"execute $numberOfConcurrentQueriesLarge concurrent queries " +
    s"over a single druid broker with random delays" in {

      implicit val config = DruidConfig(clientBackend = classOf[DruidAdvancedHttpClient],
                                        hosts = Seq(QueryHost("localhost", 8082)))

      implicit val ec = config.system.dispatcher

      val requests = Future.sequence {
        (1 to numberOfConcurrentQueriesLarge).map(idx => queries(idx % queries.length).execute)
      }

      whenReady(requests) { responses =>
        responses.size shouldBe numberOfConcurrentQueriesLarge
      }

      config.client.shutdown().futureValue
    }

    s"load-balance $numberOfConcurrentQueriesSmall concurrent queries " +
    s"across multiple Druid Brokers where one of them is unhealthy" in {

      implicit val config: DruidConfig = DruidConfig(
        hosts = Seq(
          // Healthy nodes
          QueryHost("localhost", 8082),
          QueryHost("localhost", 8083),
          QueryHost("localhost", 8084),
          // Node with random delays
          QueryHost("localhost", 8085),
          // Node with Internal Server Error (HTTP code 500)
          QueryHost("localhost", 8086)
        ),
        clientBackend = classOf[DruidAdvancedHttpClient],
        clientConfig = DruidAdvancedHttpClient.ConfigBuilder().withQueryRetries(10).build()
      )

      implicit val ec: ExecutionContextExecutor = config.system.dispatcher

      val rand = new Random(seed = 42)

      val randomQueries =
        (1 to numberOfConcurrentQueriesSmall).map(_ => queries(rand.nextInt(queries.length)))

      val requests = Future.sequence(randomQueries.map(_.execute))

      whenReady(requests) { responses =>
        responses.size shouldBe numberOfConcurrentQueriesSmall
      }

      config.client.shutdown().futureValue

    }

  }

}
