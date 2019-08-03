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

import akka.http.scaladsl.model.StatusCode
import ing.wbaa.druid.client.{ DruidHttpClient, HttpStatusException }
import ing.wbaa.druid.definitions.{ CountAggregation, GranularityType }
import org.scalatest._
import org.scalatest.concurrent._

import scala.concurrent.duration._
import scala.language.postfixOps

class DruidClientSpec extends WordSpec with Matchers with ScalaFutures {

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
                               hosts = Seq(QueryHost("localhost", 8087)))
      val client = config.client

      whenReady(client.isHealthy()) { result =>
        result shouldBe false
      }
    }

    "fail to load when having multiple query nodes" in {
      val config = DruidConfig(clientBackend = classOf[DruidHttpClient],
                               hosts =
                                 Seq(QueryHost("localhost", 8082), QueryHost("localhost", 8083)))

      assertThrows[IllegalStateException] {
        config.client
      }
    }

    "throw HttpStatusException for non-200 status codes" in {
      val config = DruidConfig(clientBackend = classOf[DruidHttpClient],
                               hosts = Seq(QueryHost("localhost", 8086))) // yields HTTP 500
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
          exception.status shouldBe StatusCode.int2StatusCode(500)
          exception.entity match {
            case Some(entity) => entity.isKnownEmpty() shouldBe true
            case _            => fail("expected empty entity, got empty option")
          }
        case response => fail(s"expected HttpStatusException, got $response")
      }

      client.shutdown().futureValue
    }

  }
}
