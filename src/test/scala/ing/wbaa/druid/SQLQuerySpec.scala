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

import java.time.{ LocalDateTime, ZonedDateTime }

import akka.stream.scaladsl.Sink
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{ Millis, Seconds, Span }
import ing.wbaa.druid.SQL._
import ing.wbaa.druid.client.CirceDecoders
import io.circe.generic.auto._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

//noinspection SqlNoDataSourceInspection
class SQLQuerySpec extends AnyWordSpec with Matchers with ScalaFutures with CirceDecoders {
  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(5, Millis))
  private val totalNumberOfEntries  = 39244
  private val usOnlyNumberOfEntries = 528

  implicit val config = DruidConfig()
  implicit val mat    = config.client.actorMaterializer

  case class Result(hourTime: ZonedDateTime, count: Int)

  "SQL query" should {

    val sqlQuery: SQLQuery = dsql"""
      |SELECT FLOOR(__time to HOUR) AS hourTime, count(*) AS "count"
      |FROM wikipedia
      |WHERE "__time" BETWEEN TIMESTAMP '2015-09-12 00:00:00' AND TIMESTAMP '2015-09-13 00:00:00'
      |GROUP BY 1
      |""".stripMargin

    "successfully be interpreted by Druid" in {
      val resultsF = sqlQuery.execute()
      whenReady(resultsF) { response =>
        response.list[Result].map(_.count).sum shouldBe totalNumberOfEntries
      }
    }

    "support streaming" in {
      val resultsF = sqlQuery.streamAs[Result]().runWith(Sink.seq)

      whenReady(resultsF) { results =>
        results.map(_.count).sum shouldBe totalNumberOfEntries
      }
    }
  }

  "SQL parameterized query" should {

    val fromDateTime   = LocalDateTime.of(2015, 9, 12, 0, 0, 0, 0)
    val untilDateTime  = fromDateTime.plusDays(1)
    val countryIsoCode = "US"

    val sqlQuery: SQLQuery =
      dsql"""
      |SELECT FLOOR(__time to HOUR) AS hourTime, count(*) AS "count"
      |FROM wikipedia
      |WHERE "__time" BETWEEN ${fromDateTime} AND ${untilDateTime} AND countryIsoCode = ${countryIsoCode}
      |GROUP BY 1
      |""".stripMargin

    "be expressed as a parameterized query with three parameters" in {
      sqlQuery.query.count(_ == '?') shouldBe 3
      sqlQuery.parameters.size shouldBe 3

      sqlQuery.parameters(0) shouldBe SQLQueryParameter(SQLQueryParameterType.Timestamp,
                                                        "2015-09-12 00:00:00")
      sqlQuery.parameters(1) shouldBe SQLQueryParameter(SQLQueryParameterType.Timestamp,
                                                        "2015-09-13 00:00:00")
      sqlQuery.parameters(2) shouldBe SQLQueryParameter(SQLQueryParameterType.Varchar, "US")
    }

    "successfully be interpreted by Druid" in {
      val resultsF = sqlQuery.execute()
      whenReady(resultsF) { response =>
        response.list[Result].map(_.count).sum shouldBe usOnlyNumberOfEntries
      }
    }

    "support streaming" in {
      val resultsF = sqlQuery.streamAs[Result]().runWith(Sink.seq)

      whenReady(resultsF) { results =>
        results.map(_.count).sum shouldBe usOnlyNumberOfEntries
      }

    }

  }
}
