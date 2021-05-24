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

import ing.wbaa.druid.definitions._
import ing.wbaa.druid.util._
import io.circe.generic.auto._
import io.circe.syntax._
import org.scalatest.concurrent._
import org.scalatest.matchers.should.Matchers
import org.scalatest.time._
import org.scalatest.wordspec.AnyWordSpec

class QueryContextSpec extends AnyWordSpec with Matchers with ScalaFutures {

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(5, Millis))
  private val totalNumberOfEntries = 39244
  implicit val config              = DruidConfig()
  implicit val mat                 = config.client.actorMaterializer

  case class TimeseriesCount(count: Int)

  "TimeSeriesQuery with context" should {

    "successfully be interpreted by Druid" in {

      val query = TimeSeriesQuery(
        aggregations = List(
          CountAggregation(name = "count")
        ),
        granularity = GranularityType.Hour,
        intervals = List("2011-06-01/2017-06-01"),
        context = Map(
          QueryContext.QueryId          -> "some_custom_id",
          QueryContext.Priority         -> 1,
          QueryContext.UseCache         -> false,
          QueryContext.SkipEmptyBuckets -> true
        )
      )

      val requestJson = query.asJson.noSpaces

      requestJson shouldBe
      """{
          |"aggregations":[{"name":"count","type":"count"}],
          |"intervals":["2011-06-01/2017-06-01"],
          |"filter":null,
          |"granularity":"hour",
          |"descending":"true",
          |"postAggregations":[],
          |"context":{"queryId":"some_custom_id","priority":1,"useCache":false,"skipEmptyBuckets":true}
          |}""".toOneLine

      val resultF = query.execute()

      whenReady(resultF) { response =>
        response.list[TimeseriesCount].map(_.count).sum shouldBe totalNumberOfEntries
      }
    }

  }

}
