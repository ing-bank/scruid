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

package ing.wbaa.druid.dql

import ing.wbaa.druid.{ GroupByQuery, TimeSeriesQuery, TopNQuery }
import ing.wbaa.druid.definitions._
import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.concurrent._
import org.scalatest.time.{ Millis, Seconds, Span }
import ing.wbaa.druid.dql.DSL._
import io.circe.generic.auto._

class DQLSpec extends WordSpec with Matchers with ScalaFutures {

  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(5, Millis))
  private val totalNumberOfEntries = 39244

  case class TimeseriesCount(count: Int)
  case class GroupByIsAnonymous(isAnonymous: String, count: Int)
  case class TopCountry(count: Int, countryName: Option[String])
  case class AggregatedFilteredAnonymous(count: Int, isAnonymous: String, filteredCount: Int)
  case class PostAggregationAnonymous(count: Int, isAnonymous: String, halfCount: Double)

  "DQL TimeSeriesQuery" should {
    "successfully be interpreted by Druid" in {

      val query: TimeSeriesQuery = DQL
        .granularity(GranularityType.Hour)
        .interval("2011-06-01/2017-06-01")
        .agg(count as "count")
        .build()

      val request = query.execute()

      whenReady(request) { response =>
        response.list[TimeseriesCount].map(_.count).sum shouldBe totalNumberOfEntries
      }
    }

    "extract the data and return a map with the timestamps as keys" in {
      val query: TimeSeriesQuery = DQL
        .granularity(GranularityType.Hour)
        .interval("2011-06-01/2017-06-01")
        .agg(count as "count")
        .build()

      val request = query.execute()

      whenReady(request) { response =>
        response
          .series[TimeseriesCount]
          .flatMap { case (_, items) => items.map(_.count) }
          .sum shouldBe totalNumberOfEntries
      }
    }
  }

  "DQL GroupByQuery" should {
    "successfully be interpreted by Druid" in {
      val query: GroupByQuery = DQL
        .interval("2011-06-01/2017-06-01")
        .agg(count as "count")
        .groupBy('isAnonymous)
        .build()

      val request = query.execute()

      whenReady(request) { response =>
        response.list[GroupByIsAnonymous].map(_.count).sum shouldBe totalNumberOfEntries
      }
    }

    "successfully be interpreted by Druid when using lower granularity" in {
      val query = DQL
        .interval("2011-06-01/2017-06-01")
        .agg(count as "count")
        .groupBy('isAnonymous)
        .granularity(GranularityType.Hour)
        .build()

      val request = query.execute()

      whenReady(request) { response =>
        response.list[GroupByIsAnonymous].map(_.count).sum shouldBe totalNumberOfEntries
      }
    }
  }

  "DQL TopNQuery" should {
    "successfully be interpreted by Druid" in {
      val topNLimit = 5

      val query = DQL
        .agg(count as "count")
        .interval("2011-06-01/2017-06-01")
        .topN(dimension = 'countryName, metric = "count", threshold = topNLimit)
        .build()

      val request = query.execute()

      whenReady(request) { response =>
        val topN = response.list[TopCountry]

        topN.size shouldBe topNLimit

        topN.head shouldBe TopCountry(count = 35445, countryName = None)
        topN(1) shouldBe TopCountry(count = 528, countryName = Some("United States"))
        topN(2) shouldBe TopCountry(count = 256, countryName = Some("Italy"))
        topN(3) shouldBe TopCountry(count = 234, countryName = Some("United Kingdom"))
        topN(4) shouldBe TopCountry(count = 205, countryName = Some("France"))
      }
    }

    "also work with a filter" in {

      val query = DQL
        .agg(count as "count")
        .interval("2011-06-01/2017-06-01")
        .topN(dimension = 'countryName, metric = "count", threshold = 5)
        .where('countryName === "United States" or 'countryName === "Italy")
        .build()

      val request = query.execute()

      whenReady(request) { response =>
        val topN = response.list[TopCountry]
        topN.size shouldBe 2
        topN.head shouldBe TopCountry(count = 528, countryName = Some("United States"))
        topN(1) shouldBe TopCountry(count = 256, countryName = Some("Italy"))
      }
    }
  }

  "DQL also work with 'in' filtered aggregations" should {
    "successfully be interpreted by Druid" in {

      val query = DQL
        .agg('count.longSum as "count")
        .agg(
          'channel.inFiltered('count.longSum as "filteredCount", "#en.wikipedia", "#de.wikipedia")
        )
        .interval("2011-06-01/2017-06-01")
        .topN(dimension = 'isAnonymous, metric = "count", threshold = 5)
        .build()

      val request = query.execute()

      whenReady(request) { response =>
        val topN = response.list[AggregatedFilteredAnonymous]
        topN.size shouldBe 2
        topN.head shouldBe AggregatedFilteredAnonymous(count = 35445,
                                                       filteredCount = 12374,
                                                       isAnonymous = "false")
        topN(1) shouldBe AggregatedFilteredAnonymous(count = 3799,
                                                     filteredCount = 1698,
                                                     isAnonymous = "true")
      }
    }
  }

  "DQL also work with 'selector' filtered aggregations" should {
    "successfully be interpreted by Druid" in {

      val query = DQL
        .topN('isAnonymous, metric = "count", threshold = 5)
        .agg('count.longSum as "count")
        .agg(
          'channel.selectorFiltered(aggregator = 'count.longSum as "filteredCount",
                                    value = "#en.wikipedia") as "SelectorFilteredAgg"
        )
        .interval("2011-06-01/2017-06-01")
        .build()

      val request = query.execute()

      whenReady(request) { response =>
        val topN = response.list[AggregatedFilteredAnonymous]
        topN.size shouldBe 2
        topN.head shouldBe AggregatedFilteredAnonymous(count = 35445,
                                                       filteredCount = 9993,
                                                       isAnonymous = "false")
        topN(1) shouldBe AggregatedFilteredAnonymous(count = 3799,
                                                     filteredCount = 1556,
                                                     isAnonymous = "true")
      }
    }
  }

  "DQL also work with post 'arithmetic' post-aggregations" should {
    "successfully be interpreted by Druid" in {

      val query: TopNQuery = DQL
        .topN('isAnonymous, metric = "count", threshold = 5)
        .agg(count)
        .postAgg(('count / 2) as "halfCount")
        .interval("2011-06-01/2017-06-01")
        .build()

      val request = query.execute()

      whenReady(request) { response =>
        val topN = response.list[PostAggregationAnonymous]
        topN.size shouldBe 2
        topN.head shouldBe PostAggregationAnonymous(count = 35445,
                                                    halfCount = 17722.5,
                                                    isAnonymous = "false")
        topN(1) shouldBe PostAggregationAnonymous(count = 3799,
                                                  halfCount = 1899.5,
                                                  isAnonymous = "true")
      }
    }
  }

}
