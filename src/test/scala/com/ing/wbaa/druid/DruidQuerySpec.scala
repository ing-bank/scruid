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

package com.ing.wbaa.druid

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

import akka.stream.scaladsl.Sink
import com.ing.wbaa.druid.client.DruidHttpClient
import com.ing.wbaa.druid.definitions._
import com.ing.wbaa.druid.definitions.ArithmeticFunctions._
import com.ing.wbaa.druid.definitions.FilterOperators._
import io.circe.generic.auto._
import org.scalatest.concurrent._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DruidQuerySpec extends AnyWordSpec with Matchers with ScalaFutures {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(1 minute, 100 millis)

  private val totalNumberOfEntries = 39244
  implicit val config              = DruidConfig(clientBackend = classOf[DruidHttpClient])
  implicit val mat                 = config.client.actorMaterializer

  case class TimeseriesCount(count: Int)
  case class GroupByIsAnonymous(isAnonymous: String, count: Int)
  case class TopCountry(count: Int, countryName: Option[String])
  case class AggregatedFilteredAnonymous(count: Int, isAnonymous: String, filteredCount: Int)
  case class PostAggregationAnonymous(count: Int, isAnonymous: String, halfCount: Double)

  "TimeSeriesQuery" should {
    "successfully be interpreted by Druid" in {
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

    "extract the data and return a map with the timestamps as keys" in {
      val request = TimeSeriesQuery(
        aggregations = List(
          CountAggregation(name = "count")
        ),
        granularity = GranularityType.Hour,
        intervals = List("2011-06-01/2017-06-01")
      ).execute

      whenReady(request) { response =>
        response
          .series[TimeseriesCount]
          .flatMap { case (_, items) => items.map(_.count) }
          .sum shouldBe totalNumberOfEntries
      }
    }
  }

  "TimeSeriesQuery (streaming)" should {
    val query = TimeSeriesQuery(
      aggregations = List(
        CountAggregation(name = "count")
      ),
      granularity = GranularityType.Hour,
      intervals = List("2011-06-01/2017-06-01")
    )

    "successfully be interpreted by Druid" in {
      val request: Future[Int] = query
        .streamAs[TimeseriesCount]
        .map(_.count)
        .runWith(Sink.fold(0)(_ + _))

      whenReady(request) { response =>
        response shouldBe totalNumberOfEntries
      }
    }

    "extract the data and return a map with the timestamps as keys" in {
      val request = query
        .streamSeriesAs[TimeseriesCount]
        .map { case (_, entry) => entry.count }
        .runWith(Sink.fold(0)(_ + _))

      whenReady(request) { response =>
        response shouldBe totalNumberOfEntries
      }
    }
  }

  "GroupByQuery" should {
    "successfully be interpreted by Druid" in {
      val request = GroupByQuery(
        aggregations = List(
          CountAggregation(name = "count")
        ),
        dimensions = List(Dimension(dimension = "isAnonymous")),
        intervals = List("2011-06-01/2017-06-01")
      ).execute

      whenReady(request) { response =>
        response.list[GroupByIsAnonymous].map(_.count).sum shouldBe totalNumberOfEntries
      }
    }

    "successfully be interpreted by Druid when using lower granularity" in {
      val request = GroupByQuery(
        aggregations = List(
          CountAggregation(name = "count")
        ),
        dimensions = List(Dimension(dimension = "isAnonymous")),
        intervals = List("2011-06-01/2017-06-01"),
        granularity = GranularityType.Hour
      ).execute

      whenReady(request) { response =>
        response.list[GroupByIsAnonymous].map(_.count).sum shouldBe totalNumberOfEntries
      }
    }
  }

  "GroupByQuery (streaming)" should {

    "successfully be interpreted by Druid" in {
      val query = GroupByQuery(
        aggregations = List(
          CountAggregation(name = "count")
        ),
        dimensions = List(Dimension(dimension = "isAnonymous")),
        intervals = List("2011-06-01/2017-06-01")
      )

      val request = query
        .streamAs[GroupByIsAnonymous]
        .map(_.count)
        .runWith(Sink.fold(0)(_ + _))

      whenReady(request) { response =>
        response shouldBe totalNumberOfEntries
      }
    }

    "successfully be interpreted by Druid when using lower granularity" in {
      val query = GroupByQuery(
        aggregations = List(
          CountAggregation(name = "count")
        ),
        dimensions = List(Dimension(dimension = "isAnonymous")),
        intervals = List("2011-06-01/2017-06-01"),
        granularity = GranularityType.Hour
      )

      val request = query
        .streamAs[GroupByIsAnonymous]
        .map(_.count)
        .runWith(Sink.fold(0)(_ + _))

      whenReady(request) { response =>
        response shouldBe totalNumberOfEntries
      }
    }

    "extract the data and return a map with the timestamps as keys, lower granularity" in {
      val query = GroupByQuery(
        aggregations = List(
          CountAggregation(name = "count")
        ),
        dimensions = List(Dimension(dimension = "isAnonymous")),
        intervals = List("2011-06-01/2017-06-01"),
        granularity = GranularityType.Hour
      )

      val request = query
        .streamSeriesAs[GroupByIsAnonymous]
        .map { case (_, entry) => entry.count }
        .runWith(Sink.fold(0)(_ + _))

      whenReady(request) { response =>
        response shouldBe totalNumberOfEntries
      }
    }

    "large query" in {
      val totalRecords = 37903
      val dimNames =
        List(
          "channel",
          "cityName",
          "countryIsoCode",
          "countryName",
          "isAnonymous",
          "isMinor",
          "isNew",
          "isRobot",
          "isUnpatrolled",
          "metroCode",
          "namespace",
          "page",
          "regionIsoCode",
          "regionName",
          "user"
        )

      case class GroupByRecord(channel: Option[String],
                               cityName: Option[String],
                               countryIsoCode: Option[String],
                               countryName: Option[String],
                               isMinor: Option[String],
                               isNew: Option[String],
                               isRobot: Option[String],
                               isUnpatrolled: Option[String],
                               metroCode: Option[String],
                               namespace: Option[String],
                               page: Option[String],
                               regionIsoCode: Option[String],
                               regionName: Option[String],
                               user: Option[String],
                               count: Int)

      val query = GroupByQuery(
        aggregations = List(
          CountAggregation(name = "count")
        ),
        dimensions = dimNames.map(name => Dimension(name)),
        intervals = List("2011-06-01/2017-06-01"),
        granularity = GranularityType.Hour
      )

      val request = query
        .streamAs[GroupByRecord]
        .map(_ => 1)
        .runWith(Sink.fold(0)(_ + _))

      whenReady(request) { response =>
        response shouldBe totalRecords
      }
    }
  }

  "TopNQuery" should {
    "successfully be interpreted by Druid" in {
      val threshold = 5

      val request = TopNQuery(
        dimension = Dimension(
          dimension = "countryName"
        ),
        threshold = threshold,
        metric = "count",
        aggregations = List(
          CountAggregation(name = "count")
        ),
        intervals = List("2011-06-01/2017-06-01")
      ).execute

      whenReady(request) { response =>
        val topN = response.list[TopCountry]

        topN.size shouldBe threshold

        topN.head shouldBe TopCountry(count = 35445, countryName = None)
        topN(1) shouldBe TopCountry(count = 528, countryName = Some("United States"))
        topN(2) shouldBe TopCountry(count = 256, countryName = Some("Italy"))
        topN(3) shouldBe TopCountry(count = 234, countryName = Some("United Kingdom"))
        topN(4) shouldBe TopCountry(count = 205, countryName = Some("France"))
      }
    }

    "also work with a filter" in {
      val filterUnitedStates = SelectFilter(dimension = "countryName", value = "United States")
      val filterBoth = filterUnitedStates || SelectFilter(dimension = "countryName",
                                                          value = "Italy")

      val request = TopNQuery(
        dimension = Dimension(
          dimension = "countryName"
        ),
        filter = Some(filterBoth),
        threshold = 5,
        metric = "count",
        aggregations = List(
          CountAggregation(name = "count")
        ),
        intervals = List("2011-06-01/2017-06-01")
      ).execute

      whenReady(request) { response =>
        val topN = response.list[TopCountry]
        topN.size shouldBe 2
        topN.head shouldBe TopCountry(count = 528, countryName = Some("United States"))
        topN(1) shouldBe TopCountry(count = 256, countryName = Some("Italy"))
      }
    }
  }

  "TopNQuery (streaming)" should {
    "successfully be interpreted by Druid" in {
      val threshold = 5

      val query = TopNQuery(
        dimension = Dimension(
          dimension = "countryName"
        ),
        threshold = threshold,
        metric = "count",
        aggregations = List(
          CountAggregation(name = "count")
        ),
        intervals = List("2011-06-01/2017-06-01")
      )

      val request = query.streamAs[TopCountry].runWith(Sink.seq[TopCountry])

      whenReady(request) { topN =>
        topN.size shouldBe threshold

        topN.head shouldBe TopCountry(count = 35445, countryName = None)
        topN(1) shouldBe TopCountry(count = 528, countryName = Some("United States"))
        topN(2) shouldBe TopCountry(count = 256, countryName = Some("Italy"))
        topN(3) shouldBe TopCountry(count = 234, countryName = Some("United Kingdom"))
        topN(4) shouldBe TopCountry(count = 205, countryName = Some("France"))
      }
    }
  }

  "also work with 'in' filtered aggregations" should {
    "successfully be interpreted by Druid" in {

      val request = TopNQuery(
        dimension = Dimension(
          dimension = "isAnonymous"
        ),
        threshold = 5,
        metric = "count",
        aggregations = List(
          CountAggregation("count"),
          InFilteredAggregation(
            name = "filteredCount",
            InFilter(dimension = "channel", values = List("#en.wikipedia", "#de.wikipedia")),
            aggregator = CountAggregation("filteredCount")
          )
        ),
        intervals = List("2011-06-01/2017-06-01")
      ).execute

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

  "also work with 'selector' filtered aggregations" should {
    "successfully be interpreted by Druid" in {

      val request = TopNQuery(
        dimension = Dimension(
          dimension = "isAnonymous"
        ),
        threshold = 5,
        metric = "count",
        aggregations = List(
          CountAggregation("count"),
          SelectorFilteredAggregation(
            name = "filteredCount",
            SelectFilter(dimension = "channel", value = "#en.wikipedia"),
            aggregator = CountAggregation("filteredCount")
          )
        ),
        intervals = List("2011-06-01/2017-06-01")
      ).execute

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

  "also work with post 'arithmetic' postaggregations" should {
    "successfully be interpreted by Druid" in {
      val request = TopNQuery(
        dimension = Dimension(
          dimension = "isAnonymous"
        ),
        threshold = 5,
        metric = "count",
        aggregations = List(
          CountAggregation("count")
        ),
        postAggregations = List(
          (FieldAccessPostAggregation("count") / ConstantPostAggregation(2))
            .withName("halfCount")
        ),
        intervals = List("2011-06-01/2017-06-01")
      ).execute

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

  "DruidResponse" should {
    "successfully convert the series" in {
      val request = GroupByQuery(
        aggregations = List(
          CountAggregation(name = "count")
        ),
        dimensions = List(Dimension(dimension = "isAnonymous")),
        intervals = List("2011-06-01/2017-06-01"),
        granularity = GranularityType.All
      ).execute

      whenReady(request) { response =>
        response
          .series[GroupByIsAnonymous]
          .flatMap {
            case (timestamp @ _, values) =>
              values.map(_.count)
          }
          .sum shouldBe totalNumberOfEntries
      }
    }

    "successfully convert the series when using lower granularity" in {
      val request = GroupByQuery(
        aggregations = List(
          CountAggregation(name = "count")
        ),
        dimensions = List(Dimension(dimension = "isAnonymous")),
        intervals = List("2011-06-01/2017-06-01"),
        granularity = GranularityType.Hour
      ).execute

      whenReady(request) { response =>
        response
          .series[GroupByIsAnonymous]
          .flatMap {
            case (timestamp @ _, values) =>
              values.map(_.count)
          }
          .sum shouldBe totalNumberOfEntries
      }
    }
  }
}
