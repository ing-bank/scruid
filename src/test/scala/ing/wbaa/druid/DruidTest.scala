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

import ing.wbaa.druid.definitions.FilterOperators._
import ing.wbaa.druid.definitions._
import ing.wbaa.druid.query.{GroupByQuery, TimeSeriesQuery, TopNQuery}
import org.scalatest.{FunSuiteLike, Inside, OptionValues}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

case class TimeseriesCount(count: Int)

case class GroupByIsAnonymous(isAnonymous: Boolean, count: Int)

case class TopCountry(count: Int, countryName: String = null)

class DruidTest extends FunSuiteLike with Inside with OptionValues {

  private val queryTimeout = 5 seconds
  private val totalNumberOfEntries = 39244

  test("Do a Timeseries Query") {

    //  {
    //    "queryType": "timeseries",
    //    "granularity": "week",
    //    "descending": "true",
    //    "filter": null,
    //    "aggregations": [{
    //      "type": "count",
    //      "name": "count",
    //      "fieldName": "count"
    //    }],
    //    "dataSource": "wikiticker",
    //    "intervals": ["2016-06-01/2017-06-01"]
    //  }

    val resultFuture = TimeSeriesQuery[TimeseriesCount](
      aggregations = List(
        CountAggregation(name = "count")
      ),
      granularity = "hour",
      intervals = List("2011-06-01/2017-06-01")
    ).execute

    val result = Await.result(resultFuture, queryTimeout)

    //  [{
    //    "timestamp": "2015-09-07T00:00:00.000Z",
    //    "result": {
    //      "count": 39244
    //    }
    //  }]

    // The number of entries in the data set
    assert(result.map(_.result.count).sum == totalNumberOfEntries)
  }

  test("Do a GroupBy Query") {

    //  {
    //    "queryType": "groupBy",
    //    "dimensions": [],
    //    "granularity": "all",
    //    "aggregations": [{
    //      "type": "count",
    //      "name": "count",
    //      "fieldName": "count"
    //    }],
    //    "dimension": ["isAnonymous"],
    //    "intervals": ["2011-06-01/2017-06-01"],
    //    "dataSource": "wikiticker"
    //  }

    val resultFuture = GroupByQuery[GroupByIsAnonymous](
      aggregations = List(
        CountAggregation(name = "count")
      ),
      dimensions = List(Dimension(dimension = "isAnonymous")),
      intervals = List("2011-06-01/2017-06-01")
    ).execute

    val result = Await.result(resultFuture, queryTimeout)

    //  [{
    //    "version": "v1",
    //    "timestamp": "2011-06-01T00:00:00.000Z",
    //    "event": {
    //    "count": 35445,
    //    "isAnonymous": "false"
    //  }
    //  }, {
    //    "version": "v1",
    //    "timestamp": "2011-06-01T00:00:00.000Z",
    //    "event": {
    //    "count": 3799,
    //    "isAnonymous": "true"
    //  }
    //  }]

    // The number of entries in the data set
    assert(result.map(_.event.count).sum == totalNumberOfEntries)
  }


  test("Do a TopN Query") {

    //  {
    //    "queryType": "topN",
    //    "dimension": {
    //      "dimension": "countryName",
    //      "outputName": null,
    //      "outputType": null,
    //      "dimensionType": "default"
    //    },
    //    "threshold": 5,
    //    "filter": null,
    //    "metric": "count",
    //    "granularity": "all",
    //    "aggregations": [{
    //      "type": "count",
    //      "name": "count",
    //      "fieldName": "count/"
    //    }],
    //    "intervals": ["2011-06-01/2017-06-01"],
    //    "dataSource": "wikiticker"
    //  }

    val threshold = 5

    val resultFuture = TopNQuery[TopCountry](
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

    val result = Await.result(resultFuture, queryTimeout).head.result

    //  [{
    //    "timestamp": "2015-09-12T00:46:58.771Z",
    //    "result": [{
    //    "count": 35445,
    //    "countryName": null
    //  }, {
    //    "count": 528,
    //    "countryName": "United States"
    //  }, {
    //    "count": 256,
    //    "countryName": "Italy"
    //  }, {
    //    "count": 234,
    //    "countryName": "United Kingdom"
    //  }, {
    //    "count": 205,
    //    "countryName": "France"
    //  }]
    //  }]

    assert(result.size == threshold)
    assert(result.head.count === 35445)

    assert(result(1).count === 528)
    assert(result(1).countryName === "United States")

    assert(result(2).count === 256)
    assert(result(2).countryName === "Italy")

    assert(result(3).count === 234)
    assert(result(3).countryName === "United Kingdom")

    assert(result(4).count === 205)
    assert(result(4).countryName === "France")
  }


  test("Test filter") {

    //  {
    //  "queryType": "topN",
    //  "dimension": {
    //    "dimension": "countryName",
    //    "outputName": null,
    //    "outputType": null
    //  },
    //  "threshold": 5,
    //  "metric": "count",
    //  "granularity": "all",
    //  "aggregations": [{
    //  "type": "count",
    //  "name": "count",
    //  "fieldName": "count"
    //  }],
    //  "filter": {
    //    "type": "or",
    //    "fields": [{
    //    "type": "selector",
    //    "dimension": "countryName",
    //    "value": "United States"
    //  }, {
    //    "type": "selector",
    //    "dimension": "countryName",
    //    "value": "Italy"
    //  }]
    //  },
    //  "intervals": ["2011-06-01/2017-06-01"],
    //  "dataSource": "wikiticker"
    //  }

    val filterUnitedStates = FilterSelect(dimension = "countryName", value = "United States")
    val filterBoth = filterUnitedStates || FilterSelect(dimension = "countryName", value = "Italy") && FilterEmpty

    val resultFuture = TopNQuery[TopCountry](
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

    val result = Await.result(resultFuture, queryTimeout).head.result

    //  [{
    //    "count": 528,
    //    "countryName": "United States"
    //  }, {
    //    "count": 256,
    //    "countryName": "Italy"
    //  }]
    assert(result.size === 2)

    assert(result.head.count === 528)
    assert(result.head.countryName === "United States")

    assert(result(1).count === 256)
    assert(result(1).countryName === "Italy")


    // Test the filter composition

    val actualFilter = FilterSelect(dimension = "countryName", value = "United States")

    assert((FilterEmpty && actualFilter) == actualFilter)
    assert((actualFilter && FilterEmpty) == actualFilter)

    // Should give an Empty filter
    assert((FilterEmpty && FilterEmpty) == FilterEmpty)

  }

}
