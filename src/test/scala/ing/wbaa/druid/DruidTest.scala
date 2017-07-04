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
import ing.wbaa.druid.query.{GroupByQuery, TimeSeriesQuery, TopNQuery}
import org.scalatest.{FunSuiteLike, Inside, OptionValues}

case class TimeseriesCount(count: Int)

case class GroupByIsAnonymous(isAnonymous: String, count: Int)

case class TopCountry(count: Int, countryName: String = null)

class DruidTest extends FunSuiteLike with Inside with OptionValues {

  val totalNumberOfEntries = 39244


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

    val result = TimeSeriesQuery[TimeseriesCount](
      aggregations = List(
        Aggregation(
          kind = "count",
          name = "count",
          fieldName = "count"
        )
      ),
      granularity = "hour",
      intervals = List("2011-06-01/2017-06-01")
    ).execute

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

    val result = GroupByQuery[GroupByIsAnonymous](
      aggregations = List(
        Aggregation(
          kind = "count",
          name = "count",
          fieldName = "count"
        )
      ),
      dimensions = List("isAnonymous"),
      intervals = List("2011-06-01/2017-06-01")
    ).execute()

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
    assert(result.map(_.count).sum == totalNumberOfEntries)
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

val result = TopNQuery[TopCountry](
  dimension = Dimension(
    dimension = "countryName"
  ),
  threshold = 5,
  metric = "count",
  aggregations = List(
    Aggregation(
      kind = "count",
      name = "count",
      fieldName = "count"
    )
  ),
  intervals = List("2011-06-01/2017-06-01")
).execute

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

}