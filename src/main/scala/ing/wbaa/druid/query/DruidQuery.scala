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

package ing.wbaa.druid.query

import ing.wbaa.druid.common.DruidClient.doQuery
import ing.wbaa.druid.common.json.{BoolSerializer, IntSerializer}
import ing.wbaa.druid.common.{DruidClient, DruidConfig}
import ing.wbaa.druid.definitions.{Aggregation, Dimension, Filter}
import org.joda.time.DateTime
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization
import org.json4s.{FieldSerializer, Formats, NoTypeHints}

object DruidQuery {
  val formats: Formats = Serialization.formats(NoTypeHints) +
    new IntSerializer +
    new BoolSerializer +
    FieldSerializer[DruidQuery[_]]() ++
    JodaTimeSerializers.all
}

sealed trait DruidQuery[T] {
  val queryType: String
  val dataSource: String = DruidConfig.datasource
  val granularity: String
  val aggregations: List[Aggregation]
  val intervals: List[String]

  def execute()(implicit x$1: Manifest[List[T]]): List[T]
}

case class GroupByQuery[T](queryType: String = "groupBy",
                           dimensions: List[String] = List(),
                           granularity: String = "all",
                           aggregations: List[Aggregation],
                           intervals: List[String]
                          ) extends DruidQuery[T] {
  override def execute()(implicit mf: Manifest[List[T]]): List[T] = {
    val queryResult = doQuery(this)
    val json = parse(queryResult)

    (json \ "event").extract[List[T]](formats = DruidQuery.formats, mf = mf)
  }
}

case class TimeSeriesResult[T](timestamp: DateTime, result: T)

case class TimeSeriesQuery[T](queryType: String = "timeseries",
                              granularity: String = "week",
                              descending: String = "true",
                              filter: Option[Filter] = None,
                              aggregations: List[Aggregation],
                              intervals: List[String]
                             ) extends DruidQuery[TimeSeriesResult[T]] {

  override def execute()(implicit mf: Manifest[List[TimeSeriesResult[T]]]): List[TimeSeriesResult[T]] = {

    val queryResult = DruidClient.doQuery(this)

    parse(queryResult).extract[List[TimeSeriesResult[T]]](formats = DruidQuery.formats, mf = mf)
  }
}


case class TopNQuery[T](queryType: String = "topN",
                        dimension: Dimension,
                        threshold: Int,
                        filter: Option[Filter] = None,
                        metric: String,
                        granularity: String = "all",
                        aggregations: List[Aggregation],
                        intervals: List[String]
                       ) extends DruidQuery[T] {

  override def execute()(implicit mf: Manifest[List[T]]): List[T] = {
    val queryResult = DruidClient.doQuery(this)
    val json = parse(queryResult)

    (json \\ "result").extract[List[T]](formats = DruidQuery.formats, mf = mf)
  }
}

