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

import ing.wbaa.druid.common.json.{BoolSerializer, IntSerializer}
import ing.wbaa.druid.common.{DruidClient, DruidConfig}
import ing.wbaa.druid.definitions.{Aggregation, Dimension, Filter}
import org.joda.time.DateTime
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization
import org.json4s.{FieldSerializer, Formats, NoTypeHints}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}


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
  val filter: Option[Filter]
  val intervals: List[String]

  private val logger = LoggerFactory.getLogger(getClass)

  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  def execute()(implicit mf: Manifest[List[T]]): Future[List[T]] =
    DruidClient.doQuery(this).map(parseAndExtractJson)

  protected def parseAndExtractJson(json: String)(implicit mf: Manifest[List[T]]): List[T] = {
    logger.debug(s"Druid response: $json")

    parse(json).extract[List[T]](formats = DruidQuery.formats, mf = mf)
  }
}

case class GroupByQueryResult[T](timestamp: DateTime, event: T)

case class GroupByQuery[T](queryType: String = "groupBy",
                           dimensions: List[Dimension] = List(),
                           granularity: String = "all",
                           aggregations: List[Aggregation],
                           filter: Option[Filter] = None,
                           intervals: List[String]
                          ) extends DruidQuery[GroupByQueryResult[T]]

case class TimeSeriesResult[T](timestamp: DateTime, result: T)

case class TimeSeriesQuery[T](queryType: String = "timeseries",
                              granularity: String = "week",
                              descending: String = "true",
                              aggregations: List[Aggregation],
                              filter: Option[Filter] = None,
                              intervals: List[String]
                             ) extends DruidQuery[TimeSeriesResult[T]]

case class TopNResult[T](timestamp: DateTime, result: List[T])

case class TopNQuery[T](queryType: String = "topN",
                        dimension: Dimension,
                        threshold: Int,
                        metric: String,
                        granularity: String = "all",
                        aggregations: List[Aggregation],
                        filter: Option[Filter] = None,
                        intervals: List[String]
                       ) extends DruidQuery[TopNResult[T]]
