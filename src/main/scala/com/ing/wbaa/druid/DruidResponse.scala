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

import java.time._

import scala.collection.immutable.ListMap

// DO NOT REMOVE: required for scala 2.11
import cats.syntax.either._
import com.ing.wbaa.druid.client.CirceDecoders
import io.circe._
import io.circe.Decoder.Result
import io.circe.generic.semiauto.deriveDecoder

sealed trait DruidResponse extends CirceDecoders {

  def list[T](implicit decoder: Decoder[T]): List[T]

}

sealed trait DruidResponseSeries extends DruidResponse {
  def series[T](implicit decoder: Decoder[T]): ListMap[ZonedDateTime, List[T]]
}

case class DruidResponseTimeseriesImpl(results: List[DruidResult], queryType: QueryType)
    extends DruidResponse
    with DruidResponseSeries {

  private def decodeList[T](implicit decoder: Decoder[T]): List[T] = results.map { result =>
    result.as[T](decoder)
  }

  private def decode[T](result: Json)(implicit decoder: Decoder[T]): T =
    decoder.decodeJson(result).toTry.get

  override def list[T](implicit decoder: Decoder[T]): List[T] = queryType match {
    case QueryType.TopN => decodeList[List[T]].flatten
    case _              => decodeList[T]
  }

  override def series[T](implicit decoder: Decoder[T]): ListMap[ZonedDateTime, List[T]] =
    results.foldLeft[ListMap[ZonedDateTime, List[T]]](ListMap.empty) {
      case (acc, DruidResult(timestampOpt, result)) =>
        val elements = List(decode(result)(decoder))

        timestampOpt
          .map { timestamp =>
            acc ++ ListMap(
              timestamp -> (acc.getOrElse(timestamp, List.empty[T]) ++ elements)
            )
          }
          .getOrElse(acc)

    }
}

sealed trait BaseResult {
  def as[T](implicit decoder: Decoder[T]): T
  val timestamp: Option[ZonedDateTime]
}

case class DruidResult(timestamp: Option[ZonedDateTime], result: Json) extends BaseResult {

  override def as[T](implicit decoder: Decoder[T]): T = decoder.decodeJson(this.result).toTry.get
}

object DruidResult extends CirceDecoders {
  private def extractResultField(c: HCursor): ACursor = {
    val result = c.downField("result")

    if (result.succeeded) result else c.downField("event")
  }

  implicit val decoder: Decoder[DruidResult] = new Decoder[DruidResult] {
    final def apply(c: HCursor): Decoder.Result[DruidResult] =
      for {
        timestamp <- c.downField("timestamp").as[ZonedDateTime].map(Option(_))
        result    <- extractResultField(c).as[Json]
      } yield DruidResult(timestamp, result)
  }
}

case class DruidScanResponse(results: List[DruidScanResults])
    extends DruidResponse
    with DruidResponseSeries {

  override def list[T](implicit decoder: Decoder[T]): List[T] = results.flatMap(_.as[T])

  override def series[T](implicit decoder: Decoder[T]): ListMap[ZonedDateTime, List[T]] =
    results.foldLeft(ListMap.empty[ZonedDateTime, List[T]]) { (acc, scanResult) =>
      scanResult.events
        .map(event => event.timestamp -> event.as[T])
        .groupBy { case (timestamp, _) => timestamp }
        .foldLeft(acc) { (internalAcc, record) =>
          val (timestampOpt, entries) = record
          val elements                = entries.map { case (_, event) => event }
          timestampOpt
            .map { timestamp =>
              internalAcc ++ ListMap(
                timestamp -> (internalAcc.getOrElse(timestamp, List.empty[T]) ++ elements)
              )
            }
            .getOrElse(internalAcc)

        }
    }
}

case class DruidScanResults(
    segmentId: String,
    columns: Seq[String],
    events: List[DruidScanResult]
) {

  def as[T](implicit decoder: Decoder[T]): List[T] = events.map(_.as[T])

}

object DruidScanResults {
  implicit val decoder: Decoder[DruidScanResults] = deriveDecoder[DruidScanResults]
}

case class DruidScanResult(result: Json) extends BaseResult with CirceDecoders {

  override val timestamp: Option[ZonedDateTime] = {
    val timestampField = result.hcursor.downField("timestamp")

    if (timestampField.succeeded) {
      timestampField
        .as[ZonedDateTime]
        .map(Option(_))
        .getOrElse(throw new IllegalStateException("Failed to parse JSON field 'timestamp'"))
    } else {
      val milliseconds = result.hcursor
        .downField("__time")
        .as[Long]
        .getOrElse(throw new IllegalStateException(s"Failed to parse JSON field '__time'"))

      Option(ZonedDateTime.ofInstant(Instant.ofEpochMilli(milliseconds), ZoneId.of("UTC")))
    }
  }

  def as[T](implicit decoder: Decoder[T]): T = result.as[T].toTry.get
}

object DruidScanResult {

  implicit val decoder: Decoder[DruidScanResult] = new Decoder[DruidScanResult] {
    override def apply(c: HCursor): Result[DruidScanResult] = Right(DruidScanResult(c.value))
  }

}

case class DruidResponseSearch(response: DruidResponseSeries) {
  def list: List[DruidSearchResult] = response.list[List[DruidSearchResult]].flatten
  def series: ListMap[ZonedDateTime, List[DruidSearchResult]] =
    response.series[List[DruidSearchResult]].map {
      case (zonedDateTime, entries) => zonedDateTime -> entries.flatten
    }
}

case class DruidSearchResult(dimension: String, value: String, count: Long)

object DruidSearchResult {
  implicit val decoder: Decoder[DruidSearchResult] = deriveDecoder[DruidSearchResult]
}

case class DruidSQLResults(results: List[Json]) extends DruidResponse {

  override def list[T](implicit decoder: Decoder[T]): List[T] = results.map(_.as[T].toTry.get)

}

case class DruidSQLResult(result: Json) extends BaseResult {

  def as[T](implicit decoder: Decoder[T]): T =
    result.as[T].toTry.get

  override val timestamp: Option[ZonedDateTime] = None
}
