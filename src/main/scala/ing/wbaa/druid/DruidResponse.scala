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

import java.time._

import io.circe._
import io.circe.java8.time._
import scala.collection.immutable.ListMap
import cats.syntax.either._

case class DruidResponse(results: List[DruidResult], queryType: QueryType) {
  private def decodeList[T](implicit decoder: Decoder[T]): List[T] = results.map { result =>
    result.as[T](decoder)
  }

  private def decode[T](result: Json)(implicit decoder: Decoder[T]): T =
    decoder.decodeJson(result) match {
      case Left(e)      => throw e
      case Right(value) => value
    }

  def list[T](implicit decoder: Decoder[T]): List[T] = queryType match {
    case QueryType.TopN => decodeList[List[T]].flatten
    case _              => decodeList[T]
  }

  def series[T](implicit decoder: Decoder[T]): ListMap[ZonedDateTime, List[T]] =
    results.foldLeft[ListMap[ZonedDateTime, List[T]]](ListMap.empty) {
      case (acc, DruidResult(timestamp, result)) =>
        acc ++ ListMap(
          timestamp -> (acc.getOrElse(timestamp, List.empty[T]) :+ decode(result)(decoder))
        )
    }
}

case class DruidResult(timestamp: ZonedDateTime, result: Json) {

  def as[T](implicit decoder: Decoder[T]): T = decoder.decodeJson(this.result) match {
    case Left(e)      => throw e
    case Right(value) => value
  }
}

object DruidResult extends JavaTimeDecoders {
  private def extractResultField(c: HCursor): ACursor = {
    val result = c.downField("result")
    val event  = c.downField("event")
    if (result.succeeded) result else event
  }

  implicit val decoder: Decoder[DruidResult] = new Decoder[DruidResult] {
    final def apply(c: HCursor): Decoder.Result[DruidResult] =
      for {
        timestamp <- c.downField("timestamp").as[ZonedDateTime]
        result    <- extractResultField(c).as[Json]
      } yield {

        DruidResult(timestamp, result)
      }
  }
}
