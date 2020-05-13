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

package ing.wbaa.druid.sql

import java.sql.Timestamp
import java.time.{ Instant, LocalDate, LocalDateTime }

import ing.wbaa.druid.{ DruidConfig, SQLQueryParameter, SQLQueryParameterType }
import scala.language.implicitConversions

trait ParameterConversions {
  implicit def char2Param(v: Char): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Char, v.toString)

  implicit def string2Param(v: String): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Varchar, v)

  implicit def byte2Param(v: Byte): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Tinyint, v.toString)

  implicit def short2Param(v: Short): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Smallint, v.toString)

  implicit def int2Param(v: Int): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Integer, v.toString)

  implicit def long2Param(v: Long): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Bigint, v.toString)

  implicit def float2Param(v: Float): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Float, v.toString)

  implicit def double2Param(v: Double): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Double, v.toString)

  implicit def boolean2Param(v: Boolean): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Boolean, v.toString)

  implicit def localDate2Param(v: LocalDate)(implicit config: DruidConfig =
                                               DruidConfig.DefaultConfig): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Date, v.format(config.FormatterDate))

  implicit def localDateTime2Param(
      v: LocalDateTime
  )(implicit config: DruidConfig = DruidConfig.DefaultConfig): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Timestamp, v.format(config.FormatterDateTime))

  implicit def timestamp2Param(v: Timestamp)(implicit config: DruidConfig =
                                               DruidConfig.DefaultConfig): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Timestamp, config.FormatterDateTime.format(v.toInstant))

  implicit def instant2Param(
      v: Instant
  )(implicit config: DruidConfig = DruidConfig.DefaultConfig): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Timestamp, config.FormatterDateTime.format(v))
}
