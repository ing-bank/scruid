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
package definitions

import ca.mrvisser.sealerate
import io.circe.Encoder
import io.circe.generic.auto._
import io.circe.syntax._

sealed trait ExtractionFnType extends Enum with CamelCaseEnumStringEncoder
object ExtractionFnType extends EnumCodec[ExtractionFnType] {
  case object Regex            extends ExtractionFnType
  case object Partial          extends ExtractionFnType
  case object SearchQuery      extends ExtractionFnType // not implemented yet
  case object Strlen           extends ExtractionFnType
  case object Substring        extends ExtractionFnType
  case object TimeFormat       extends ExtractionFnType
  case object Time             extends ExtractionFnType
  case object Javascript       extends ExtractionFnType
  case object Lookup           extends ExtractionFnType // not implemented yet
  case object RegisteredLookup extends ExtractionFnType // not implemented yet
  case object Cascade          extends ExtractionFnType
  case object StringFormat     extends ExtractionFnType
  case object Upper            extends ExtractionFnType
  case object Lower            extends ExtractionFnType
  case object Bucket           extends ExtractionFnType

  override val values: Set[ExtractionFnType] = sealerate.values[ExtractionFnType]
}

sealed trait ExtractionFn {
  val `type`: ExtractionFnType
}

object ExtractionFn {
  implicit val encoder: Encoder[ExtractionFn] = new Encoder[ExtractionFn] {
    override def apply(extFn: ExtractionFn) =
      (extFn match {
        case x: RegexExtractionFn        => x.asJsonObject
        case x: PartialExtractionFn      => x.asJsonObject
        case x: SubstringExtractionFn    => x.asJsonObject
        case StrlenExtractionFn          => StrlenExtractionFn.asJsonObject
        case x: TimeFormatExtractionFn   => x.asJsonObject
        case x: TimeParsingExtractionFn  => x.asJsonObject
        case x: JavascriptExtractionFn   => x.asJsonObject
        case x: CascadeExtractionFn      => x.asJsonObject
        case x: StringFormatExtractionFn => x.asJsonObject
        case x: UpperExtractionFn        => x.asJsonObject
        case x: LowerExtractionFn        => x.asJsonObject
        case x: BucketExtractionFn       => x.asJsonObject
      }).add("type", extFn.`type`.asJson).asJson
  }
}

case class RegexExtractionFn(
    expr: String,
    index: Option[Int] = Some(1),
    replaceMissingValue: Option[Boolean] = Some(false),
    replaceMissingValueWith: Option[String] = None
) extends ExtractionFn {
  override val `type` = ExtractionFnType.Regex
}

case class PartialExtractionFn(
    expr: String
) extends ExtractionFn {
  override val `type` = ExtractionFnType.Partial
}

case class SubstringExtractionFn(
    index: Int,
    length: Option[Int] = None
) extends ExtractionFn {
  override val `type` = ExtractionFnType.Substring
}

case object StrlenExtractionFn extends ExtractionFn {
  override val `type` = ExtractionFnType.Strlen
}

case class TimeFormatExtractionFn(
    format: Option[String] = None,
    timeZone: Option[String] = Some("utc"),
    locale: Option[String] = None,
    granularity: Option[Granularity] = Some(GranularityType.None),
    asMillis: Option[Boolean] = Some(false)
) extends ExtractionFn {
  override val `type` = ExtractionFnType.TimeFormat
}

case class TimeParsingExtractionFn(
    timeFormat: String,
    resultFormat: String
) extends ExtractionFn {
  override val `type` = ExtractionFnType.Time
}

case class JavascriptExtractionFn(
    function: String,
    injective: Option[Boolean] = Some(false)
) extends ExtractionFn {
  override val `type` = ExtractionFnType.Javascript
}

case class CascadeExtractionFn(
    extractionFns: Iterable[ExtractionFn]
) extends ExtractionFn {
  override val `type` = ExtractionFnType.Cascade
}

sealed trait NullHandling extends Enum with CamelCaseEnumStringEncoder
object NullHandling extends EnumCodec[NullHandling] {
  case object NullString  extends NullHandling
  case object EmptyString extends NullHandling
  case object ReturnNull  extends NullHandling

  override val values: Set[NullHandling] = sealerate.values[NullHandling]
}

case class StringFormatExtractionFn(
    format: String,
    nullHandling: Option[NullHandling] = Some(NullHandling.NullString)
) extends ExtractionFn {
  override val `type` = ExtractionFnType.StringFormat
}

case class UpperExtractionFn(locale: Option[String] = None) extends ExtractionFn {
  val `type` = ExtractionFnType.Upper
}

case class LowerExtractionFn(locale: Option[String] = None) extends ExtractionFn {
  val `type` = ExtractionFnType.Lower
}

case class BucketExtractionFn(
    size: Option[Int] = Some(1),
    offset: Option[Int] = Some(0)
) extends ExtractionFn {
  val `type` = ExtractionFnType.Bucket
}
