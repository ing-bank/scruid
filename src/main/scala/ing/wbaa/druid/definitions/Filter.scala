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
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

sealed trait FilterType extends Enum with CamelCaseEnumStringEncoder

object FilterType extends EnumCodec[FilterType] {
  case object And              extends FilterType
  case object Or               extends FilterType
  case object Selector         extends FilterType
  case object ColumnComparison extends FilterType
  case object Regex            extends FilterType
  case object Not              extends FilterType
  case object Javascript       extends FilterType
  val values: Set[FilterType] = sealerate.values[FilterType]
}

sealed trait Filter {
  val `type`: FilterType
}

object Filter {
  implicit val encoder: Encoder[Filter] = new Encoder[Filter] {
    final def apply(filter: Filter): Json =
      (filter match {
        case x: SelectFilter     => x.asJsonObject
        case x: NotFilter        => x.asJsonObject
        case x: RegexFilter      => x.asJsonObject
        case x: AndFilter        => x.asJsonObject
        case x: OrFilter         => x.asJsonObject
        case x: JavascriptFilter => x.asJsonObject
      }).add("type", filter.`type`.asJson).asJson
  }
}

object FilterOperators {
  def &&(filterA: Filter, filterB: Filter): AndFilter = (filterA, filterB) match {
    case (AndFilter(fields), AndFilter(otherFields)) => AndFilter(fields = fields ++ otherFields)
    case (AndFilter(fields), other)                  => AndFilter(fields = fields :+ other)
    case (other, AndFilter(fields))                  => AndFilter(fields = fields :+ other)
    case _                                           => AndFilter(fields = List(filterA, filterB))
  }
  def ||(filterA: Filter, filterB: Filter): OrFilter = (filterA, filterB) match {
    case (OrFilter(fields), OrFilter(otherFields)) => OrFilter(fields = fields ++ otherFields)
    case (OrFilter(fields), other)                 => OrFilter(fields = fields :+ other)
    case (other, OrFilter(fields))                 => OrFilter(fields = fields :+ other)
    case _                                         => OrFilter(fields = List(filterA, filterB))
  }

  implicit class OptionalFilterExtension(filter: Option[Filter]) {
    private def apply(operator: (Filter, Filter) => Filter, other: Option[Filter]) =
      (filter, other) match {
        case (Some(filterA), Some(filterB)) => Some(operator(filterA, filterB))
        case _                              => filter orElse other
      }
    def &&(otherFilter: Option[Filter]) = apply(FilterOperators.&&, otherFilter)
    def ||(otherFilter: Option[Filter]) = apply(FilterOperators.||, otherFilter)
  }

  implicit class FilterExtensions(filter: Filter) {
    def &&(otherFilter: Filter) = FilterOperators.&&(filter, otherFilter)
    def ||(otherFilter: Filter) = FilterOperators.||(filter, otherFilter)
    def unary_!()               = NotFilter(field = filter)
  }
}

case class SelectFilter(dimension: String, value: Option[String]) extends Filter {
  val `type` = FilterType.Selector
}
object SelectFilter {
  def apply(dimension: String, value: String): SelectFilter =
    SelectFilter(dimension = dimension, value = Some(value))
}
case class RegexFilter(dimension: String, pattern: String) extends Filter {
  val `type` = FilterType.Regex
}
case class AndFilter(fields: List[Filter]) extends Filter { val `type` = FilterType.And }
case class OrFilter(fields: List[Filter])  extends Filter { val `type` = FilterType.Or  }
case class NotFilter(field: Filter)        extends Filter { val `type` = FilterType.Not }
case class JavascriptFilter(dimension: String, function: String) extends Filter {
  val `type` = FilterType.Javascript
}
