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

sealed trait HavingType extends Enum with CamelCaseEnumStringEncoder
object HavingType extends EnumCodec[HavingType] {

  case object Filter      extends HavingType
  case object EqualTo     extends HavingType
  case object GreaterThan extends HavingType
  case object LessThan    extends HavingType
  case object DimSelector extends HavingType
  case object And         extends HavingType
  case object Or          extends HavingType
  case object Not         extends HavingType

  val values: Set[HavingType] = sealerate.values[HavingType]
}

sealed trait Having {
  val `type`: HavingType
}

object Having {
  implicit val encoder: Encoder[Having] = new Encoder[Having] {
    override def apply(having: Having) =
      (having match {
        case x: FilterHaving      => x.asJsonObject
        case x: EqualToHaving     => x.asJsonObject
        case x: GreaterThanHaving => x.asJsonObject
        case x: LessThanHaving    => x.asJsonObject
        case x: DimSelectorHaving => x.asJsonObject
        case x: AndHaving         => x.asJsonObject
        case x: OrHaving          => x.asJsonObject
        case x: NotHaving         => x.asJsonObject
      }).add("type", having.`type`.asJson).asJson
  }
}

object HavingSpecOperators {
  def &&(havingA: Having, havingB: Having): AndHaving = (havingA, havingB) match {
    case (AndHaving(fields), AndHaving(otherFields)) => AndHaving(fields ++ otherFields)
    case (AndHaving(fields), other)                  => AndHaving(fields :+ other)
    case (other, AndHaving(fields))                  => AndHaving(fields :+ other)
    case _                                           => AndHaving(List(havingA, havingB))
  }

  def ||(havingA: Having, havingB: Having): OrHaving = (havingA, havingB) match {
    case (OrHaving(fields), OrHaving(otherFields)) => OrHaving(fields ++ otherFields)
    case (OrHaving(fields), other)                 => OrHaving(fields :+ other)
    case (other, OrHaving(fields))                 => OrHaving(fields :+ other)
    case _                                         => OrHaving(List(havingA, havingB))
  }

  implicit class HavingSpecOps(having: Having) {
    def &&(other: Having): AndHaving = HavingSpecOperators.&&(having, other)
    def ||(other: Having): OrHaving  = HavingSpecOperators.||(having, other)
    def unary_!(): NotHaving         = NotHaving(having)
  }
}

case class FilterHaving(filter: Filter) extends Having {
  override val `type` = HavingType.Filter
}

case class EqualToHaving(aggregation: String, value: Double) extends Having {
  override val `type` = HavingType.EqualTo
}

case class GreaterThanHaving(aggregation: String, value: Double) extends Having {
  override val `type` = HavingType.GreaterThan
}

case class LessThanHaving(aggregation: String, value: Double) extends Having {
  override val `type` = HavingType.LessThan
}

case class DimSelectorHaving(dimension: String, value: String) extends Having {
  override val `type` = HavingType.DimSelector
}

case class AndHaving(havingSpecs: List[Having]) extends Having {
  override val `type` = HavingType.And
}

case class OrHaving(havingSpecs: List[Having]) extends Having {
  override val `type` = HavingType.Or
}

case class NotHaving(havingSpec: Having) extends Having {
  override val `type` = HavingType.Not
}
