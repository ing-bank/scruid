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

import definitions.Filter.{ encoder => filterEncoder }

sealed trait AggregationType extends Enum with CamelCaseEnumStringEncoder
object AggregationType extends EnumCodec[AggregationType] {
  case object Count       extends AggregationType
  case object LongSum     extends AggregationType
  case object DoubleSum   extends AggregationType
  case object DoubleMax   extends AggregationType
  case object DoubleMin   extends AggregationType
  case object LongMin     extends AggregationType
  case object LongMax     extends AggregationType
  case object DoubleFirst extends AggregationType
  case object DoubleLast  extends AggregationType
  case object LongFirst   extends AggregationType
  case object LongLast    extends AggregationType
  case object ThetaSketch extends AggregationType
  case object HyperUnique extends AggregationType
  case object Cardinality extends AggregationType
  case object Filtered    extends AggregationType
  val values: Set[AggregationType] = sealerate.values[AggregationType]
}

sealed trait Aggregation {
  val `type`: AggregationType
  val name: String
}

object Aggregation {
  implicit val encoder: Encoder[Aggregation] = new Encoder[Aggregation] {
    final def apply(agg: Aggregation): Json =
      (agg match {
        case x: CountAggregation       => x.asJsonObject
        case x: CardinalityAggregation => x.asJsonObject
        case x: SingleFieldAggregation => x.asJson.asObject.get
        case x: FilteredAggregation    => x.asJson.asObject.get
      }).add("type", agg.`type`.asJson).asJson
  }
}

trait SingleFieldAggregation extends Aggregation {
  val fieldName: String
}

object SingleFieldAggregation {
  implicit val encoder: Encoder[SingleFieldAggregation] = new Encoder[SingleFieldAggregation] {
    final def apply(agg: SingleFieldAggregation): Json = agg match {
      case x: LongSumAggregation     => x.asJson
      case x: DoubleSumAggregation   => x.asJson
      case x: DoubleMaxAggregation   => x.asJson
      case x: DoubleMinAggregation   => x.asJson
      case x: LongMaxAggregation     => x.asJson
      case x: LongMinAggregation     => x.asJson
      case x: DoubleFirstAggregation => x.asJson
      case x: DoubleLastAggregation  => x.asJson
      case x: LongLastAggregation    => x.asJson
      case x: LongFirstAggregation   => x.asJson
      case x: ThetaSketchAggregation => x.asJson
      case x: HyperUniqueAggregation => x.asJson
    }
  }
}

case class CountAggregation(name: String) extends Aggregation { val `type` = AggregationType.Count }
case class LongSumAggregation(name: String, fieldName: String) extends SingleFieldAggregation {
  val `type` = AggregationType.LongSum
}
case class DoubleSumAggregation(name: String, fieldName: String) extends SingleFieldAggregation {
  val `type` = AggregationType.DoubleSum
}
case class DoubleMaxAggregation(name: String, fieldName: String) extends SingleFieldAggregation {
  val `type` = AggregationType.DoubleMax
}
case class DoubleMinAggregation(name: String, fieldName: String) extends SingleFieldAggregation {
  val `type` = AggregationType.DoubleMin
}
case class LongMinAggregation(name: String, fieldName: String) extends SingleFieldAggregation {
  val `type` = AggregationType.LongMin
}
case class LongMaxAggregation(name: String, fieldName: String) extends SingleFieldAggregation {
  val `type` = AggregationType.LongMax
}
case class DoubleFirstAggregation(name: String, fieldName: String) extends SingleFieldAggregation {
  val `type` = AggregationType.DoubleFirst
}
case class DoubleLastAggregation(name: String, fieldName: String) extends SingleFieldAggregation {
  val `type` = AggregationType.DoubleLast
}
case class LongFirstAggregation(name: String, fieldName: String) extends SingleFieldAggregation {
  val `type` = AggregationType.LongFirst
}
case class LongLastAggregation(name: String, fieldName: String) extends SingleFieldAggregation {
  val `type` = AggregationType.LongLast
}
case class ThetaSketchAggregation(name: String,
                                  fieldName: String,
                                  isInputThetaSketch: Boolean = false,
                                  size: Long = 16384)
    extends SingleFieldAggregation {
  val `type` = AggregationType.ThetaSketch
}

case class HyperUniqueAggregation(
    name: String,
    fieldName: String,
    isInputHyperUnique: Boolean = false,
    round: Boolean = false
) extends SingleFieldAggregation {
  val `type` = AggregationType.HyperUnique
}

case class CardinalityAggregation(
    override val name: String,
    fields: Iterable[Dimension],
    byRow: Boolean = false,
    round: Boolean = false
) extends Aggregation {
  override val `type`: AggregationType = AggregationType.Cardinality
}

trait FilteredAggregation extends Aggregation {
  override val `type`: AggregationType = AggregationType.Filtered
  val aggregator: Aggregation
  val filter: Filter
}

object FilteredAggregation {
  implicit val encoder: Encoder[FilteredAggregation] = new Encoder[FilteredAggregation] {
    final def apply(agg: FilteredAggregation): Json =
      (agg match {
        case x: InFilteredAggregation       => x.asJsonObject
        case x: SelectorFilteredAggregation => x.asJsonObject
      }).add("filter", filterEncoder(agg.filter)).asJson
  }
}

case class InFilteredAggregation(name: String, filter: InFilter, aggregator: Aggregation)
    extends FilteredAggregation
case class SelectorFilteredAggregation(name: String, filter: SelectFilter, aggregator: Aggregation)
    extends FilteredAggregation
