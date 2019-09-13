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

package ing.wbaa.druid.dql.expressions

import ing.wbaa.druid.definitions._
import ing.wbaa.druid.dql.Dim

sealed trait AggregationExpression extends Named[AggregationExpression] {

  protected[dql] def build(): Aggregation

  def isComplex: Boolean = false
}

final class CountAgg(name: Option[String] = None) extends AggregationExpression {

  override protected[dql] def build(): Aggregation = CountAggregation(this.getName)

  override def alias(name: String): AggregationExpression = new CountAgg(Option(name))

  override def getName: String = name.getOrElse("count")
}

final class LongSumAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation = LongSumAggregation(this.getName, fieldName)

  override def alias(name: String): AggregationExpression = new LongSumAgg(fieldName, Option(name))

  override def getName: String = name.getOrElse(s"long_sum_$fieldName")
}

final class LongMaxAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    LongMaxAggregation(this.getName, fieldName)

  override def alias(name: String): AggregationExpression = new LongMaxAgg(fieldName, Option(name))

  override def getName: String = name.getOrElse(s"long_max_$fieldName")
}

final class LongMinAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    LongMinAggregation(this.getName, fieldName)

  override def alias(name: String): AggregationExpression = new LongMinAgg(fieldName, Option(name))

  override def getName: String = name.getOrElse(s"long_min_$fieldName")
}

final class LongFirstAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    LongFirstAggregation(this.getName, fieldName)

  override def alias(name: String): AggregationExpression =
    new LongFirstAgg(fieldName, Option(name))

  override def getName: String = name.getOrElse(s"long_first_$fieldName")
}

final class LongLastAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    LongLastAggregation(this.getName, fieldName)

  override def alias(name: String): AggregationExpression = new LongLastAgg(fieldName, Option(name))

  override def getName: String = name.getOrElse(s"long_last_$fieldName")
}

final class DoubleSumAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    DoubleSumAggregation(this.getName, fieldName)

  override def alias(name: String): AggregationExpression =
    new DoubleSumAgg(fieldName, Option(name))

  override def getName: String = name.getOrElse(s"double_sum_$fieldName")
}

final class DoubleMaxAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    DoubleMaxAggregation(this.getName, fieldName)

  override def alias(name: String): AggregationExpression =
    new DoubleMaxAgg(fieldName, Option(name))

  override def getName: String = name.getOrElse(s"double_max_$fieldName")
}

final class DoubleMinAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    DoubleMinAggregation(this.getName, fieldName)

  override def alias(name: String): AggregationExpression =
    new DoubleMinAgg(fieldName, Option(name))

  override def getName: String = name.getOrElse(s"double_min_$fieldName")
}

final class DoubleFirstAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    DoubleFirstAggregation(this.getName, fieldName)

  override def alias(name: String): AggregationExpression =
    new DoubleFirstAgg(fieldName, Option(name))

  override def getName: String = name.getOrElse(s"double_first_$fieldName")
}

final class DoubleLastAgg(fieldName: String, name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    DoubleLastAggregation(this.getName, fieldName)

  override def alias(name: String): AggregationExpression =
    new DoubleLastAgg(fieldName, Option(name))

  override def getName: String = name.getOrElse(s"double_last_$fieldName")
}

final case class ThetaSketchAgg(fieldName: String,
                                name: Option[String] = None,
                                isInputThetaSketch: Boolean = false,
                                size: Long = ThetaSketchAgg.DefaultSize)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    ThetaSketchAggregation(
      this.getName,
      fieldName,
      isInputThetaSketch,
      size
    )

  override def alias(name: String): ThetaSketchAgg = copy(name = Option(name))

  override def isComplex: Boolean = true

  def isInputThetaSketch(v: Boolean): ThetaSketchAgg = copy(isInputThetaSketch = v)

  def withSize(size: Long): ThetaSketchAgg = copy(size = size)

  def set(isInputThetaSketch: Boolean = false,
          size: Long = ThetaSketchAgg.DefaultSize): ThetaSketchAgg =
    copy(isInputThetaSketch = isInputThetaSketch, size = size)

  override def getName: String = name.getOrElse(s"theta_sketch_$fieldName")
}

object ThetaSketchAgg {
  final val DefaultSize = 16384
}

final case class HyperUniqueAgg(fieldName: String,
                                name: Option[String] = None,
                                isInputHyperUnique: Boolean = false,
                                round: Boolean = false)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    HyperUniqueAggregation(
      this.getName,
      fieldName,
      isInputHyperUnique,
      round
    )
  override def alias(name: String): HyperUniqueAgg = copy(name = Option(name))

  override def isComplex: Boolean = true

  def setInputHyperUnique(v: Boolean): HyperUniqueAgg = copy(isInputHyperUnique = v)

  def setRound(v: Boolean): HyperUniqueAgg = copy(round = v)

  def set(isInputHyperUnique: Boolean = false, isRound: Boolean = false): HyperUniqueAgg =
    copy(isInputHyperUnique = isInputHyperUnique, round = isRound)

  override def getName: String = name.getOrElse(s"hyper_unique_$fieldName")
}

final case class CardinalityAgg(fields: Seq[Dim],
                                name: Option[String] = None,
                                byRow: Boolean = false,
                                round: Boolean = false)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation = CardinalityAggregation(
    this.getName,
    fields.map(_.build()),
    byRow,
    round
  )

  override def alias(name: String): AggregationExpression = copy(name = Option(name))

  override def getName: String = name.getOrElse(s"cardinality_${fields.map(_.name).mkString("_")}")

  def setByRow(v: Boolean): CardinalityAgg = copy(byRow = v)

  def setRound(v: Boolean): CardinalityAgg = copy(round = v)

  def set(byRow: Boolean = false, round: Boolean = false): CardinalityAgg =
    copy(byRow = byRow, round = round)

}

final case class InFilteredAgg(dimension: String,
                               values: Seq[String],
                               aggregator: Aggregation,
                               name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    InFilteredAggregation(
      this.getName,
      InFilter(dimension, values),
      aggregator
    )

  override def alias(name: String): InFilteredAgg = copy(name = Option(name))

  override def getName: String = name.getOrElse(s"in_filtered_$dimension")
}

final case class SelectorFilteredAgg(dimension: String,
                                     value: Option[String] = None,
                                     aggregator: Aggregation,
                                     name: Option[String] = None)
    extends AggregationExpression {

  override protected[dql] def build(): Aggregation =
    SelectorFilteredAggregation(
      this.getName,
      SelectFilter(dimension, value),
      aggregator
    )

  override def alias(name: String): SelectorFilteredAgg = copy(name = Option(name))

  override def getName: String = name.getOrElse(s"selector_filtered_$dimension")
}
