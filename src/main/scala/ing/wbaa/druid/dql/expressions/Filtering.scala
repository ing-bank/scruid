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

import ing.wbaa.druid.{ definitions, DimensionOrderType }
import ing.wbaa.druid.definitions._
import ing.wbaa.druid.dql.Dim

sealed trait FilteringExpression {

  protected[dql] def createFilter: Filter

  protected[dql] def createHaving: Having

  def asFilter: Filter = createFilter

  def asHaving: Having = this match {
    case _: FilterOnlyOperator => FilterHaving(this.asFilter)
    case _                     => createHaving
  }

  def or(others: FilteringExpression*): FilteringExpression = new Or(others :+ this)

  def and(others: FilteringExpression*): FilteringExpression = new And(others :+ this)
}

class And(expressions: Iterable[FilteringExpression]) extends FilteringExpression {
  override protected[dql] def createFilter: Filter = AndFilter(expressions.map(_.createFilter))
  override protected[dql] def createHaving: Having = AndHaving(expressions.map(_.createHaving))
}

class Or(expressions: Iterable[FilteringExpression]) extends FilteringExpression {
  override protected[dql] def createFilter: Filter = OrFilter(expressions.map(_.createFilter))
  override protected[dql] def createHaving: Having = OrHaving(expressions.map(_.createHaving))
}

class Not(val op: FilteringExpression) extends FilteringExpression {
  override protected[dql] def createFilter: Filter = NotFilter(op.createFilter)
  override protected[dql] def createHaving: Having =
    op match {
      case _: FilterOnlyOperator => FilterHaving(NotFilter(op.createFilter))
      case _                     => NotHaving(op.createHaving)
    }
}

class EqString(dim: Dim, value: String) extends FilteringExpression {
  override protected[dql] def createFilter: Filter =
    SelectFilter(dim.name, Option(value), dim.extractionFnOpt)
  override protected[dql] def createHaving: Having =
    if (dim.extractionFnOpt.isDefined) FilterHaving(this.createFilter)
    else DimSelectorHaving(dim.name, value)

}

class EqDouble(dim: Dim, value: Double) extends FilteringExpression {
  override protected[dql] def createFilter: Filter =
    SelectFilter(dim.name, Option(value.toString), dim.extractionFnOpt)
  override protected[dql] def createHaving: Having =
    if (dim.extractionFnOpt.isDefined) FilterHaving(this.createFilter)
    else EqualToHaving(dim.name, value)
}

class EqLong(dim: Dim, value: Long) extends FilteringExpression {
  override protected[dql] def createFilter: Filter =
    SelectFilter(dim.name, Option(value.toString), dim.extractionFnOpt)
  override protected[dql] def createHaving: Having =
    if (dim.extractionFnOpt.isDefined) FilterHaving(this.createFilter)
    else EqualToHaving(dim.name, value.toDouble)
}

trait FilterOnlyOperator extends FilteringExpression {
  override protected[dql] def createHaving: Having = FilterHaving(this.createFilter)
}

class In(dim: Dim, values: Iterable[String]) extends FilteringExpression with FilterOnlyOperator {
  override protected[dql] def createFilter: Filter = InFilter(dim.name, values)
}

class InNumeric[T: Numeric](dim: Dim, values: Iterable[T])
    extends FilteringExpression
    with FilterOnlyOperator {
  override protected[dql] def createFilter: Filter = InFilter(dim.name, values.map(_.toString))
}

class Like(dim: Dim, pattern: String) extends FilteringExpression with FilterOnlyOperator {
  override protected[dql] def createFilter: Filter = LikeFilter(dim.name, pattern)
}

class RegexExp(dim: Dim, pattern: String) extends FilteringExpression with FilterOnlyOperator {
  override protected[dql] def createFilter: Filter = RegexFilter(dim.name, pattern)
}

class NullDim(dim: Dim) extends FilteringExpression with FilterOnlyOperator {
  override protected[dql] def createFilter: Filter = SelectFilter(dim.name, None)
}

class Gt(dim: Dim, value: Double) extends FilteringExpression {
  override protected[dql] def createFilter: Filter =
    BoundFilter(
      dimension = dim.name,
      lower = Option(value.toString),
      lowerStrict = Some(true),
      ordering = Option(DimensionOrderType.Numeric),
      extractionFn = dim.extractionFnOpt
    )

  override protected[dql] def createHaving: Having =
    if (dim.extractionFnOpt.isDefined) FilterHaving(this.createFilter)
    else GreaterThanHaving(dim.name, value)
}

class GtEq(dim: Dim, value: Double) extends FilteringExpression {
  override protected[dql] def createFilter: Filter =
    BoundFilter(
      dimension = dim.name,
      lower = Option(value.toString),
      lowerStrict = Some(false),
      ordering = Option(DimensionOrderType.Numeric),
      extractionFn = dim.extractionFnOpt
    )

  override protected[dql] def createHaving: Having =
    if (dim.extractionFnOpt.isDefined) FilterHaving(this.createFilter)
    else OrHaving(EqualToHaving(dim.name, value) :: GreaterThanHaving(dim.name, value) :: Nil)
}

class Lt(dim: Dim, value: Double) extends FilteringExpression {
  override protected[dql] def createFilter: Filter =
    BoundFilter(
      dimension = dim.name,
      upper = Option(value.toString),
      upperStrict = Some(true),
      ordering = Option(DimensionOrderType.Numeric),
      extractionFn = dim.extractionFnOpt
    )

  override protected[dql] def createHaving: Having =
    if (dim.extractionFnOpt.isDefined) FilterHaving(this.createFilter)
    else LessThanHaving(dim.name, value)
}

class LtEq(dim: Dim, value: Double) extends FilteringExpression {

  override protected[dql] def createFilter: Filter =
    BoundFilter(
      dimension = dim.name,
      upper = Option(value.toString),
      upperStrict = Some(false),
      ordering = Option(DimensionOrderType.Numeric),
      extractionFn = dim.extractionFnOpt
    )

  override protected[dql] def createHaving: Having =
    if (dim.extractionFnOpt.isDefined) FilterHaving(this.createFilter)
    else OrHaving(EqualToHaving(dim.name, value) :: LessThanHaving(dim.name, value) :: Nil)

}

case class Bound(dimension: String,
                 lower: Option[String] = None,
                 upper: Option[String] = None,
                 lowerStrict: Option[Boolean] = None,
                 upperStrict: Option[Boolean] = None,
                 ordering: Option[DimensionOrderType] = None,
                 extractionFn: Option[ExtractionFn] = None)
    extends FilteringExpression
    with FilterOnlyOperator {

  def withOrdering(v: DimensionOrderType): Bound = copy(ordering = Option(v))

  def withExtractionFn(fn: ExtractionFn): Bound = copy(extractionFn = Option(fn))

  override protected[dql] def createFilter: Filter =
    BoundFilter(dimension, lower, upper, lowerStrict, upperStrict, ordering, extractionFn)

}

class ColumnComparison(dimensions: Iterable[Dim])
    extends FilteringExpression
    with FilterOnlyOperator {
  override protected[dql] def createFilter: Filter = {

    val dimSpecs = dimensions.map { dim =>
      dim.extractionFnOpt match {
        case Some(extractionFn) =>
          ExtractionDimension(dimension = dim.name,
                              outputName = dim.outputNameOpt,
                              outputType = dim.outputTypeOpt,
                              extractionFn = extractionFn)
        case None =>
          Dimension(dimension = dim.name,
                    outputName = dim.outputNameOpt,
                    outputType = dim.outputTypeOpt)
      }
    }
    ColumnComparisonFilter(dimSpecs)
  }
}

class Interval(dim: Dim, values: Iterable[String])
    extends FilteringExpression
    with FilterOnlyOperator {
  override protected[dql] def createFilter: Filter =
    IntervalFilter(dim.name, values, dim.extractionFnOpt)
}

class Contains(dim: Dim, value: String, caseSensitive: Boolean)
    extends FilteringExpression
    with FilterOnlyOperator {

  override protected[dql] def createFilter: Filter =
    SearchFilter(dim.name,
                 query = ContainsCaseSensitive(value, Option(caseSensitive)),
                 dim.extractionFnOpt)
}
class InsensitiveContains(dim: Dim, value: String)
    extends FilteringExpression
    with FilterOnlyOperator {
  override protected[dql] def createFilter: Filter =
    SearchFilter(dim.name, query = definitions.ContainsInsensitive(value), dim.extractionFnOpt)
}

class GeoRectangular(dim: Dim, minCoords: Iterable[Double], maxCoords: Iterable[Double])
    extends FilteringExpression
    with FilterOnlyOperator {

  override protected[dql] def createFilter: Filter =
    SpatialFilter(dimension = dim.name, bound = RectangularBound(minCoords, maxCoords))
}

class GeoRadius(dim: Dim, coords: Iterable[Double], radius: Double)
    extends FilteringExpression
    with FilterOnlyOperator {

  override protected[dql] def createFilter: Filter =
    SpatialFilter(dimension = dim.name, bound = RadiusBound(coords, radius))
}
