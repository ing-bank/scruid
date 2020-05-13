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

package ing.wbaa.druid.dql

import ing.wbaa.druid.{ DimensionOrder, DimensionOrderType, Direction, OrderByColumnSpec }
import ing.wbaa.druid.definitions._
import ing.wbaa.druid.dql.Dim.DimType
import ing.wbaa.druid.dql.expressions._

/**
  * This class represents a single dimension in a Druid datasource, along with all operations that can be
  * performed in a query (e.g., filtering, aggregation, post-aggregation, etc.).
  *
  * @param name The name of the dimension
  * @param outputNameOpt The output name of the result (e.g., the name after a aggregation operation)
  * @param outputTypeOpt The resulting output type
  * @param extractionFnOpt An extraction function to apply in this dimension
  */
// scalastyle:off number.of.methods
case class Dim private[dql] (name: String,
                             outputNameOpt: Option[String] = None,
                             outputTypeOpt: Option[String] = None,
                             extractionFnOpt: Option[ExtractionFn] = None)
    extends Named[Dim] {

  /**
    * Sets an alias of the dimension
    *
    * @return the resulting dimension with the given name
    */
  override def alias(name: String): Dim = copy(outputNameOpt = Option(name))

  /**
    * @return the name of the dimension
    */
  override def getName: String = name

  /**
    * Set the output type of the dimension.
    * In can be any of `STRING`, `LONG` or `FLOAT`.
    *
    * @param outputType the output type (`STRING`, `LONG` or `FLOAT`.)
    * @return the dimension having set the specified output type
    */
  def cast(outputType: String): Dim = {
    val uppercase = outputType.toUpperCase
    require(Dim.ValidTypes.contains(uppercase))
    copy(outputTypeOpt = Option(uppercase))
  }

  def cast(outputType: Dim.DimType): Dim =
    copy(outputTypeOpt = Option(outputType.toString))

  /**
    * @return this dimension with FLOAT as output type
    */
  def asFloat: Dim = cast(DimType.FLOAT)

  /**
    * @return this dimension with LONG as output type
    */
  def asLong: Dim = cast(DimType.LONG)

  /**
    * @return this dimension with STRING as output type
    */
  def asString: Dim = cast(DimType.STRING)

  /**
    * Set an extraction function
    * @param fn the extraction function to use (see [[ExtractionFn]])
    * @return
    */
  def extract(fn: ExtractionFn): Dim =
    copy(extractionFnOpt = Option(fn))

  /**
    * @return the resulting instance of Dimension. When `extractionFn` is set,
    *         then give an instance of ExtractionDimension, otherwise DefaultDimension.
    */
  protected[dql] def build(): Dimension =
    extractionFnOpt match {
      case Some(extractionFn) =>
        ExtractionDimension(name, outputNameOpt.orElse(Option(name)), outputTypeOpt, extractionFn)

      case None =>
        DefaultDimension(name, outputNameOpt.orElse(Option(name)), outputTypeOpt)
    }

  // --------------------------------------------------------------------------
  // --- Functions for creating filtering expressions
  // --------------------------------------------------------------------------

  @inline
  private def eqVal(value: String): FilteringExpression = new EqString(this, value)

  @inline
  private def eqNumDouble(value: Double): FilteringExpression = new EqDouble(this, value)

  @inline
  private def eqNumLong(value: Long): FilteringExpression = new EqLong(this, value)

  @inline
  private def compareWith(other: Dim): FilteringExpression =
    new ColumnComparison(this :: other :: Nil)

  /**
    * @return column-comparison filtering expression between this and the specified dimensions.
    */
  def ===(other: Dim): FilteringExpression = compareWith(other)

  /**
    * @return a filtering expression of value equals (string)
    */
  def ===(value: String): FilteringExpression = eqVal(value)

  /**
    * @return a filtering expression of value equals (numeric)
    */
  def ===(value: Double): FilteringExpression = eqNumDouble(value)

  def ===(value: Long): FilteringExpression = eqNumLong(value)

  /**
    * @return negated column-comparison filtering expression between this and the specified dimensions.
    */
  def =!=(other: Dim): FilteringExpression = FilteringExpressionOps.not(compareWith(other))

  /**
    * @return a filtering expression of not equals (string)
    */
  def =!=(value: String): FilteringExpression = FilteringExpressionOps.not(eqVal(value))

  /**
    * @return a filtering expression of not equals (numeric)
    */
  def =!=(value: Double): FilteringExpression = FilteringExpressionOps.not(eqNumDouble(value))

  def =!=(value: Long): FilteringExpression = FilteringExpressionOps.not(eqNumLong(value))

  /**
    * @return in-filter of this dimension for the specified values
    */
  def in(value: String, values: String*): FilteringExpression =
    new In(this, value +: values)

  def in(values: Iterable[String]): In =
    new In(this, values)

  def in[T: Numeric](value: T, values: T*): FilteringExpression =
    new InNumeric(this, value +: values)

  def in[T: Numeric](values: Iterable[T]): InNumeric[T] =
    new InNumeric(this, values)

  /**
    * @return negated in-filter of this dimension for the specified values
    */
  def notIn(value: String, values: String*): FilteringExpression =
    FilteringExpressionOps.not(in(value +: values))

  def notIn(values: Iterable[String]): FilteringExpression =
    FilteringExpressionOps.not(in(values))

  def notIn[T: Numeric](value: T, values: T*): FilteringExpression =
    FilteringExpressionOps.not(in(value +: values))

  def notIn[T: Numeric](values: Iterable[T]): FilteringExpression =
    FilteringExpressionOps.not(in(values))

  /**
    * @return like-filter of this dimension for the specified LIKE pattern
    */
  def like(pattern: String): FilteringExpression = new Like(this, pattern)

  /**
    * @return regex-filter of this dimension for the specified Java regular expression pattern
    */
  def regex(pattern: String): FilteringExpression = new RegexExp(this, pattern)

  /**
    * @return selector expression stating that the contents of the dimension should be null
    */
  def isNull: FilteringExpression = new NullDim(this)

  /**
    * @return selector expression stating that the contents of the dimension should not be null
    */
  def isNotNull: FilteringExpression = FilteringExpressionOps.not(this.isNull)

  /**
    * Filter based on a strict lower bound of dimension values, using numeric ordering.
    *
    * @return a bound filtering expression, with numeric ordering, specifying that the value of
    *         the dimension should be greater than the given number.
    */
  def >(value: Double): FilteringExpression = new Gt(this, value)

  /**
    * Filter based on a lower bound of dimension values, using numeric ordering.
    *
    * @return a bound filtering expression, with numeric ordering, specifying that the value of
    *         the dimension should be greater than, or equal to the given number.
    */
  def >=(value: Double): FilteringExpression = new GtEq(this, value)

  /**
    * Filter based on a strict upper bound of dimension values, using numeric ordering.
    *
    * @return a bound filtering expression, with numeric ordering, specifying that the value of
    *         the dimension should be less than the given number.
    */
  def <(value: Double): FilteringExpression = new Lt(this, value)

  /**
    * Filter based on an upper bound of dimension values, using numeric ordering.
    *
    * @return a bound filtering expression, with numeric ordering, specifying that the value of
    *         the dimension should be less than, or equal to the given number.
    */
  def <=(value: Double): FilteringExpression = new LtEq(this, value)

  /**
    * Filter on ranges of dimension values, using numeric ordering.
    *
    * @param lower the lower bound
    * @param upper the upper bound
    * @param lowerStrict Perform strict comparison on the lower bound ("<" instead of "<=")
    * @param upperStrict Perform strict comparison on the upper bound (">" instead of ">=")
    *
    * @return a bound filtering expression, with numeric ordering
    */
  def between(lower: Double, upper: Double, lowerStrict: Boolean, upperStrict: Boolean): Bound =
    Bound(
      dimension = name,
      lower = Option(lower.toString),
      lowerStrict = Option(lowerStrict),
      upper = Option(upper.toString),
      upperStrict = Option(upperStrict),
      ordering = Option(DimensionOrderType.Numeric)
    )

  /**
    * Filter on ranges of dimension values, using numeric ordering, where lower and upper bounds are non-strict.
    *
    * @param lower the lower bound
    * @param upper the upper bound
    *
    * @return a bound filtering expression, with numeric ordering, where each bound is not-strict
    */
  def between(lower: Double, upper: Double): Bound =
    between(lower, upper, lowerStrict = false, upperStrict = false)

  /**
    * Filter based on a lower bound of dimension values, using lexicographic ordering.
    *
    * @return a bound filtering expression, with lexicographic ordering, specifying that the value of
    *         the dimension should be greater than the given number.
    */
  def >(value: String): Bound =
    Bound(dimension = name,
          lower = Option(value),
          upper = None,
          ordering = Option(DimensionOrderType.Lexicographic))

  /**
    * Filter based on a strict lower bound of dimension values, using lexicographic ordering.
    *
    * @return a bound filtering expression, with lexicographic ordering, specifying that the value of
    *         the dimension should be greater than, or equal to the given number.
    */
  def >=(value: String): Bound =
    Bound(dimension = name,
          lower = Option(value),
          lowerStrict = Some(true),
          upper = None,
          ordering = Option(DimensionOrderType.Lexicographic))

  /**
    * Filter based on a upper bound of dimension values, using lexicographic ordering.
    *
    * @return a bound filtering expression, with lexicographic ordering, specifying that the value of
    *         the dimension should be less than the given number.
    */
  def <(value: String): Bound =
    Bound(dimension = name,
          lower = None,
          upper = Option(value),
          ordering = Option(DimensionOrderType.Lexicographic))

  /**
    * Filter based on a strict upper bound of dimension values, using lexicographic ordering.
    *
    * @return a bound filtering expression, with lexicographic ordering, specifying that the value of
    *         the dimension should be less than, or equal to the given number.
    */
  // scalastyle:off method.name
  @deprecated(s"use <=", "2.4.0")
  def =<(value: String): Bound = <=(value)
  // scalastyle:on method.name

  /**
    * Filter based on a strict upper bound of dimension values, using lexicographic ordering.
    *
    * @return a bound filtering expression, with lexicographic ordering, specifying that the value of
    *         the dimension should be less than, or equal to the given number.
    */
  def <=(value: String): Bound =
    Bound(dimension = name,
          lower = None,
          upper = Option(value),
          upperStrict = Some(true),
          ordering = Option(DimensionOrderType.Lexicographic))

  /**
    * Filter on ranges of dimension values, using lexicographic ordering.
    *
    * @param lower the lower bound
    * @param upper the upper bound
    * @param lowerStrict Perform strict comparison on the lower bound ("<" instead of "<=")
    * @param upperStrict Perform strict comparison on the upper bound (">" instead of ">=")
    *
    * @return a bound filtering expression, with lexicographic ordering
    */
  def between(lower: String, upper: String, lowerStrict: Boolean, upperStrict: Boolean): Bound =
    Bound(
      dimension = name,
      lower = Option(lower),
      lowerStrict = Option(lowerStrict),
      upper = Option(upper),
      upperStrict = Option(upperStrict),
      ordering = Option(DimensionOrderType.Lexicographic)
    )

  /**
    * Filter on ranges of dimension values, using lexicographic ordering, where lower and upper bounds are non-strict.
    *
    * @param lower the lower bound
    * @param upper the upper bound
    *
    * @return a bound filtering expression, with lexicographic ordering, where each bound is not-strict
    */
  def between(lower: String, upper: String): Bound =
    between(lower, upper, lowerStrict = false, upperStrict = false)

  /**
    * Performs range filtering on columns that contain long millisecond values,
    * the boundary specified as ISO 8601 time interval
    *
    * @return the interval filtering expression
    */
  def interval(value: String): FilteringExpression = new Interval(this, value :: Nil)

  /**
    * Performs range filtering on columns that contain long millisecond values,
    * the boundaries specified as ISO 8601 time intervals
    *
    * @return the interval filtering expression
    */
  def intervals(first: String, second: String, rest: String*): FilteringExpression =
    new Interval(this, Seq(first, second) ++: rest)

  def intervals(values: Iterable[String]): FilteringExpression =
    new Interval(this, values.toList)

  /**
    * Filter on partial string matches
    *
    * @param value the value to search
    * @param caseSensitive enable/disable case-sensitive search
    *
    * @return the search filtering expression
    */
  def contains(value: String, caseSensitive: Boolean = true): FilteringExpression =
    new Contains(this, value, caseSensitive)

  /**
    * Filter on partial string matches, with ignore case search
    *
    * @return the search filtering expression
    */
  def containsIgnoreCase(value: String): FilteringExpression =
    new Contains(this, value, caseSensitive = false)

  /**
    * Filter on partial string matches, with ignore case search
    *
    * @return the search filtering expression
    */
  def containsInsensitive(value: String): FilteringExpression =
    new InsensitiveContains(this, value)

  /**
    * Filter spatially indexed columns by specifying the bounds of minimum and maximum coordinates
    *
    * @param minCoords a list of minimum dimension coordinates for coordinates [x, y, z, ...]
    * @param maxCoords a list of maximum dimension coordinates for coordinates [x, y, z, ...]
    *
    * @return the resulting spatial filtering expression
    */
  def within(minCoords: Iterable[Double], maxCoords: Iterable[Double]): FilteringExpression =
    new GeoRectangular(this, minCoords, maxCoords)

  /**
    * Filter spatially indexed columns by specifying the origin coordinates and a distance
    *
    * @param coords a list of origin coordinates in the form [x, y, z, ...]
    * @param distance the distance from origin coordinates.
    *                 It can be specified in kilometers, miles or radius degrees (default)
    *
    * @return the resulting spatial filtering expression
    */
  def within(coords: Iterable[Double],
             distance: Double,
             unit: Distance.Unit = Distance.DistanceUnit.DEGREES): FilteringExpression =
    new GeoRadius(this, coords, Distance.toDegrees(distance, unit))

  // --------------------------------------------------------------------------
  // --- Functions for creating OrderByColumnSpec
  // --------------------------------------------------------------------------

  /**
    * Set column ascending direction, using lexicographic ordering
    */
  def asc: OrderByColumnSpec =
    OrderByColumnSpec(dimension = this.name, direction = Direction.Ascending)

  /**
    * Set column descending direction, using lexicographic ordering
    */
  def desc: OrderByColumnSpec =
    OrderByColumnSpec(dimension = this.name, direction = Direction.Descending)

  /**
    * Set column ascending direction, using the specified ordering
    */
  def asc(orderType: DimensionOrderType): OrderByColumnSpec =
    OrderByColumnSpec(
      dimension = this.name,
      direction = Direction.Ascending,
      dimensionOrder = DimensionOrder(orderType)
    )

  /**
    * Set column descending direction, using the specified ordering
    */
  def desc(orderType: DimensionOrderType): OrderByColumnSpec =
    OrderByColumnSpec(
      dimension = this.name,
      direction = Direction.Descending,
      dimensionOrder = DimensionOrder(orderType)
    )

  // --------------------------------------------------------------------------
  // --- Functions for creating aggregation expressions
  // --------------------------------------------------------------------------

  /**
    * Aggregation to count the number of rows
    *
    * Please note the count aggregator counts the number of Druid rows, which
    * does not always reflect the number of raw events ingested. This is because
    * Apache Druid can be configured to roll up data at ingestion time.
    *
    * To count the number of ingested rows of data, include a count aggregator
    * at ingestion time, and a longSum aggregator at query time.
    */
  def count: AggregationExpression = AggregationOps.count

  /**
    * Aggregation to compute the sum of values (as a 64-bit signed integer)
    */
  def longSum: AggregationExpression = AggregationOps.longSum(this)

  /**
    * Aggregation to compute the maximum of all metric values and Long.MAX_VALUE
    */
  def longMax: AggregationExpression = AggregationOps.longMax(this)

  /**
    * Aggregation to compute the minimum of all metric values and Long.MIN_VALUE
    */
  def longMin: AggregationExpression = AggregationOps.longMin(this)

  /**
    * Aggregation to compute the metric value with the minimum timestamp or 0 if no row exist
    */
  def longFirst: AggregationExpression = AggregationOps.longFirst(this)

  /**
    * Aggregation to compute the metric value with the maximum timestamp or 0 if no row exist
    */
  def longLast: AggregationExpression = AggregationOps.longLast(this)

  /**
    * Aggregation to compute the sum of values (as a 64-bit floating point value)
    */
  def doubleSum: AggregationExpression = AggregationOps.doubleSum(this)

  /**
    * Aggregation to compute the maximum of all metric values and Double.NEGATIVE_INFINITY
    */
  def doubleMax: AggregationExpression = AggregationOps.doubleMax(this)

  /**
    * Aggregation to compute the minimum of all metric values and Double.POSITIVE_INFINITY
    */
  def doubleMin: AggregationExpression = AggregationOps.doubleMin(this)

  /**
    * Aggregation to compute the metric value with the minimum timestamp or 0 if no row exist
    */
  def doubleFirst: AggregationExpression = AggregationOps.doubleFirst(this)

  /**
    * Aggregation to compute the metric value with the maximum timestamp or 0 if no row exist
    */
  def doubleLast: AggregationExpression = AggregationOps.doubleLast(this)

  /**
    * Aggregation to compute the sum of values (as a 32-bit floating point value)
    */
  def floatSum: AggregationExpression = AggregationOps.floatSum(this)

  /**
    * Aggregation to compute the maximum of all metric values and Float.NEGATIVE_INFINITY
    */
  def floatMax: AggregationExpression = AggregationOps.floatMax(this)

  /**
    * Aggregation to compute the minimum of all metric values and Float.POSITIVE_INFINITY
    */
  def floatMin: AggregationExpression = AggregationOps.floatMin(this)

  /**
    * Aggregation to compute the metric value with the minimum timestamp or 0 if no row exist
    */
  def floatFirst: AggregationExpression = AggregationOps.floatFirst(this)

  /**
    * Aggregation to compute the metric value with the maximum timestamp or 0 if no row exist
    */
  def floatLast: AggregationExpression = AggregationOps.floatLast(this)

  /**
    * Aggregation to compute the metric value with the minimum timestamp or null if no row exist
    */
  def stringFirst: AggregationExpression = AggregationOps.stringFirst(this)

  /**
    * Aggregation to compute the metric value with the maximum timestamp or null if no row exist
    */
  def stringLast: AggregationExpression = AggregationOps.stringLast(this)

  /**
    * Aggregation to compute theta-sketches
    */
  def thetaSketch: ThetaSketchAgg = AggregationOps.thetaSketch(this)

  /**
    * Aggregation to compute the estimated cardinality of a dimension that
    * has been aggregated as a "hyperUnique" metric at indexing time.
    */
  def hyperUnique: HyperUniqueAgg = AggregationOps.hyperUnique(this)

  /**
    * Wraps any given aggregator, but only aggregates the values
    * for which the given dimension filter matches, by using the in filter.
    *
    * @param aggregator the aggregator to wrap
    * @param first the value to filter
    * @param rest the rest values to filter (when are more than one values to filter)
    */
  def inFiltered(aggregator: AggregationExpression,
                 first: String,
                 rest: String*): AggregationExpression =
    AggregationOps.inFiltered(this, aggregator, first, rest: _*)

  def inFiltered(aggregator: AggregationExpression,
                 values: Iterable[String]): AggregationExpression =
    AggregationOps.inFiltered(this.name, aggregator, values)

  /**
    * Wraps any given aggregator
    *
    * @param aggregator the aggregator to wrap
    */
  def selectorFiltered(aggregator: AggregationExpression): SelectorFilteredAgg =
    AggregationOps.selectorFiltered(this, aggregator)

  /**
    * Wraps any given aggregator, but only aggregates the values
    * for which the given dimension filter matches, by using the selector filter.
    *
    * @param aggregator the aggregator to wrap
    * @param value the values to select
    */
  def selectorFiltered(aggregator: AggregationExpression, value: String): SelectorFilteredAgg =
    AggregationOps.selectorFiltered(this, aggregator, value)

  // --------------------------------------------------------------------------
  // --- Functions for creating post-aggregation expressions
  // --------------------------------------------------------------------------

  @inline
  private def arithmeticAgg(value: Double, fn: ArithmeticFunction): ArithmeticPostAgg =
    ArithmeticPostAgg(leftField = new FieldAccessPostAgg(this.name),
                      rightField = new ConstantPostAgg(value),
                      fn = fn)
  @inline
  private def arithmeticFieldAgg(right: Dim, fn: ArithmeticFunction): ArithmeticPostAgg =
    ArithmeticPostAgg(leftField = new FieldAccessPostAgg(this.name),
                      rightField = new FieldAccessPostAgg(right.name),
                      fn = fn)

  /**
    * Arithmetic addition post-aggregator
    */
  def +(v: Double): ArithmeticPostAgg = arithmeticAgg(v, ArithmeticFunction.PLUS)

  /**
    * Arithmetic subtraction post-aggregator
    */
  def -(v: Double): ArithmeticPostAgg = arithmeticAgg(v, ArithmeticFunction.MINUS)

  /**
    * Arithmetic multiplication post-aggregator
    */
  def *(v: Double): ArithmeticPostAgg = arithmeticAgg(v, ArithmeticFunction.MULT)

  /**
    * Arithmetic division post-aggregator, that always returns 0 when dividing by 0,
    * regardless of the numerator
    */
  def /(v: Double): ArithmeticPostAgg = arithmeticAgg(v, ArithmeticFunction.DIV)

  /**
    * Arithmetic division post-aggregator
    */
  def quotient(v: Double): ArithmeticPostAgg = arithmeticAgg(v, ArithmeticFunction.QUOT)

  /**
    * Arithmetic addition post-aggregator
    */
  def +(v: Dim): ArithmeticPostAgg = arithmeticFieldAgg(v, ArithmeticFunction.PLUS)

  /**
    * Arithmetic subtraction post-aggregator
    */
  def -(v: Dim): ArithmeticPostAgg = arithmeticFieldAgg(v, ArithmeticFunction.MINUS)

  /**
    * Arithmetic multiplication post-aggregator
    */
  def *(v: Dim): ArithmeticPostAgg = arithmeticFieldAgg(v, ArithmeticFunction.MULT)

  /**
    * Arithmetic division post-aggregator, that always returns 0 when dividing by 0,
    * regardless of the numerator
    */
  def /(v: Dim): ArithmeticPostAgg = arithmeticFieldAgg(v, ArithmeticFunction.DIV)

  /**
    * Arithmetic division post-aggregator
    */
  def quotient(v: Dim): ArithmeticPostAgg = arithmeticFieldAgg(v, ArithmeticFunction.QUOT)

  /**
    * HyperUnique Cardinality post-aggregator
    */
  def hyperUniqueCardinality: HyperUniqueCardinalityPostAgg =
    HyperUniqueCardinalityPostAgg(this.name)
}
// scalastyle:on number.of.methods

object Dim {

  final val ValidTypes = Set("STRING", "LONG", "FLOAT")

  type DimType = DimType.Value

  object DimType extends Enumeration {
    val STRING: DimType = Value(0, "STRING")
    val LONG: DimType   = Value(1, "LONG")
    val FLOAT: DimType  = Value(2, "FLOAT")
  }
}
