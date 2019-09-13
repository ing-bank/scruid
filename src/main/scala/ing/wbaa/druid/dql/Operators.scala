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

import ing.wbaa.druid.definitions.{ ExtractionFn, Filter }
import ing.wbaa.druid.dql.expressions._

trait AggregationOps {

  def longSum(dimName: String): LongSumAgg = new LongSumAgg(dimName)
  def longSum(dim: Dim): LongSumAgg        = longSum(dim.name)

  def longMax(dimName: String): LongMaxAgg = new LongMaxAgg(dimName)
  def longMax(dim: Dim): LongMaxAgg        = longMax(dim.name)

  def longMin(dimName: String): LongMinAgg = new LongMinAgg(dimName)
  def longMin(dim: Dim): LongMinAgg        = longMin(dim.name)

  def longFirst(dimName: String): LongFirstAgg = new LongFirstAgg(dimName)
  def longFirst(dim: Dim): LongFirstAgg        = longFirst(dim.name)

  def longLast(dimName: String): LongLastAgg = new LongLastAgg(dimName)
  def longLast(dim: Dim): LongLastAgg        = longLast(dim.name)

  def doubleSum(dimName: String): DoubleSumAgg = new DoubleSumAgg(dimName)
  def doubleSum(dim: Dim): DoubleSumAgg        = doubleSum(dim.name)

  def doubleMax(dimName: String): DoubleMaxAgg = new DoubleMaxAgg(dimName)
  def doubleMax(dim: Dim): DoubleMaxAgg        = doubleMax(dim.name)

  def doubleMin(dimName: String): DoubleMinAgg = new DoubleMinAgg(dimName)
  def doubleMin(dim: Dim): DoubleMinAgg        = doubleMin(dim.name)

  def doubleFirst(dimName: String): DoubleFirstAgg = new DoubleFirstAgg(dimName)
  def doubleFirst(dim: Dim): DoubleFirstAgg        = doubleFirst(dim.name)

  def doubleLast(dimName: String): DoubleLastAgg = new DoubleLastAgg(dimName)
  def doubleLast(dim: Dim): DoubleLastAgg        = doubleLast(dim.name)

  def thetaSketch(dimName: String): ThetaSketchAgg = ThetaSketchAgg(dimName)
  def thetaSketch(dim: Dim): ThetaSketchAgg        = thetaSketch(dim.name)

  def hyperUnique(dimName: String): HyperUniqueAgg = HyperUniqueAgg(dimName)

  def hyperUnique(dim: Dim): HyperUniqueAgg = hyperUnique(dim.name)

  def cardinality(dims: Dim*): CardinalityAgg               = CardinalityAgg(dims)
  def cardinality(name: String, dims: Dim*): CardinalityAgg = CardinalityAgg(dims, Option(name))

  def inFiltered(dimName: String,
                 aggregator: AggregationExpression,
                 values: Iterable[String]): InFilteredAgg =
    InFilteredAgg(dimName, values.toSeq, aggregator.build())

  def inFiltered(dim: Dim,
                 aggregator: AggregationExpression,
                 values: Iterable[String]): InFilteredAgg =
    inFiltered(dim.name, aggregator, values.toSeq)

  def inFiltered(dimName: String,
                 aggregator: AggregationExpression,
                 first: String,
                 rest: String*): InFilteredAgg = {
    val values = first +: rest
    inFiltered(dimName, aggregator, values)
  }

  def inFiltered(dim: Dim,
                 aggregator: AggregationExpression,
                 first: String,
                 rest: String*): InFilteredAgg = {
    val values = first +: rest
    inFiltered(dim.name, aggregator, values)
  }

  def selectorFiltered(dimName: String,
                       aggregator: AggregationExpression,
                       value: String): SelectorFilteredAgg =
    SelectorFilteredAgg(dimName, Option(value), aggregator.build())

  def selectorFiltered(dim: Dim,
                       aggregator: AggregationExpression,
                       value: String): SelectorFilteredAgg =
    selectorFiltered(dim.name, aggregator, value)

  def selectorFiltered(dimName: String, aggregator: AggregationExpression): SelectorFilteredAgg =
    SelectorFilteredAgg(dimName, None, aggregator.build())

  def selectorFiltered(dim: Dim, aggregator: AggregationExpression): SelectorFilteredAgg =
    SelectorFilteredAgg(dim.name, None, aggregator.build())

  def count: CountAgg = new CountAgg()
}

trait FilteringExpressionOps {

  def not(op: FilteringExpression): FilteringExpression = op match {
    case neg: Not => neg.op
    case _        => new Not(op)
  }

  def disjunction(others: FilteringExpression*): FilteringExpression = new Or(others)

  def conjunction(others: FilteringExpression*): FilteringExpression = new And(others)

  def filter(value: FilteringExpression): FilteringExpression = new FilterOnlyOperator {
    override protected[dql] def createFilter: Filter = value.createFilter
  }
}

trait ExtractionFnOps {
  def extract(dim: Dim, fn: ExtractionFn): Dim        = dim.extract(fn)
  def extract(dimName: String, fn: ExtractionFn): Dim = Dim(dimName, extractionFnOpt = Option(fn))
  def extract(dim: Symbol, fn: ExtractionFn): Dim     = Dim(dim.name, extractionFnOpt = Option(fn))
}

trait PostAggregationOps {
  def hyperUniqueCardinality(fieldName: String): PostAggregationExpression =
    HyperUniqueCardinalityPostAgg(fieldName)

  def hyperUniqueCardinality(dim: Dim): PostAggregationExpression =
    HyperUniqueCardinalityPostAgg(dim.name, dim.outputNameOpt)
}

object AggregationOps         extends AggregationOps
object FilteringExpressionOps extends FilteringExpressionOps
object ExtractionFnOps        extends ExtractionFnOps
object PostAggregationOps     extends PostAggregationOps
