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

package com.ing.wbaa.druid.dql

import com.ing.wbaa.druid._
import com.ing.wbaa.druid.definitions._
import com.ing.wbaa.druid.definitions.QueryContext.{ QueryContextParam, QueryContextValue }
import com.ing.wbaa.druid.dql.expressions._

// scalastyle:off var.field

/**
  * Collection of common functions for all query builders
  */
private[dql] sealed trait QueryBuilderCommons {

  protected var queryContextParams              = Map.empty[QueryContextParam, QueryContextValue]
  protected var dataSourceOpt                   = Option.empty[String]
  protected var granularityOpt                  = Option.empty[Granularity]
  protected var aggregations: List[Aggregation] = Nil

  protected var complexAggregationNames: Set[String] = Set.empty[String]

  protected var intervals: List[String] = Nil

  protected var filters: List[Filter] = Nil

  protected var postAggregationExpr: List[PostAggregationExpression] = Nil

  def withQueryContext(params: Map[QueryContextParam, QueryContextValue]): this.type = {
    queryContextParams = params
    this
  }

  def setQueryContextParam(key: QueryContextParam, value: QueryContextValue): this.type = {
    queryContextParams += (key -> value)
    this
  }

  /**
    * Specify the datasource to use, other than the default one in the configuration
    */
  def from(dataSource: String): this.type = {
    dataSourceOpt = Option(dataSource)
    this
  }

  /**
    * Define the granularity to bucket query results
    */
  def granularity(granularity: Granularity): this.type = {
    granularityOpt = Option(granularity)
    this
  }

  /**
    * Specify the time range to run the query over (should be expressed as ISO-8601 interval)
    */
  def interval(interval: String): this.type = {
    intervals = interval :: intervals
    this
  }

  /**
    * Specify multiple time ranges to run the query over (should be expressed as ISO-8601 intervals)
    */
  def intervals(ints: String*): this.type = {
    intervals ++= ints.toList
    this
  }

  /**
    * Specify one or more filter operations to use in the query
    */
  def where(filter: FilteringExpression): this.type = {

    filters = filter.asFilter :: filters
    this
  }

  protected def getFilters: Option[Filter] =
    if (filters.isEmpty) None
    else if (filters.size == 1) Option(filters.head)
    else Option(AndFilter(filters))

  protected def copyTo[T <: QueryBuilderCommons](other: T): T = {
    other.queryContextParams = queryContextParams
    other.dataSourceOpt = dataSourceOpt
    other.granularityOpt = granularityOpt
    other.aggregations = aggregations
    other.complexAggregationNames = complexAggregationNames
    other.intervals = intervals
    other.filters = filters
    other.postAggregationExpr = postAggregationExpr
    other
  }

  protected def getPostAggs: List[PostAggregation] =
    postAggregationExpr.map(expr => expr.build(complexAggregationNames))

}

private[dql] sealed trait AggregationQueryBuilderCommons {
  this: QueryBuilderCommons =>

  /**
    * Specify one or more aggregations to use
    */
  def agg(aggs: AggregationExpression*): this.type = {

    complexAggregationNames ++= aggs.filter(_.isComplex).map(_.getName)
    aggregations = aggs.foldRight(aggregations)((agg, acc) => agg.build() :: acc)

    this
  }

  /**
    * Specify one or more post-aggregations to use
    */
  def postAgg(postAggs: PostAggregationExpression*): this.type = {
    postAggregationExpr = postAggs.foldRight(postAggregationExpr)((agg, acc) => agg :: acc)
    this
  }

}

trait DescendingOption {
  this: QueryBuilderCommons =>

  // Default is false (ascending)
  protected var descending = false

  /**
    * Define whether to make descending ordered result
    */
  def setDescending(v: Boolean): this.type = {
    descending = v
    this
  }
}

/**
  * This is the default query build, in order to create Timeseries queries
  */
final class QueryBuilder private[dql] ()
    extends QueryBuilderCommons
    with AggregationQueryBuilderCommons
    with DescendingOption {

  /**
    * Gives the resulting time-series query, wrt. the given query parameters (e.g., where, datasource, etc.)
    *
    * @return the resulting time-series query
    */
  @deprecated(message = "use timeseries.build()", since = "2.4.0")
  def build()(implicit druidConfig: DruidConfig = DruidConfig.DefaultConfig): TimeSeriesQuery =
    timeseries.build()

  /**
    * Define that the query will be a timeseries query
    * @return the builder for timeseries
    */
  def timeseries: TimeseriesQueryBuilder = copyTo(new TimeseriesQueryBuilder)

  /**
    * Define that the query will be a top-n query
    *
    * @param dimension the dimension that you want the top taken for
    * @param metric the metric to sort by for the top list
    * @param threshold an integer defining the N in the top-n
    *
    * @return the builder for top-n queries
    */
  def topN(dimension: Dim, metric: String, threshold: Int): TopNQueryBuilder =
    copyTo(new TopNQueryBuilder(dimension, metric, threshold))

  /**
    * Define that the query will be a group-by query
    *
    * @param dimensions the dimensions to perform a group-by query
    * @return the builder for group-by queries
    */
  def groupBy(dimensions: Dim*): GroupByQueryBuilder =
    copyTo(new GroupByQueryBuilder(dimensions))

  def groupBy(dimensions: Iterable[Dim]): GroupByQueryBuilder =
    copyTo(new GroupByQueryBuilder(dimensions))

  def scan(): ScanQueryBuilder = copyTo(new ScanQueryBuilder())

  def search(q: SearchQuerySpec): SearchQueryBuilder = copyTo(new SearchQueryBuilder(q))
}

final class TimeseriesQueryBuilder private[dql] ()
    extends QueryBuilderCommons
    with AggregationQueryBuilderCommons
    with DescendingOption {

  /**
    * Gives the resulting time-series query, wrt. the given query parameters (e.g., where, datasource, etc.)
    *
    * @return the resulting time-series query
    */
  def build()(implicit druidConfig: DruidConfig = DruidConfig.DefaultConfig): TimeSeriesQuery = {

    val conf = dataSourceOpt
      .map(ds => druidConfig.copy(datasource = ds))
      .getOrElse(druidConfig)

    TimeSeriesQuery(
      aggregations = this.aggregations,
      intervals = this.intervals,
      filter = this.getFilters,
      granularity = this.granularityOpt.getOrElse(GranularityType.Week),
      descending = this.descending.toString,
      postAggregations = this.getPostAggs,
      context = this.queryContextParams
    )(conf)
  }
}

/**
  * Builder for top-n queries
  *
  * @param dimension the dimension that you want the top taken for
  * @param metric the metric to sort by for the top list
  * @param n an integer defining the N in the top-n
  */
final class TopNQueryBuilder private[dql] (dimension: Dim, metric: String, n: Int)
    extends QueryBuilderCommons
    with AggregationQueryBuilderCommons
    with DescendingOption {

  /**
    * Gives the resulting top-n query, wrt. the given query parameters (e.g., where, datasource, etc.)
    *
    * @return the resulting top-n query
    */
  def build()(implicit druidConfig: DruidConfig = DruidConfig.DefaultConfig): TopNQuery = {

    val conf = dataSourceOpt
      .map(ds => druidConfig.copy(datasource = ds))
      .getOrElse(druidConfig)

    TopNQuery(
      dimension = this.dimension.build(),
      threshold = n,
      metric = metric,
      aggregations = this.aggregations,
      intervals = this.intervals,
      granularity = this.granularityOpt.getOrElse(GranularityType.All),
      filter = this.getFilters,
      postAggregations = this.getPostAggs,
      context = this.queryContextParams
    )(conf)
  }

}

/**
  * Builder for group-by queries
  *
  * @param dimensions the dimensions to perform a group-by query
  */
final class GroupByQueryBuilder private[dql] (dimensions: Iterable[Dim])
    extends QueryBuilderCommons
    with AggregationQueryBuilderCommons {

  protected var limitOpt                        = Option.empty[Int]
  protected var limitCols                       = Iterable.empty[OrderByColumnSpec]
  protected var havingExpressions: List[Having] = Nil

  protected var excludeNullsOpt = Option.empty[Boolean]

  /**
    * Specify which rows from a group-by query should be returned, by specifying conditions on aggregated values
    */
  def having(conditions: FilteringExpression): this.type = {
    havingExpressions = conditions.asHaving :: havingExpressions
    this
  }

  /**
    * Sort and limit the set of results of the group-by query
    *
    * @param n the upper limit of the number of results
    * @param direction specify the order of the results (i.e., ascending or descending) for all group-by dimensions
    * @param dimensionOrderType specify the type of the ordering (lexicographic, alphanumeric, etc.)
    *                           for all group-by dimensions. Default is lexicographic.
    */
  def limit(
      n: Int,
      direction: Direction,
      dimensionOrderType: DimensionOrderType = DimensionOrderType.Lexicographic
  ): this.type = {
    limitOpt = Option(n)
    limitCols = dimensions.map(
      dim => OrderByColumnSpec(dim.name, direction, DimensionOrder(dimensionOrderType))
    )
    this
  }

  /**
    * Sort and limit the set of results of the group-by query
    *
    * @param n the upper limit of the number of results
    * @param cols specify the ordering per group-by dimension
    */
  def limit(n: Int, cols: OrderByColumnSpec*): this.type = {
    limitOpt = Option(n)
    limitCols = cols
    this
  }

  /**
    * Specify whether or not to exclude null for all group-by dimensions
    */
  def setExcludeNulls(v: Boolean): this.type = {
    excludeNullsOpt = Option(v)
    this
  }

  /**
    * Gives the resulting group-by query, wrt. the given query parameters (e.g., where, datasource, etc.)
    *
    * @return the resulting group-by query
    */
  def build()(implicit druidConfig: DruidConfig = DruidConfig.DefaultConfig): GroupByQuery = {

    val havingOpt =
      if (havingExpressions.isEmpty) None
      else if (havingExpressions.size == 1) Option(havingExpressions.head)
      else Option(definitions.AndHaving(havingExpressions))

    val limitSpecOpt =
      limitOpt.map { n =>
        if (limitCols.nonEmpty) LimitSpec(limit = n, columns = limitCols)
        else LimitSpec(limit = n, columns = dimensions.map(dim => OrderByColumnSpec(dim.name)))
      }

    // when excludeNullsOpt is Some(true)
    // then set not null filtering for all dimensions
    if (excludeNullsOpt.contains(true)) {
      val excludeNullsExpressions = dimensions.map(dim => new Not(new NullDim(dim)))

      this.where(new And(excludeNullsExpressions))
    }

    val conf = dataSourceOpt
      .map(ds => druidConfig.copy(datasource = ds))
      .getOrElse(druidConfig)

    GroupByQuery(
      aggregations = this.aggregations,
      intervals = this.intervals,
      filter = this.getFilters,
      dimensions = this.dimensions.map(_.build()),
      granularity = this.granularityOpt.getOrElse(GranularityType.All),
      having = havingOpt,
      limitSpec = limitSpecOpt,
      postAggregations = this.getPostAggs,
      context = this.queryContextParams
    )(conf)
  }
}

final class ScanQueryBuilder private[dql] () extends QueryBuilderCommons {

  private var columns: List[String]     = Nil
  private var limitOpt: Option[Int]     = None
  private var batchSizeOpt: Option[Int] = None
  private var order: Order              = OrderType.None

  def columns(cols: String*): this.type = this.columns(cols)

  def columns(cols: Iterable[String]): this.type = {
    columns = cols.foldRight(columns)((col, acc) => col :: acc)
    this
  }

  def limit(lim: Int): this.type = {
    limitOpt = Option(lim)
    this
  }

  def batchSize(size: Int): this.type = {
    batchSizeOpt = Option(size)
    this
  }

  def order(ord: Order): this.type = {
    order = ord
    this
  }

  def build()(implicit druidConfig: DruidConfig = DruidConfig.DefaultConfig): ScanQuery = {
    val conf = dataSourceOpt
      .map(ds => druidConfig.copy(datasource = ds))
      .getOrElse(druidConfig)

    ScanQuery(
      granularity = this.granularityOpt.getOrElse(GranularityType.All),
      intervals = this.intervals,
      filter = this.getFilters,
      columns = this.columns,
      batchSize = this.batchSizeOpt,
      limit = this.limitOpt,
      order = this.order,
      context = this.queryContextParams
    )(conf)
  }
}

final class SearchQueryBuilder private[dql] (query: SearchQuerySpec) extends QueryBuilderCommons {

  private var sortOpt: Option[DimensionOrderType] = None
  private var limitOpt: Option[Int]               = None
  private var dims: List[String]                  = Nil

  def sort(v: DimensionOrderType): this.type = {
    sortOpt = Option(v)
    this
  }

  def limit(lim: Int): this.type = {
    limitOpt = Option(lim)
    this
  }

  def dimensions(dimNames: String*): this.type = {
    dims = dimNames.foldRight(dims)((dim, acc) => dim :: acc)
    this
  }

  def dimensions(dimNames: Iterable[String]): this.type = {
    dims = dimNames.foldRight(dims)((dim, acc) => dim :: acc)
    this
  }

  def build()(implicit druidConfig: DruidConfig = DruidConfig.DefaultConfig): SearchQuery = {
    val conf = dataSourceOpt
      .map(ds => druidConfig.copy(datasource = ds))
      .getOrElse(druidConfig)

    SearchQuery(
      granularity = this.granularityOpt.getOrElse(GranularityType.All),
      intervals = this.intervals,
      query = this.query,
      filter = this.getFilters,
      limit = this.limitOpt,
      searchDimensions = this.dims,
      sort = this.sortOpt.map(DimensionOrder),
      context = this.queryContextParams
    )(conf)
  }
}

// scalastyle:on var.field
