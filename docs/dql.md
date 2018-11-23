# Druid query language (DQL)

Scruid provides a rich Scala API for building queries using the fluent pattern.

In order to use DQL, you have to import `ing.wbaa.druid.dql.DSL._` and thereafter build a query using the `DQL` query
builder. The type of the query can be time-series (default), group-by or top-n.

For all three types of queries you can define the following:

 - The datasource name to perform the query, or defaults to the one that has been defined in the configuration.
 - The granularity of the query, e.g., `Hour`, `Day`, `Week`, etc (default is `Week` for time-series and `All`
 for top-n and group-by).
 - The interval of the query, expressed as [ISO-8601 intervals](https://en.wikipedia.org/wiki/ISO_8601).
 - Filter dimensions
 - Aggregations and post-aggregations

For example, consider the following fragment of a DQL query:

```scala
import ing.wbaa.druid.definitions.GranularityType
import ing.wbaa.druid.dql.DSL._

val query = DQL
  .from("wikiticker")
  .granularity(GranularityType.Hour)
  .interval("2011-06-01/2017-06-01")
  .where('countryName === "Italy" or 'countryName === "Greece")
  .agg(count as "agg_count")
  .postAgg(('agg_count / 2) as "halfCount")
  //...
```

Function `from` defines the datasource to use, `granularity` defines the granularity of the query, `interval` defines the
temporal interval of the data expressed in ISO-8601, `where` defines which rows of data should be included in the
computation for a query, `agg` defines functions that summarize data (e.g., count of rows) and `postAgg` defines
specifications of processing that should happen on aggregated values.

In the above example we are performing a query over the datasource `wikiticker`, using hourly granularity, for the
interval `2011-06-01` until `2017-06-01`. We are considering rows of data where the value of dimension `countryName`
is either `Italy` or `Greece`. Furthermore, we are interested in half counting the rows. To achieve that we define
the aggregation function `count` we name it as `agg_count` and thereafter we define a post-aggregation function named
as `halfCount` that takes the result of `agg_count` and divides it by `2`.

The equivalent fragment of a Druid query expressed in JSON is given below:

```
{
  "dataSource" : "wikiticker",
  "granularity" : "hour",
  "intervals" : [ "2011-06-01/2017-06-01"],
  "filter" : {
      "fields" : [
        {
          "dimension" : "countryName",
          "value" : "Italy",
          "type" : "selector"
        },
        {
          "dimension" : "countryName",
          "value" : "Greece",
          "type" : "selector"
        }
      ],
      "type" : "or"
   },
  "aggregations" : [
    {
      "name" : "agg_count",
      "type" : "count"
    }
  ],
  "postAggregations" : [
    {
      "name" : "halfCount",
      "fn" : "/",
      "fields" : [
        {
          "name" : "agg_count",
          "fieldName" : "agg_count",
          "type" : "fieldAccess"
        },
        {
          "name" : "c_2.0",
          "value" : 2.0,
          "type" : "constant"
        }
      ],
      "type" : "arithmetic"
    }
  ]
}
```

Dimensions can be represented using Scala symbols, e.g., \`countryName, or by using function `dim`,
e.g., `dim("countryName")` or as a String prefix function symbol `d`, e.g., `d"countryName"`. In the latter case
it is possible to pass a string with variables in order to perform string interpolation, for example:

```
val prefix = "country"

DQL.where(d"${prefix}Name" === "Greece")
```

### Operators

In `where` function you can define the following operators to filter the rows of data in a query.


#### Equals

| Example                                    | Description                                                             |
|--------------------------------------------|-------------------------------------------------------------------------|
| `'countryName === "Greece"`                | the value of dimension `countryName` equals to "Greece"                 |
| `'dim === 10`                              | the value of dimension `dim` equals to 10                               |
| `'dim === 10.1`                            | the value of dimension `dim` equals to 10.1                             |
| `'dim1 === 'dim2`                          | the values of dimensions `'dim1` and `'dim2` are equal                  |

#### Not equals

| Example                                    | Description                                                             |
|--------------------------------------------|-------------------------------------------------------------------------|
| `'countryName =!= "Greece"`                | the value of dimension `countryName` not equals to "Greece"             |
| `'dim =!= 10`                              | the value of dimension `dim` not equals to 10                           |
| `'dim =!= 10.1`                            | the value of dimension `dim` not equals to 10.1                         |
| `'dim1 =!= 'dim2`                          | the values of dimensions`'dim1` and `'dim2` are not equal               |


#### Regular expression

It matches the specified dimension with the given pattern. The pattern can be any standard [Java regular expression](https://docs.oracle.com/javase/6/docs/api/java/util/regex/Pattern.html).
For example, match a floating point number from a string:

```scala
'dim regex "\\d+(\\.\\d+)"
```

#### Like

Like operators can be used for basic wildcard searches. They are equivalent to the `SQL LIKE` operator. For example,
match all last names that start with character 'S'.

```scala
'lastName like "S%"
```

#### Search

Search operators can be used to filter on partial string matches. For example, for case sensitive search (default):

```scala
'dim contains "some string"

// which is equivalent to:
'dim contains("some string", caseSensitive = true)
```

Similarly, to ignore case sensitivity in search:

```scala
'dim containsIgnoreCase "some string"

// which is equivalent to:
'dim contains("some string", caseSensitive = false)
```

#### In

To express the `SQL IN` operator, in order to match the value of a dimension into any value of a specified set of values.
In the example below, the dimension `outlaw` matches to any of 'Good', 'Bad' or 'Ugly' values:

```scala
'outlaw in ("Good", "Bad", "Ugly")
```

We can easily express a negation of the `in` operator, by directly using the `notIn` operator.

```scala
'outlaw notIn ("Good", "Bad", "Ugly")
```

#### Bound

Bound operator can be used to filter on ranges of dimension values. It can be used for comparison filtering like
greater than, less than, greater than or equal to, less than or equal to, and "between" (if both "lower" and
"upper" are set). For details see the examples below.

When numbers are given to bound operators, then the ordering is numeric:

```scala
'dim > 10
'dim >= 10.0
'dim < 1.1
'dim <= 1

// 0.0 < dim < 10.0
'dim between(0.0, 10.0)

// 0.0 <= dim <= 10.0
'dim between(0.0, 10.0, lowerStrict = true, upperStrict = true)

// 0.0 <= dim < 10.0
'dim between(0.0, 10.0, lowerStrict = true, upperStrict = false)
```

When strings are given to bound operators, then the ordering is lexicographic:

```scala
'dim > "10"
'dim >= "10.0"
'dim < "1.1"
'dim <= "1"

// "0.0" < dim < "10.0"
'dim between("0.0", "10.0")

// "0.0" <= dim <= "10.0"
'dim between("0.0", "10.0", lowerStrict = true, upperStrict = true)

// "0.0" <= dim < "10.0"
'dim between("0.0", "10.0", lowerStrict = true, upperStrict = false)
```

Furthermore, you can specify any other ordering (e.g. Alphanumeric) or define some extraction function:

```
'dim > "10" withOrdering(DimensionOrderType.Alphanumeric)

// "0.0" < dim < "10.0"
'dim between("0.0", "10.0") withOrdering(DimensionOrderType.Alphanumeric)
```

```scala
'dim > "10" withOrdering(DimensionOrderType.Alphanumeric)

// "0.0" < dim < "10.0"
'dim between("0.0", "10.0") withOrdering(DimensionOrderType.Alphanumeric)

// apply some extraction function
'dim > "10" withExtractionFn(<some ExtractionFn>)
```

#### Interval

Interval operator performs range filtering on dimensions that contain long millisecond values,
with the boundaries specified as ISO 8601 time intervals.

```scala
'dim interval "2011-06-01/2012-06-01"

'dim interval("2011-06-01/2012-06-01", "2012-06-01/2013-06-01", ...)

'__time interval "2011-06-01/2012-06-01"

'__time interval("2011-06-01/2012-06-01", "2012-06-01/2013-06-01", ...)
```

#### Logical operators

###### AND

To define a logical `AND` between other operators you can use the operator `and`, for example:

```scala
('dim1 === 10) and ('dim2 interval "2011-06-01/2012-06-01") and ('dim3 === "foo")
```

Alternatively, you can use the function `conjunction`, as follows:

```scala
conjunction('dim1 === 10, 'dim2 interval "2011-06-01/2012-06-01", 'dim3 === "foo")
```

###### OR

To define a logical `OR` between other operators you can use the operator `or`, for example:

```scala
('dim1 === 10) or ('dim2 interval "2011-06-01/2012-06-01") or ('dim3 === "foo")
```

Alternatively, you can use the function `disjunction`, as follows:

```scala
disjunction('dim1 === 10, 'dim2 interval "2011-06-01/2012-06-01", 'dim3 === "foo")
```

###### NOT

To define a negation of an operator, you can use the operator `not`:

```scala
not('dim1 between (10, 100))
```

## Aggregations

Aggregations are functions that summarize data. To add one or more aggregation functions in a DQL query you can
define aggregations as multiple arguments to the `agg` function of the builder, or alternatively call `agg` function
multiple times.

```scala
val query = DQL
    .from("some_data_source_name")
    .agg(aggregation0, aggregation1, aggregation2, ...)
    ...

// or using multiple agg calls:
val query = DQL
    .from("some_data_source_name")
    .agg(aggregation0)
    .agg(aggregation1)
    .agg(aggregation2)
    ...

// or a combination
val query = DQL
    .from("some_data_source_name")
    .agg(aggregation0, aggregation1)
    .agg(aggregation2)
    ...
```

The available aggregators are outlined below:

#### Count aggregator

`count` computes the count of Druid rows that match the filters.

```scala
count // uses the default name "count"

count as "some_count" // uses the name "some_count"
```

#### Sum aggregators

`longSum` and `doubleSum` computes the sum of values as a 64-bit signed integer or floating point value, respectively.

```scala
// can be defined over some dimension
'dim_name.longSum as "agg_sum"

// or as function
doubleSum('dim_name) as "agg_sum"
```

#### Min / Max aggregators

`longMin` and `doubleMin` computes the minimum of all metric values and Long.MAX_VALUE
or Double.POSITIVE_INFINITY, respectively.

```scala
// can be defined over some dimension
'dim_name.longMin as "agg_min"

// or as function
doubleMin('dim_name) as "agg_min"
```

Similarly, `longMax` and `doubleMax` computes the maximum of all metric values and Long.MIN_VALUE
or Double.NEGATIVE_INFINITY, respectively.


#### First / Last aggregator

`longFirst` and `doubleFirst` computes the metric value with the minimum timestamp or 0 if no row exist.
`longLast` and `doubleLast` computes the metric value with the maximum timestamp or 0 if no row exist

```scala
// can be defined over some dimension
'dim_name.longFirst as "agg_first"

// or as function
doubleLast('dim_name) as "agg_last"
```

#### Approximate Aggregations

DQL supports `thetaSketch` and `hyperUnique` approximate aggregators.

```scala
// can be defined over some dimension
'dim_name.thetaSketch as "agg_theta"

// or as function
hyperUnique('dim_name) as "agg_hyper_unique"

// can also set additional parameters

thetaSketch('dim_name).set(isInputThetaSketch = true, size = 32768) as "agg_theta"

'dim_name.hyperUnique.set(isInputHyperUnique = true, isRound = true) as "agg_hyper_unique"
```

#### Filtered Aggregator


`inFiltered` wraps any given aggregator, but only aggregates the values for which the given dimension the *in filter* matches
Similarly, `selectorFiltered` wraps any given aggregator and filters the values using the *selector filter*.


For example, the `inFiltered` below applies over the `longSum` aggregation, only for values
`#en.wikipedia` and `#de.wikipedia` of `channel` dimension:

```scala
'channel.inFiltered('count.longSum, "#en.wikipedia", "#de.wikipedia")

// Equivalent inFiltered as function
inFiltered('channel, 'count.longSum, "#en.wikipedia", "#de.wikipedia")
```

Similarly, the `selectorFiltered` below applies over the `longSum` aggregation, only for value `"#en.wikipedia"` of
`channel` dimension:

```scala
'channel.selectorFiltered('count.longSum, "#en.wikipedia")

// Equivalent selectorFiltered as function
selectorFiltered('channel, 'count.longSum, "#en.wikipedia")

```

## Post-aggregations

Post-aggregations are specifications of processing that should happen on aggregated values as they come out of Druid.
To add one or more post-aggregation functions in a DQL query you can define post-aggregation as multiple arguments to
the `postAgg` function of the builder, or alternatively call `postAgg` function  multiple times.

```scala
val query = DQL
    .from("some_data_source_name")
    .agg(...)
    .postAgg(post-aggregation0, post-aggregation1, post-aggregation2, ...)
    ...

// or using multiple postAgg calls:
val query = DQL
    .from("some_data_source_name")
    .agg(...)
    .postAgg(post-aggregation0)
    .postAgg(post-aggregation1)
    .postAgg(post-aggregation2)
    ...

// or a combination
val query = DQL
    .from("some_data_source_name")
    .agg(...)
    .postAgg(post-aggregation0, post-aggregation1)
    .postAgg(post-aggregation2)
    ...
```

#### Arithmetic post-aggregator

Arithmetic post-aggregator can be applied to aggregators or other post aggregators and
the supported functions are `+`, `-`, `*`, `/`, and `quotient`.

```scala
'dim_name + 2

('count / 2) as "halfCount"
```

In arithmetic post-aggregators you can specify an ordering (e.g., NumericFirst) of the results (this can be useful
for topN queries). By default, Druid uses *floating point* ordering. You can explicitly set the ordering to be
either *floating point* or *numeric first*. The latter ordering always returns finite values first, followed
by *NaN*, and *infinite values* last.

```scala
import ing.wbaa.druid.definitions.Ordering

// Set explicitly 'floating point' ordering:
('count / 2).withOrdering(Ordering.FloatingPoint) as "halfCount"

// or equivalently:
('count / 2).floatingPointOrdering as "halfCount"

// Set numeric first ordering:
('count / 2).withOrdering(Ordering.NumericFirst) as "halfCount"

// or equivalently:
('count / 2).numericFirstOrdering as "halfCount"
```

#### HyperUnique Cardinality post-aggregator

Is used to wrap a hyperUnique object such that it can be used in post aggregations.

```scala
'dim_name.hyperUniqueCardinality

// or alternatively as function:
hyperUniqueCardinality('dim_name)
```


## Extraction functions

Extraction functions define the transformation applied to each dimension value.

```scala
import ing.wbaa.druid.LowerExtractionFn

'countryName.extract(LowerExtractionFn()) as "country"

// or as function
extract('countryName, LowerExtractionFn()) as "country"
```

## Example queries

#### Time-series query

This is simplest form of query and takes all the common DQL parameters.

For example, the following query is a time-series that counts the number of rows by hour:

```scala
case class TimeseriesCount(ts_count: Long)

val query: TimeSeriesQuery = DQL
    .from("wikiticker")
    .granularity(GranularityType.Hour)
    .interval("2011-06-01/2017-06-01")
    .agg(count as "ts_count")
    .build()

val response: Future[List[GroupByIsAnonymous]] = query.execute().map(_.list[TimeseriesCount])
```

#### Top-N query

The following query computes the Top-5 `countryName` with respect to the aggregation `agg_count`.

```scala
case class PostAggregationAnonymous(countryName: Option[String], agg_count: Double, half_count: Double)

val query: TopNQuery = DQL
    .from("wikiticker")
    .granularity(GranularityType.Week)
    .interval("2011-06-01/2017-06-01")
    .agg(count as "agg_count")
    .postAgg(('agg_count / 2) as "half_count")
    .topN('countryName, metric = "agg_count", threshold = 5)
    .build()

val response: Future[List[PostAggregationAnonymous]] = query.execute().map(_.list[PostAggregationAnonymous])
```

#### Group-by query

The following query performs group-by count over the dimension `isAnonymous`:

```scala
case class GroupByIsAnonymous(isAnonymous: String, count: Int)

val query: GroupByQuery = DQL
    .from("wikiticker")
    .granularity(GranularityType.Day)
    .interval("2011-06-01/2017-06-01")
    .agg(count as "count")
    .groupBy('isAnonymous)
    .build()

val response: List[GroupByIsAnonymous] = query.execute().map(_.list[GroupByIsAnonymous])
```

A more advanced example of group-by count over the dimensions `isAnonymous` and `countryName`.
Here the `countryName` is being extracted to uppercase and the resulting dimension is named as `country`.
Furthermore, we are limiting the results to top-10 counts, therefore we define as numeric ordering
the aggregation `count`.

```scala
case class GroupByIsAnonymous(isAnonymous: String, country: Option[String], count: Int)

val query: GroupByQuery = DQL
    .from("wikiticker")
    .granularity(GranularityType.Day)
    .interval("2011-06-01/2017-06-01")
    .agg(count as "count")
    .groupBy('isAnonymous, 'countryName.extract(UpperExtractionFn()) as "country")
    .limit(10, 'count.desc(DimensionOrderType.Numeric))
    .build()

val response: Future[List[GroupByIsAnonymous]] = query.execute().map(_.list[GroupByIsAnonymous])
```

We can avoid null values in `country` by filtering the dimension `countryName`:

```scala
case class GroupByIsAnonymous(isAnonymous: String, country: String, count: Int)

val query: GroupByQuery = DQL
    .from("wikiticker")
    .granularity(GranularityType.Day)
    .interval("2011-06-01/2017-06-01")
    .agg(count as "count")
    .where('countryName.isNotNull)
    .groupBy('isAnonymous, 'countryName.extract(UpperExtractionFn()) as "country")
    .limit(10, 'count.desc(DimensionOrderType.Numeric))
    .build()

val response: Future[List[GroupByIsAnonymous]] = query.execute().map(_.list[GroupByIsAnonymous])
```

We can also keep only those records that they are having count above 100 and below 200:

```scala
case class GroupByIsAnonymous(isAnonymous: String, country: String, count: Int)

val query: GroupByQuery = DQL
    .from("wikiticker")
    .granularity(GranularityType.Day)
    .interval("2011-06-01/2017-06-01")
    .agg(count as "count")
    .where('countryName.isNotNull)
    .groupBy('isAnonymous, 'countryName.extract(UpperExtractionFn()) as "country")
    .having('count > 100 and 'count < 200)
    .limit(10, 'count.desc(DimensionOrderType.Numeric))
    .build()

val response: Future[List[GroupByIsAnonymous]] = query.execute().map(_.list[GroupByIsAnonymous])
```


