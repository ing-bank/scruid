[![CircleCI](https://circleci.com/gh/ing-bank/scruid.svg?style=svg)](https://circleci.com/gh/ing-bank/scruid)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/9b7c4adf8ad447efa9c7ea8a9ffda6b2)](https://www.codacy.com/app/fokko/scruid?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=ing-bank/scruid&amp;utm_campaign=Badge_Grade)
[![Download](https://api.bintray.com/packages/ing-bank/maven-releases/scruid/images/download.svg)](https://bintray.com/ing-bank/maven-releases/scruid/_latestVersion)

# Scruid

Scruid (Scala+Druid) is an open source library that allows you to compose queries easily in Scala. The library will take care of the translation of the query into json, parse the result in the case class that you define.

Currently the API is under heavy development, so changes might occur.



## Example queries:

Scruid provides three query consructors: `TopNQuery`, `GroupByQuery` and `TimeSeriesQuery` (see below for details). You can call the `execute` method ona query to send the query to Druid. This will return a `Future[DruidResponse]`. This response contains the [Circe](http://circe.io) JSON data without having it parsed to a specific case class yet. To interpret this JSON data you can run two methods on a `DruidResponse`:

- `.list[T](implicit decoder: Decoder[T]): List[T]` : This decodes the JSON to a list with items of type `T`.
- `.series[T](implicit decoder: Decoder[T]): Map[ZonedDateTime, T]` : This decodes the JSON to a timeseries map with the timestamp as key and `T` as value.

Below the example queries supported by Scruid. For more information about how to query Druid, and what query to pick, please refer to the [Druid documentation](http://druid.io/docs/latest/querying/querying.html)

### TopN query
```scala
case class TopCountry(count: Int, countryName: String = null)

val response = TopNQuery(
  dimension = Dimension(
    dimension = "countryName"
  ),
  threshold = 5,
  metric = "count",
  aggregations = List(
    CountAggregation(name = "count")
  ),
  intervals = List("2011-06-01/2017-06-01")
).execute

val result: Map[ZonedDateTime, List[TopCountry]] = response.series[List[TopCountry]]
```


### GroupBy query

```scala
case class GroupByIsAnonymous(isAnonymous: Boolean, count: Int)

val response = GroupByQuery(
  aggregations = List(
    CountAggregation(name = "count")
  ),
  dimensions = List("isAnonymous"),
  intervals = List("2011-06-01/2017-06-01")
).execute()

val result: List[GroupByIsAnonymous] = response.list[GroupByIsAnonymous]
```

The returned `Future[DruidResponse]` will contain json data where `isAnonymouse` is either `true or false`. Please keep in mind that Druid is only able to handle strings, and recently also numerics. So Druid will be returning a string, and the conversion from a string to a boolean is done by the json parser.

### TimeSeries query

```scala
case class TimeseriesCount(count: Int)

val response = TimeSeriesQuery(
  aggregations = List(
    CountAggregation(name = "count")
  ),
  granularity = "hour",
  intervals = List("2011-06-01/2017-06-01")
).execute

val series: Map[ZonedDateTime, TimeseriesCount] = response.series[TimeseriesCount]
```

To get the timeseries data from this `Future[DruidRespones]` you can run `val series = result.series[TimeseriesCount]`.

## Configuration

The configuration is done by [Typesafe config](https://github.com/typesafehub/config). The configuration can be overriden by using environment variables, e.g. `DRUID_HOST`, `DRUID_PORT` and `DRUID_DATASOURCE`. Or by placing an application.conf in your own project and this will override the reference.conf of the scruid library.

```
druid = {
  host = "localhost"
  host = ${?DRUID_HOST}
  port = 8082
  port = ${?DRUID_PORT}
  secure = false
  secure = ${?DRUID_USE_SECURE_CONNECTION}
  url = "/druid/v2/"
  url = ${?DRUID_URL}

  datasource = "wikiticker"
  datasource = ${?DRUID_DATASOURCE}

  connectionTimeout = 10000
  readTimeout = 30000
}
```

## Tests

To run the tests, please make sure that you have the Druid instance running:

```
docker run --rm -i -p 8082:8082 -p 8081:8081 fokkodriesprong/docker-druid
```
