[![CircleCI](https://circleci.com/gh/ing-bank/scruid.svg?style=svg)](https://circleci.com/gh/ing-bank/scruid)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/9b7c4adf8ad447efa9c7ea8a9ffda6b2)](https://www.codacy.com/app/fokko/scruid?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=ing-bank/scruid&amp;utm_campaign=Badge_Grade)
[![Download](https://api.bintray.com/packages/ing-bank/maven-releases/scruid/images/download.svg)](https://bintray.com/ing-bank/maven-releases/scruid/_latestVersion)

# Scruid

Scruid (Scala+Druid) is an open source library that allows you to compose queries easily in Scala. The library will take care of the translation of the query into json, parse the result in the case class that you define.

Currently the API is under heavy development, so changes might occur.

## Example queries:

Below the example queries supported by Scruid. For more information about how to query Druid, and what query to pick, please refer to the [Druid documentation](http://druid.io/docs/latest/querying/querying.html)

### TopN query
```scala
case class TopCountry(count: Int, countryName: String = null)

TopNQuery[TopCountry](
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
```
This will return a `List[TopCountry]`.

### GroupBy query

```scala
case class GroupByIsAnonymous(isAnonymous: Boolean, count: Int)

val result = GroupByQuery[GroupByIsAnonymous](
  aggregations = List(
    CountAggregation(name = "count")
  ),
  dimensions = List("isAnonymous"),
  intervals = List("2011-06-01/2017-06-01")
).execute()
```

This will return `List[GroupByIsAnonymous]` where `isAnonymouse` is either `true or false`. Please keep in mind that Druid is only able to handle strings, and recently also numerics. So Druid will be returning a string, and the conversion from a string to a boolean is done by the json parser.

### TimeSeries query

```scala
case class TimeseriesCount(count: Int)

val result = TimeSeriesQuery[TimeseriesCount](
  aggregations = List(
    CountAggregation(name = "count")
  ),
  granularity = "hour",
  intervals = List("2011-06-01/2017-06-01")
).execute
```

This will return a `List[TimeSeriesResult[TimeseriesCount]]`. Where the `TimeSeriesResult` contains the timestamp of the interval in joda time, and the `TimeseriesCount` class contains the actual result of the query as defined in the aggregation.

## Configuration

The configuration is done by [Typesafe config](https://github.com/typesafehub/config). The configuration can be overriden by using environment variables, e.g. `DRUID_URL` and `DRUID_DATASOURCE`. Or by placing an application.conf in your own project and this will override the reference.conf of the scruid library.

```
druid = {
  url = "http://localhost:8082/druid/v2/"
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
