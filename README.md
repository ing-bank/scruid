[![CircleCI](https://circleci.com/gh/ing-bank/scruid.svg?style=svg)](https://circleci.com/gh/ing-bank/scruid)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/9b7c4adf8ad447efa9c7ea8a9ffda6b2)](https://www.codacy.com/app/fokko/scruid?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=ing-bank/scruid&amp;utm_campaign=Badge_Grade)
[![Download](https://api.bintray.com/packages/ing-bank/maven-releases/scruid/images/download.svg)](https://bintray.com/ing-bank/maven-releases/scruid/_latestVersion)

# Scruid

Scruid (Scala+Druid) is an open source library that allows you to compose queries easily in Scala. The library will take care of the translation of the query into json, parse the result in the case class that you define.

Currently the API is under heavy development, so changes might occur.

For example:
```scala
case class TopCountry(count: Int, countryName: String = null)

TopNQuery[TopCountry](
  dimension = Dimension(
    dimension = "countryName"
  ),
  threshold = 5,
  metric = "count",
  aggregations = List(
    Aggregation(
      kind = "count",
      name = "count",
      fieldName = "count"
    )
  ),
  intervals = List("2011-06-01/2017-06-01")
).execute
```
This will return a `List[TopCountry]`.

## Tests

To run the tests, please make sure that you have the Druid instance running:

```
docker run --rm -i -p 8082:8082 -p 8081:8081 fokkodriesprong/docker-druid
```
