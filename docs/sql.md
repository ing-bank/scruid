# Druid SQL

Scruid provides querying via [Druid SQL](https://druid.apache.org/docs/latest/querying/sql.html) by using the 
processed string function `dsql`. 

```scala
import scala.concurrent.Future
import ing.wbaa.druid.SQL._
import ing.wbaa.druid.DruidSQLResults

val query = dsql"""SELECT COUNT(*) as "count" FROM wikipedia WHERE "__time" >= TIMESTAMP '2015-09-12 00:00:00'"""

val response: Future[DruidSQLResults] = query.execute()

// The resulting response can also mapped to a case class
case class Result(count: Double)

val result: Future[Result] = response.map(_.list[Result])
```

Function `sql`, allows multiline queries:

```scala
import ing.wbaa.druid.SQL._

val query = dsql"""
    |SELECT COUNT(*) as "count" 
    |FROM wikipedia 
    |WHERE "__time" >= TIMESTAMP '2015-09-12 00:00:00'
    """.stripMargin
```

### Parameterized Queries

Supports [Druid SQL Parameterized Queries](https://druid.apache.org/docs/latest/querying/sql.html#http-post):

For example, we would like to query for a specific range of time which is expressed using LocalDateTime instances and
filter for a specific country ISO code.

```scala
import java.time.LocalDateTime
import ing.wbaa.druid.SQLQuery

import ing.wbaa.druid.SQL._

val fromDateTime: LocalDateTime = LocalDateTime.of(2015, 9, 12, 0, 0, 0, 0)
val untilDateTime: LocalDateTime = fromDateTime.plusDays(1)
val countryIsoCode: String = "US"

val query: SQLQuery =
  dsql"""
  |SELECT FLOOR(__time to HOUR) AS hourTime, count(*) AS "count"
  |FROM wikipedia
  |WHERE "__time" BETWEEN ${fromDateTime} AND ${untilDateTime} AND countryIsoCode = ${countryIsoCode}
  |GROUP BY 1
  |""".stripMargin
```

Any variable or expression injected into a query gets turned into a parameter. It is not inserted directly into 
the query string and therefore there is no danger of SQL injection attacks. 

For example, the query above is a parameterized query and will be represented by the following JSON request:

```json
{
  "query" : "\nSELECT FLOOR(__time to HOUR) AS hourTime, count(*) AS \"count\"\nFROM wikipedia\nWHERE \"__time\" BETWEEN ? AND ? AND countryIsoCode = ?\nGROUP BY 1\n",
  "context" : {

  },
  "parameters" : [
    {
      "type" : "TIMESTAMP",
      "value" : "2015-09-12 00:00:00"
    },
    {
      "type" : "TIMESTAMP",
      "value" : "2015-09-13 00:00:00"
    },
    {
      "type" : "VARCHAR",
      "value" : "US"
    }
  ],
  "resultFormat" : "object"
}
```

Each interpolated variable has been replaced by the symbol `?` and added to the list of `parameters` with 
its corresponding `type` and format. For instance, the variables `fromDateTime` and `untilDateTime` are 
instances of `LocalDateTime` and appear as types of SQL `TIMESTAMP` with values in formatted as `y-MM-dd HH:mm:ss`.

`dsql` supports the following types:

| Scala type              | Druid SQL type |
|-------------------------|----------------|
| Char                    | CHAR           |
| String                  | VARCHAR        |
| Byte                    | TINYINT        |
| Short                   | SMALLINT       |
| Int                     | INTEGER        |
| Long                    | BIGINT         |
| Float                   | FLOAT          |
| Double                  | DOUBLE         |
| Boolean                 | BOOLEAN        |
| java.time.LocalDate     | DATE           |
| java.time.LocalDateTime | TIMESTAMP      |
| java.sql.Timestamp      | TIMESTAMP      |


### Context parameters

SQL queries can also parameterized with context parameters:

```scala
import ing.wbaa.druid.SQL._
import ing.wbaa.druid.definitions.QueryContext

val contextParameters = Map(
  QueryContext.SqlQueryId -> "scruid-sql-example-query",
  QueryContext.SqlTimeZone -> "America/Los_Angeles"
)
val query =
  dsql"""SELECT COUNT(*) as "count" FROM wikipedia WHERE "__time" >= TIMESTAMP '2015-09-12 00:00:00'"""
      .setContext(contextParameters)
```

### Handling large payloads with Akka Streams

SQL queries in Scruid support streaming using the `stream` and `streamAs` functions.

```scala
import akka.NotUsed
import akka.stream.scaladsl.Source
import ing.wbaa.druid.SQL._
import ing.wbaa.druid.{DruidSQLResult, DruidSQLResults}

val query = dsql"""SELECT COUNT(*) as "count" FROM wikipedia WHERE "__time" >= TIMESTAMP '2015-09-12 00:00:00'"""

val source: Source[DruidSQLResult, NotUsed] = query.stream()

// The resulting stream of DruidSQLResult can also mapped to a case class
case class Result(count: Double)

val sourceOfResults: Source[Result, NotUsed] = query.streamAs[Result]()
```

### Limitations

Please note that Druid does not support all SQL features (e.g., JOIN between native datasources, OVER clauses and 
analytic functions such as LAG and LEAD).  Additionally, some Druid native query features are also not supported 
by the Druid SQL language (e.g., Spatial filters and query cancellation).

In contrast to other Scruid queries (including DQL queries) that integrate directly with the Druid native API, 
SQL interface does not support `.series[T]` and `.streamSeriesAs[T]` functionality. 