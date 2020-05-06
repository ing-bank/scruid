# Druid SQL

Scruid provides querying via [Druid SQL](https://druid.apache.org/docs/latest/querying/sql.html) by using the 
processed string function `sql`. 

```scala
import scala.concurrent.Future
import ing.wbaa.druid.SQL._
import ing.wbaa.druid.DruidSQLResults

val query = sql"""SELECT COUNT(*) as "count" FROM wikipedia WHERE "__time" >= TIMESTAMP '2015-09-12 00:00:00'"""

val response: Future[DruidSQLResults] = query.execute()

// The resulting response can also mapped to a case class
case class Result(count: Double)

val result: Future[Result] = response.map(_.list[Result])
```

Function `sql`, allows multiline queries:

```scala
import ing.wbaa.druid.SQL._

val query = sql"""
    |SELECT COUNT(*) as "count" 
    |FROM wikipedia 
    |WHERE "__time" >= TIMESTAMP '2015-09-12 00:00:00'
    """.stripMargin
```

Supports standard string interpolation:

```scala
import ing.wbaa.druid.SQL._

val countColumnName = "count"
val dataSourceName = "wikipedia"
val dateTime = "2015-09-12 00:00:00"

val query = sql"""
    |SELECT COUNT(*) as "${countColumnName}" 
    |FROM ${dataSourceName} 
    |WHERE "__time" >= TIMESTAMP '${dateTime}'
    """.stripMargin
```

### Parameterized Queries

Supports [Druid SQL Parameterized Queries](https://druid.apache.org/docs/latest/querying/sql.html#http-post):

```scala
import java.time.LocalDateTime
import ing.wbaa.druid.SQLQuery

import ing.wbaa.druid.SQL._

val fromDateTime  = LocalDateTime.of(2015, 9, 12, 0, 0, 0, 0)
val untilDateTime = fromDateTime.plusDays(1)

val queryParameterized: SQLQuery.Parameterized =
  sql"""
  |SELECT FLOOR(__time to HOUR) AS hourTime, count(*) AS "count"
  |FROM wikipedia
  |WHERE "__time" BETWEEN ? AND ?
  |GROUP BY 1
  |""".stripMargin.parameterized

val query: SQLQuery = queryParameterized
  .withParameter(fromDateTime)
  .withParameter(untilDateTime)
  .create()
```

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
  sql"""SELECT COUNT(*) as "count" FROM wikipedia WHERE "__time" >= TIMESTAMP '2015-09-12 00:00:00'"""
      .setContext(contextParameters)
```

### Handling large payloads with Akka Streams

SQL queries in Scruid support streaming using the `stream` and `streamAs` functions.

```scala
import akka.NotUsed
import akka.stream.scaladsl.Source
import ing.wbaa.druid.SQL._
import ing.wbaa.druid.{DruidSQLResult, DruidSQLResults}

val query = sql"""SELECT COUNT(*) as "count" FROM wikipedia WHERE "__time" >= TIMESTAMP '2015-09-12 00:00:00'"""

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