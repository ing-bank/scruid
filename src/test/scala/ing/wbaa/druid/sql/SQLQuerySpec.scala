package ing.wbaa.druid.sql

import java.time.{ LocalDateTime, ZonedDateTime }

import akka.stream.scaladsl.Sink
import ing.wbaa.druid.{ DruidConfig, SQLQuery }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.{ Matchers, WordSpec }
import ing.wbaa.druid.SQL._
import ing.wbaa.druid.client.CirceDecoders
import io.circe.generic.auto._
import io.circe.syntax._

//noinspection SqlNoDataSourceInspection
class SQLQuerySpec extends WordSpec with Matchers with ScalaFutures with CirceDecoders {
  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(5, Millis))
  private val totalNumberOfEntries = 39244

  implicit val config = DruidConfig()
  implicit val mat    = config.client.actorMaterializer

  case class Result(hourTime: ZonedDateTime, count: Int)

  "SQL query" should {

    val query: SQLQuery = sql"""
      |SELECT FLOOR(__time to HOUR) AS hourTime, count(*) AS "count"
      |FROM wikipedia
      |WHERE "__time" BETWEEN TIMESTAMP '2015-09-12 00:00:00' AND TIMESTAMP '2015-09-13 00:00:00'
      |GROUP BY 1
      |""".stripMargin

    "successfully be interpreted by Druid" in {
      val resultsF = query.execute()
      whenReady(resultsF) { response =>
        response.list[Result].map(_.count).sum shouldBe totalNumberOfEntries
      }

    }

    "support streaming" in {
      val resultsF = query.streamAs[Result]().runWith(Sink.seq)

      whenReady(resultsF) { results =>
        results.map(_.count).sum shouldBe totalNumberOfEntries
      }

    }

  }

  "SQL parameterized query" should {

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

    "successfully be interpreted by Druid" in {
      val resultsF = query.execute()
      whenReady(resultsF) { response =>
        response.list[Result].map(_.count).sum shouldBe totalNumberOfEntries
      }
    }

    "support streaming" in {
      val resultsF = query.streamAs[Result]().runWith(Sink.seq)

      whenReady(resultsF) { results =>
        results.map(_.count).sum shouldBe totalNumberOfEntries
      }

    }

  }
}
