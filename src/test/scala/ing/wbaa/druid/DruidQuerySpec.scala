package ing.wbaa.druid

import org.scalatest._
import org.scalatest.concurrent._
import org.scalatest.time._
import ing.wbaa.druid.definitions._
import io.circe.generic.auto._
import ing.wbaa.druid.definitions.FilterOperators._

class DruidQuerySpec extends WordSpec with Matchers with ScalaFutures {
  implicit override val patienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(5, Millis))
  private val totalNumberOfEntries = 39244

  case class TimeseriesCount(count: Int)
  case class GroupByIsAnonymous(isAnonymous: String, count: Int)
  case class TopCountry(count: Int, countryName: Option[String])
  case class AggregatedFilteredAnonymous(count: Int, isAnonymous: String, filteredCount: Int)

  "TimeSeriesQuery" should {
    "successfully be interpreted by Druid" in {
      val request = TimeSeriesQuery(
        aggregations = List(
          CountAggregation(name = "count")
        ),
        granularity = GranularityType.Hour,
        intervals = List("2011-06-01/2017-06-01")
      ).execute

      whenReady(request) { response =>
        response.list[TimeseriesCount].map(_.count).sum shouldBe totalNumberOfEntries
      }
    }

    "extract the data and return a map with the timestamps as keys" in {
      val request = TimeSeriesQuery(
        aggregations = List(
          CountAggregation(name = "count")
        ),
        granularity = GranularityType.Hour,
        intervals = List("2011-06-01/2017-06-01")
      ).execute

      whenReady(request) { response =>
        response
          .series[TimeseriesCount]
          .flatMap { case (_, items) => items.map(_.count) }
          .sum shouldBe totalNumberOfEntries
      }
    }
  }

  "GroupByQuery" should {
    "successfully be interpreted by Druid" in {
      val request = GroupByQuery(
        aggregations = List(
          CountAggregation(name = "count")
        ),
        dimensions = List(Dimension(dimension = "isAnonymous")),
        intervals = List("2011-06-01/2017-06-01")
      ).execute

      whenReady(request) { response =>
        response.list[GroupByIsAnonymous].map(_.count).sum shouldBe totalNumberOfEntries
      }
    }

    "successfully be interpreted by Druid when using lower granularity" in {
      val request = GroupByQuery(
        aggregations = List(
          CountAggregation(name = "count")
        ),
        dimensions = List(Dimension(dimension = "isAnonymous")),
        intervals = List("2011-06-01/2017-06-01"),
        granularity = GranularityType.Hour
      ).execute

      whenReady(request) { response =>
        response.list[GroupByIsAnonymous].map(_.count).sum shouldBe totalNumberOfEntries
      }
    }
  }

  "TopNQuery" should {
    "successfully be interpreted by Druid" in {
      val threshold = 5

      val request = TopNQuery(
        dimension = Dimension(
          dimension = "countryName"
        ),
        threshold = threshold,
        metric = "count",
        aggregations = List(
          CountAggregation(name = "count")
        ),
        intervals = List("2011-06-01/2017-06-01")
      ).execute

      whenReady(request) { response =>
        val topN = response.list[TopCountry]

        topN.size shouldBe threshold

        topN.head shouldBe TopCountry(count = 35445, countryName = None)
        topN(1) shouldBe TopCountry(count = 528, countryName = Some("United States"))
        topN(2) shouldBe TopCountry(count = 256, countryName = Some("Italy"))
        topN(3) shouldBe TopCountry(count = 234, countryName = Some("United Kingdom"))
        topN(4) shouldBe TopCountry(count = 205, countryName = Some("France"))
      }
    }

    "also work with a filter" in {
      val filterUnitedStates = SelectFilter(dimension = "countryName", value = "United States")
      val filterBoth = filterUnitedStates || SelectFilter(dimension = "countryName",
                                                          value = "Italy")

      val request = TopNQuery(
        dimension = Dimension(
          dimension = "countryName"
        ),
        filter = Some(filterBoth),
        threshold = 5,
        metric = "count",
        aggregations = List(
          CountAggregation(name = "count")
        ),
        intervals = List("2011-06-01/2017-06-01")
      ).execute

      whenReady(request) { response =>
        val topN = response.list[TopCountry]
        topN.size shouldBe 2
        topN.head shouldBe TopCountry(count = 528, countryName = Some("United States"))
        topN(1) shouldBe TopCountry(count = 256, countryName = Some("Italy"))
      }
    }
  }

  "also work with 'in' filtered aggregations" should {
    "successfully be interpreted by Druid" in {

      val request = TopNQuery(
        dimension = Dimension(
          dimension = "isAnonymous"
        ),
        threshold = 5,
        metric = "count",
        aggregations = List(
          LongSumAggregation(name = "count", fieldName = "count"),
          InFilteredAggregation(
            name = "InFilteredAgg",
            InFilter(dimension = "channel", values = List("#en.wikipedia", "#de.wikipedia")),
            aggregator = LongSumAggregation(name = "filteredCount", fieldName = "count")
          )
        ),
        intervals = List("2011-06-01/2017-06-01")
      ).execute

      whenReady(request) { response =>
        val topN = response.list[AggregatedFilteredAnonymous]
        topN.size shouldBe 2
        topN.head shouldBe AggregatedFilteredAnonymous(count = 35445,
                                                       filteredCount = 12374,
                                                       isAnonymous = "false")
        topN(1) shouldBe AggregatedFilteredAnonymous(count = 3799,
                                                     filteredCount = 1698,
                                                     isAnonymous = "true")
      }
    }
  }

  "also work with 'selector' filtered aggregations" should {
    "successfully be interpreted by Druid" in {

      val request = TopNQuery(
        dimension = Dimension(
          dimension = "isAnonymous"
        ),
        threshold = 5,
        metric = "count",
        aggregations = List(
          LongSumAggregation(name = "count", fieldName = "count"),
          SelectorFilteredAggregation(
            name = "SelectorFilteredAgg",
            SelectFilter(dimension = "channel", value = "#en.wikipedia"),
            aggregator = LongSumAggregation(name = "filteredCount", fieldName = "count")
          )
        ),
        intervals = List("2011-06-01/2017-06-01")
      ).execute

      whenReady(request) { response =>
        val topN = response.list[AggregatedFilteredAnonymous]
        topN.size shouldBe 2
        topN.head shouldBe AggregatedFilteredAnonymous(count = 35445,
                                                       filteredCount = 9993,
                                                       isAnonymous = "false")
        topN(1) shouldBe AggregatedFilteredAnonymous(count = 3799,
                                                     filteredCount = 1556,
                                                     isAnonymous = "true")
      }
    }
  }

  "DruidResponse" should {
    "successfully convert the series" in {
      val request = GroupByQuery(
        aggregations = List(
          CountAggregation(name = "count")
        ),
        dimensions = List(Dimension(dimension = "isAnonymous")),
        intervals = List("2011-06-01/2017-06-01"),
        granularity = GranularityType.All
      ).execute

      whenReady(request) { response =>
        response
          .series[GroupByIsAnonymous]
          .flatMap {
            case (timestamp @ _, values) =>
              values.map(_.count)
          }
          .sum shouldBe totalNumberOfEntries
      }
    }

    "successfully convert the series when using lower granularity" in {
      val request = GroupByQuery(
        aggregations = List(
          CountAggregation(name = "count")
        ),
        dimensions = List(Dimension(dimension = "isAnonymous")),
        intervals = List("2011-06-01/2017-06-01"),
        granularity = GranularityType.Hour
      ).execute

      whenReady(request) { response =>
        response
          .series[GroupByIsAnonymous]
          .flatMap {
            case (timestamp @ _, values) =>
              values.map(_.count)
          }
          .sum shouldBe totalNumberOfEntries
      }
    }
  }
}
