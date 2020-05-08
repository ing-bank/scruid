package ing.wbaa.druid.definitions

import ing.wbaa.druid.GroupByQuery
import ing.wbaa.druid.definitions.FilterOperators._
import io.circe.generic.auto._
import io.circe.syntax._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._
import scala.language.postfixOps
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class ExtractionFnSpec extends Matchers with AnyWordSpecLike with ScalaFutures {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(10 seconds, 100 millis)

  // countyName is optional, because some extractions functions work with nulls and
  // not every fetched item has countryName.
  final case class GroupByCountryName(countryName: Option[String], count: Int)
  final case class GroupByTime(time: String, count: Int)
  final case class GroupByAddedAndCountryName(added: Int, countryName: Option[String], count: Int)

  sealed trait TestContext {
    def baseRequest(dimensions: Dimension*) = GroupByQuery(
      aggregations = List(CountAggregation(name = "count")),
      intervals = List("2015-09-12T21:00:00/2015-09-12T22:00:00"),
      granularity = GranularityType.Hour,
      dimensions = dimensions.toList,
      filter = Some(!SelectFilter("countryName", ""))
    )
  }

  "ExtractionFn" when {
    "using Regex" should {
      "be encoded properly" in {
        val fn1: ExtractionFn = RegexExtractionFn("(\\w\\w\\w).*")
        fn1.asJson.noSpaces shouldBe
        """{"expr":"(\\w\\w\\w).*","index":1,"replaceMissingValue":false,"replaceMissingValueWith":null,"type":"regex"}"""

        val fn2: ExtractionFn = RegexExtractionFn(".*", Some(2), Some(true), Some("foo"))
        fn2.asJson.noSpaces shouldBe
        """{"expr":".*","index":2,"replaceMissingValue":true,"replaceMissingValueWith":"foo","type":"regex"}"""
      }

      "return items transformed by regex" in new TestContext {
        val request = baseRequest(
          ExtractionDimension(
            dimension = "countryName",
            extractionFn = RegexExtractionFn("(\\w\\w\\w).*")
          )
        )
        whenReady(request.execute()) { response =>
          val list = response.list[GroupByCountryName].filter(_.count > 13)
          list shouldBe List(
            GroupByCountryName(Some("Fra"), 14),
            GroupByCountryName(Some("Uni"), 48) // United States 36 + United Kingdom 12
          )
        }
      }
    }

    "using Partial" should {
      "be encoded properly" in {
        val fn: ExtractionFn = PartialExtractionFn("ca$")
        fn.asJson.noSpaces shouldBe """{"expr":"ca$","type":"partial"}"""
      }

      "return items for which regular expression matches" in new TestContext {
        val request = baseRequest(
          ExtractionDimension(
            dimension = "countryName",
            extractionFn = PartialExtractionFn("ca$")
          )
        )

        whenReady(request.execute()) { response =>
          val list = response.list[GroupByCountryName]
          list shouldBe List(
            GroupByCountryName(None, 191), // null appears after function transformation, not before. Use Having to filter such nulls out.
            GroupByCountryName(Some("South Africa"), 1)
          )
        }
      }
    }

    "using Strlen" should {
      "be encoded properly" in {
        val fn: ExtractionFn = StrlenExtractionFn
        fn.asJson.noSpaces shouldBe """{"type":"strlen"}"""
      }

      "return length of items" in new TestContext {
        val request = baseRequest(
          ExtractionDimension(
            dimension = "countryName",
            extractionFn = StrlenExtractionFn
          )
        )

        whenReady(request.execute()) { response =>
          val list = response.list[GroupByCountryName].filter(_.count > 30)
          list shouldBe List(
            GroupByCountryName(Some("13"), 36),
            GroupByCountryName(Some("6"), 64)
          )
        }
      }
    }

    "using Substring" should {
      "be encoded properly" in {
        val fn1: ExtractionFn = SubstringExtractionFn(1)
        fn1.asJson.noSpaces shouldBe """{"index":1,"length":null,"type":"substring"}"""

        val fn2: ExtractionFn = SubstringExtractionFn(1, Some(3))
        fn2.asJson.noSpaces shouldBe """{"index":1,"length":3,"type":"substring"}"""
      }

      "return length of items" in new TestContext {
        val request = baseRequest(
          ExtractionDimension(
            dimension = "countryName",
            extractionFn = SubstringExtractionFn(0, Some(4))
          )
        )

        whenReady(request.execute()) { response =>
          val list = response.list[GroupByCountryName].filter(_.count > 13)
          list shouldBe List(
            GroupByCountryName(Some("Fran"), 14),
            GroupByCountryName(Some("Unit"), 48)
          )
        }
      }
    }

    "using TimeFormat" should {
      "be encoded properly" in {
        val fn1: ExtractionFn = TimeFormatExtractionFn()
        fn1.asJson.noSpaces shouldBe
        """{"format":null,"timeZone":"utc","locale":null,"granularity":"none","asMillis":false,"type":"timeFormat"}"""

        val fn2: ExtractionFn = TimeFormatExtractionFn(Some("HH"),
                                                       Some("Europe/Berlin"),
                                                       Some("en-GB"),
                                                       Some(GranularityType.Hour),
                                                       Some(true))
        fn2.asJson.noSpaces shouldBe
        """{"format":"HH","timeZone":"Europe/Berlin","locale":"en-GB","granularity":"hour","asMillis":true,"type":"timeFormat"}"""
      }

      "return extracted time" in new TestContext {
        def request(extFn: ExtractionFn) =
          baseRequest(
            ExtractionDimension(
              dimension = "__time",
              outputName = Some("time"),
              extractionFn = extFn
            )
          )

        val fn1 = TimeFormatExtractionFn()
        whenReady(request(fn1).execute()) { response =>
          response.list[GroupByTime].size shouldBe 192
        }

        val fn2 = TimeFormatExtractionFn(
          granularity = Some(GranularityType.ThirtyMinute),
          // need to set zone to None, when asMillis = true. With default "utc" it will also require to provide time format, but we want just millis
          timeZone = None,
          asMillis = Some(true)
        )
        whenReady(request(fn2).execute()) { response =>
          val list = response.list[GroupByTime]
          list shouldBe List(
            GroupByTime("1442091600000", 89),
            GroupByTime("1442093400000", 103)
          )
        }

        val fn3 = TimeFormatExtractionFn(
          granularity = Some(GranularityType.FifteenMinute),
          format = Some("mm") // minutes
        )
        whenReady(request(fn3).execute()) { response =>
          val list = response.list[GroupByTime]
          list shouldBe List(
            GroupByTime("00", 39),
            GroupByTime("15", 50),
            GroupByTime("30", 50),
            GroupByTime("45", 53)
          )
        }

        val fn4 = TimeFormatExtractionFn(
          format = Some("EEEE"),
          timeZone = Some("America/Montreal"),
          locale = Some("fr")
        )
        whenReady(request(fn4).execute()) { response =>
          val list = response.list[GroupByTime]
          list shouldBe List(
            GroupByTime("samedi", 192)
          )
        }
      }
    }

    "using TimeParsing" should {
      "be encoded properly" in {
        val fn: ExtractionFn = TimeParsingExtractionFn("yyyy-MM-dd", "dd-MM-yy")
        fn.asJson.noSpaces shouldBe
        """{"timeFormat":"yyyy-MM-dd","resultFormat":"dd-MM-yy","type":"time"}"""
      }

      "return formatted timestamp" in new TestContext {
        def request(extFn: ExtractionFn) =
          baseRequest(
            ExtractionDimension(
              dimension = "__time",
              outputName = Some("time"),
              extractionFn = extFn
            )
          )

        val fn1: ExtractionFn = TimeParsingExtractionFn(
          // this basically have no sense, since we haven't got timestamps,
          // but the result will surprise you
          timeFormat = "AAA",
          resultFormat = "EEE, MMM d, HH, ''yy"
        )
        whenReady(request(fn1).execute()) { response =>
          val list = response.list[GroupByTime]
          list shouldBe List(
            GroupByTime("Sat, Dec 20, 05, '69", 115),
            GroupByTime("Sat, Dec 20, 06, '69", 77)
          )
        }

        val fn2: ExtractionFn = TimeParsingExtractionFn(
          // when the timeFormat doesn't suit, it fallbacks to millis
          timeFormat = "yyyy-MM-dd",
          resultFormat = "EEE, MMM d, HH,''yy"
        )
        whenReady(request(fn2).execute()) { response =>
          val list = response.list[GroupByTime]
          list.size shouldBe 192
          list foreach (_.time.toLong) // no error - value is long
        }
      }
    }

    "using Javascript" should {
      "be encoded properly" in {
        val fn: ExtractionFn = JavascriptExtractionFn("function(str) { return str.substr(0, 3); }")
        fn.asJson.noSpaces shouldBe
        """{"function":"function(str) { return str.substr(0, 3); }","injective":false,"type":"javascript"}"""
      }
    }

    "using Cascade" should {
      "be encoded properly" in {
        val fn: ExtractionFn = CascadeExtractionFn(
          Seq(
            PartialExtractionFn("ia$"),
            SubstringExtractionFn(0, Some(4))
          )
        )
        fn.asJson.noSpaces shouldBe
        """{"extractionFns":[{"expr":"ia$","type":"partial"},{"index":0,"length":4,"type":"substring"}],"type":"cascade"}"""
      }

      "return item transformed by two functions" in new TestContext {
        val request =
          baseRequest(
            ExtractionDimension(
              dimension = "countryName",
              extractionFn = CascadeExtractionFn(
                Seq(
                  PartialExtractionFn("ia$"),
                  SubstringExtractionFn(0, Some(4))
                )
              )
            )
          )

        whenReady(request.execute()) { response =>
          val list = response
            .list[GroupByCountryName]
            .filter(c => c.countryName.isDefined && c.count > 3)
          list shouldBe List(
            GroupByCountryName(Some("Aust"), 4),
            GroupByCountryName(Some("Colo"), 8),
            GroupByCountryName(Some("Russ"), 12)
          )
        }
      }
    }

    "using StringFormat" should {
      "be encoded properly" in {
        val fn: ExtractionFn = StringFormatExtractionFn("[%s]", Some(NullHandling.EmptyString))
        fn.asJson.noSpaces shouldBe
        """{"format":"[%s]","nullHandling":"emptyString","type":"stringFormat"}"""
      }

      "return items transformed with function" in new TestContext {
        def request(nullHandling: NullHandling) =
          baseRequest(
            ExtractionDimension(
              dimension = "countryName",
              extractionFn = StringFormatExtractionFn("[%s]", Some(nullHandling))
            )
          ).copy(filter = None)

        whenReady(request(NullHandling.EmptyString).execute()) { response =>
          val list = response.list[GroupByCountryName].filter(_.count > 13)
          list shouldBe List(
            GroupByCountryName(Some("[France]"), 14),
            GroupByCountryName(Some("[United States]"), 36),
            GroupByCountryName(Some("[]"), 1574)
          )
        }

        whenReady(request(NullHandling.NullString).execute()) { response =>
          val list = response.list[GroupByCountryName].filter(_.count > 13)
          list shouldBe List(
            GroupByCountryName(Some("[France]"), 14),
            GroupByCountryName(Some("[United States]"), 36),
            GroupByCountryName(Some("[null]"), 1574)
          )
        }

        whenReady(request(NullHandling.ReturnNull).execute()) { response =>
          val list = response.list[GroupByCountryName].filter(_.count > 13)
          list shouldBe List(
            GroupByCountryName(None, 1574),
            GroupByCountryName(Some("[France]"), 14),
            GroupByCountryName(Some("[United States]"), 36)
          )
        }
      }
    }

    "using Upper" should {
      "be encoded properly" in {
        val fn: ExtractionFn = UpperExtractionFn(Some("fr"))
        fn.asJson.noSpaces shouldBe
        """{"locale":"fr","type":"upper"}"""
      }

      "return uppercased items" in new TestContext {
        val request =
          baseRequest(
            ExtractionDimension(
              dimension = "countryName",
              extractionFn = UpperExtractionFn()
            )
          )

        whenReady(request.execute()) { response =>
          val list = response.list[GroupByCountryName].filter(_.count > 13)
          list shouldBe List(
            GroupByCountryName(Some("FRANCE"), 14),
            GroupByCountryName(Some("UNITED STATES"), 36)
          )
        }
      }
    }

    "using Lower" should {
      "be encoded properly" in {
        val fn1: ExtractionFn = LowerExtractionFn()
        fn1.asJson.noSpaces shouldBe
        """{"locale":null,"type":"lower"}"""

        val fn2: ExtractionFn = LowerExtractionFn(Some("fr"))
        fn2.asJson.noSpaces shouldBe
        """{"locale":"fr","type":"lower"}"""
      }

      "return lowercased items" in new TestContext {
        val request =
          baseRequest(
            ExtractionDimension(
              dimension = "countryName",
              extractionFn = LowerExtractionFn()
            )
          )

        whenReady(request.execute()) { response =>
          val list = response.list[GroupByCountryName].filter(_.count > 13)
          list shouldBe List(
            GroupByCountryName(Some("france"), 14),
            GroupByCountryName(Some("united states"), 36)
          )
        }
      }
    }

    "using Bucket" should {
      "be encoded properly" in {
        val fn1: ExtractionFn = BucketExtractionFn()
        fn1.asJson.noSpaces shouldBe
        """{"size":1,"offset":0,"type":"bucket"}"""

        val fn2: ExtractionFn = BucketExtractionFn(Some(100), Some(10))
        fn2.asJson.noSpaces shouldBe
        """{"size":100,"offset":10,"type":"bucket"}"""
      }

      "return transformed items" in new TestContext {
        val request =
          baseRequest(
            ExtractionDimension(
              dimension = "added",
              outputType = Some("LONG"),
              extractionFn = BucketExtractionFn(Some(200))
            ),
            Dimension("countryName")
          )

        whenReady(request.execute()) { response =>
          val list = response.list[GroupByAddedAndCountryName]
          list.size shouldBe 62
          list foreach (_.added % 200 shouldBe 0)
          list.filter(_.added == 800).flatMap(_.countryName.toList) shouldBe List(
            "Canada",
            "Mexico",
            "Philippines",
            "Poland"
          )
        }
      }
    }

  }
}
