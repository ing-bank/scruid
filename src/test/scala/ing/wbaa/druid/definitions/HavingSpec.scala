package ing.wbaa.druid.definitions

import ing.wbaa.druid.GroupByQuery
import io.circe.generic.auto._
import io.circe.syntax._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ Matchers, WordSpecLike }

import scala.concurrent.duration._
import scala.language.postfixOps

class HavingSpec extends Matchers with WordSpecLike with ScalaFutures {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(5 seconds, 50 millis)

  case class GroupByIsAnonymous(isAnonymous: String, count: Int)

  sealed trait TestContext {
    val baseRequest = GroupByQuery(
      aggregations = List(CountAggregation(name = "count")),
      dimensions = List(Dimension(dimension = "isAnonymous")),
      intervals = List("2011-06-01/2017-06-01"),
      granularity = GranularityType.Hour
    )
  }

  "Having" when {
    "using Filter" should {
      "be encoded properly" in {
        val having: Having = FilterHaving(SelectFilter("dim", "value"))
        having.asJson.noSpaces shouldBe
        """{"filter":{"dimension":"dim","value":"value","extractionFn":null,"type":"selector"},"type":"filter"}"""
      }

      "should behave the same as usual filter" in new TestContext {
        val request = baseRequest
          .copy(
            having = Some(FilterHaving(SelectFilter("isAnonymous", "false")))
          )
          .execute

        whenReady(request) { response =>
          val list = response.list[GroupByIsAnonymous]
          list.size shouldBe 24
          list foreach (_.isAnonymous shouldBe "false")
        }
      }
    }

    "using Equalto" should {
      "be encoded properly" in {
        val having: Having = EqualToHaving("aggr", 123)
        having.asJson.noSpaces shouldBe
        """{"aggregation":"aggr","value":123.0,"type":"equalTo"}"""
      }

      "should return items with specified value" in new TestContext {
        val request = baseRequest
          .copy(
            having = Some(EqualToHaving("count", 981))
          )
          .execute

        whenReady(request) { response =>
          response.list[GroupByIsAnonymous] shouldBe List(
            GroupByIsAnonymous("false", 981)
          )
        }
      }

    }

    "using GreaterThan" should {
      "be encoded properly" in {
        val having: Having = GreaterThanHaving("aggr", 123)
        having.asJson.noSpaces shouldBe
        """{"aggregation":"aggr","value":123.0,"type":"greaterThan"}"""
      }

      "should return items with values greater than specified" in new TestContext {
        val request = baseRequest
          .copy(
            having = Some(GreaterThanHaving("count", 2000))
          )
          .execute

        whenReady(request) { response =>
          response.list[GroupByIsAnonymous] shouldBe List(
            GroupByIsAnonymous("false", 2003),
            GroupByIsAnonymous("false", 2110),
            GroupByIsAnonymous("false", 2083)
          )
        }
      }
    }

    "using LessThan" should {
      "be encoded properly" in {
        val having: Having = LessThanHaving("aggr", 123)
        having.asJson.noSpaces shouldBe
        """{"aggregation":"aggr","value":123.0,"type":"lessThan"}"""
      }

      "should return items with values less than specified" in new TestContext {
        val request = baseRequest
          .copy(
            having = Some(LessThanHaving("count", 100))
          )
          .execute

        whenReady(request) { response =>
          response.list[GroupByIsAnonymous] shouldBe List(
            GroupByIsAnonymous("true", 25)
          )
        }
      }
    }

    "using DimSelector" should {
      "be encoded properly" in {
        val having: Having = DimSelectorHaving("dim", "value")
        having.asJson.noSpaces shouldBe
        """{"dimension":"dim","value":"value","type":"dimSelector"}"""
      }

      "should return items with specified values" in new TestContext {
        val request = baseRequest
          .copy(
            having = Some(DimSelectorHaving("isAnonymous", "false"))
          )
          .execute

        whenReady(request) { response =>
          val list = response.list[GroupByIsAnonymous]
          list.size shouldBe 24
          list.foreach(_.isAnonymous shouldBe "false")
        }
      }
    }

    "using And" should {
      "be encoded properly" in {
        val having: Having = AndHaving(
          List(
            GreaterThanHaving("aggr", 123),
            DimSelectorHaving("dim", "value")
          )
        )
        having.asJson.noSpaces shouldBe
        """{"havingSpecs":[{"aggregation":"aggr","value":123.0,"type":"greaterThan"},{"dimension":"dim","value":"value","type":"dimSelector"}],"type":"and"}"""
      }

      "should return items after applying AND having" in new TestContext {
        val request = baseRequest
          .copy(
            having = Some(
              AndHaving(
                List(
                  GreaterThanHaving("count", 1400),
                  LessThanHaving("count", 1600)
                )
              )
            )
          )
          .execute

        whenReady(request) { response =>
          val list = response.list[GroupByIsAnonymous]
          list.size shouldBe 6
          list.foreach(_.count should (be > 1400 and be < 1600))
        }
      }
    }

    "using Or" should {
      "be encoded properly" in {
        val having: Having = OrHaving(
          List(
            GreaterThanHaving("aggr", 123),
            DimSelectorHaving("dim", "value")
          )
        )
        having.asJson.noSpaces shouldBe
        """{"havingSpecs":[{"aggregation":"aggr","value":123.0,"type":"greaterThan"},{"dimension":"dim","value":"value","type":"dimSelector"}],"type":"or"}"""
      }

      "should return items after applying OR having" in new TestContext {
        val request = baseRequest
          .copy(
            having = Some(
              OrHaving(
                List(
                  GreaterThanHaving("count", 2100),
                  LessThanHaving("count", 100)
                )
              )
            )
          )
          .execute

        whenReady(request) { response =>
          response.list[GroupByIsAnonymous] shouldBe List(
            GroupByIsAnonymous("true", 25),
            GroupByIsAnonymous("false", 2110)
          )
        }
      }
    }

    "using Not" should {
      "be encoded properly" in {
        val having: Having = NotHaving(GreaterThanHaving("aggr", 123))
        having.asJson.noSpaces shouldBe
        """{"havingSpec":{"aggregation":"aggr","value":123.0,"type":"greaterThan"},"type":"not"}"""
      }

      "should exclude items with specified havings" in new TestContext {
        val request = baseRequest
          .copy(
            having = Some(NotHaving(GreaterThanHaving("count", 100)))
          )
          .execute

        whenReady(request) { response =>
          response.list[GroupByIsAnonymous] shouldBe List(
            GroupByIsAnonymous("true", 25),
            GroupByIsAnonymous("true", 100)
          )
        }
      }
    }
  }

  "HavingOperators" when {
    sealed trait HavingOperatorsContext {
      val having1 = EqualToHaving("aggr1", 1)
      val having2 = GreaterThanHaving("aggr2", 2)
      val having3 = LessThanHaving("aggr3", 3)
      val having4 = LessThanHaving("aggr4", 4)
    }

    "using &&" should {
      "compose two AndHavings properly" in new HavingOperatorsContext {
        HavingSpecOperators.&&(
          AndHaving(List(having1, having2)),
          AndHaving(List(having3, having4))
        ) shouldBe AndHaving(List(having1, having2, having3, having4))
      }

      "compose AndHaving and other having properly" in new HavingOperatorsContext {
        HavingSpecOperators.&&(
          AndHaving(List(having1, having2)),
          having3
        ) shouldBe AndHaving(List(having1, having2, having3))
      }

      "compose having and AndHaving properly" in new HavingOperatorsContext {
        HavingSpecOperators.&&(
          having3,
          AndHaving(List(having1, having2))
        ) shouldBe AndHaving(List(having1, having2, having3))
      }

      "compose two havings properly " in new HavingOperatorsContext {
        HavingSpecOperators.&&(
          having1,
          OrHaving(List(having2))
        ) shouldBe AndHaving(List(having1, OrHaving(List(having2))))
      }
    }

    "using ||" should {
      "compose two OrHavings properly" in new HavingOperatorsContext {
        HavingSpecOperators.||(
          OrHaving(List(having1, having2)),
          OrHaving(List(having3, having4))
        ) shouldBe OrHaving(List(having1, having2, having3, having4))
      }

      "compose OrHaving and other having properly" in new HavingOperatorsContext {
        HavingSpecOperators.||(
          OrHaving(List(having1, having2)),
          having3
        ) shouldBe OrHaving(List(having1, having2, having3))
      }

      "compose having and OrHaving properly" in new HavingOperatorsContext {
        HavingSpecOperators.||(
          having3,
          OrHaving(List(having1, having2))
        ) shouldBe OrHaving(List(having1, having2, having3))
      }

      "compose two havings properly " in new HavingOperatorsContext {
        HavingSpecOperators.||(
          having1,
          AndHaving(List(having2))
        ) shouldBe OrHaving(List(having1, AndHaving(List(having2))))
      }
    }

    "using implicit ops" should {
      import HavingSpecOperators._

      "compose havings with &&" in new HavingOperatorsContext {
        having1 && having2 shouldBe AndHaving(List(having1, having2))
      }

      "compose havings with ||" in new HavingOperatorsContext {
        having1 || having2 shouldBe OrHaving(List(having1, having2))
      }

      "compose havings with !" in new HavingOperatorsContext {
        !having1 shouldBe NotHaving(having1)
      }
    }

  }
}
