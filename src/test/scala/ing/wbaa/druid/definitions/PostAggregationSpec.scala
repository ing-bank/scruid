/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ing.wbaa.druid.definitions

import ing.wbaa.druid.GroupByQuery
import ing.wbaa.druid.definitions.ArithmeticFunction.{ DIV, MINUS, MULT, PLUS, QUOT }
import ing.wbaa.druid.definitions.ArithmeticFunctions._
import ing.wbaa.druid.definitions.FilterOperators._
import io.circe.generic.auto._
import io.circe.syntax._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._
import scala.language.postfixOps
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class PostAggregationSpec extends Matchers with AnyWordSpecLike with ScalaFutures {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(10 seconds, 100 millis)

  private case class Aggregated(count: Double,
                                added: Double,
                                deleted: Double,
                                result: Double,
                                countryName: String)
  private case class ConstantlyAggregated(constant: Double, countryName: String)

  sealed trait TestContext {
    def baseRequest(pas: PostAggregation*): GroupByQuery = GroupByQuery(
      aggregations = List(
        CountAggregation("count"),
        LongSumAggregation("added", "added"),
        LongSumAggregation("deleted", "deleted")
      ),
      intervals = List("2010-09-12T21:00:00/2020-09-12T22:00:00"),
      granularity = GranularityType.Hour,
      dimensions = List(Dimension("countryName")),
      filter = Some(!SelectFilter("countryName", "")),
      postAggregations = pas.toList
    )
  }

  "Ordering" should {
    "be encoded properly" in {
      val ord1: Ordering = Ordering.NumericFirst
      ord1.asJson.noSpaces shouldBe "\"numericFirst\""

      val ord2: Ordering = Ordering.FloatingPoint
      ord2.asJson.noSpaces shouldBe "null"
    }
  }

  "PostAggregation" when {
    "it is Arithmetic" should {
      "add name and ordering" in {
        ArithmeticPostAggregation("", PLUS, Nil).withName("name") shouldBe ArithmeticPostAggregation(
          "name",
          PLUS,
          Nil
        )
        ArithmeticPostAggregation("", PLUS, Nil).withOrdering(Ordering.NumericFirst) shouldBe ArithmeticPostAggregation(
          "",
          PLUS,
          Nil,
          Some(Ordering.NumericFirst)
        )
      }

      "be encoded properly" in {
        val pa: PostAggregation = ArithmeticPostAggregation(
          "total",
          PLUS,
          Seq(
            FieldAccessPostAggregation("a"),
            FieldAccessPostAggregation("b")
          )
        )

        pa.asJson.noSpaces shouldBe
        """{"name":"total","fn":"+","fields":[{"fieldName":"a","name":null,"type":"fieldAccess"},{"fieldName":"b","name":null,"type":"fieldAccess"}],"ordering":null,"type":"arithmetic"}"""
      }

      "return items transformed with arithmetic post-aggregation" in new TestContext {
        val request = baseRequest(
          ArithmeticPostAggregation(
            "result",
            PLUS,
            Seq(FieldAccessPostAggregation("added"), FieldAccessPostAggregation("deleted"))
          )
        ).copy(
          having = Some(GreaterThanHaving("added", 18000))
        )

        whenReady(request.execute()) { response =>
          val list = response.list[Aggregated]
          list shouldBe List(
            Aggregated(1, 18936, 0, 18936, "Bulgaria"),
            Aggregated(14, 40426, 566, 40992, "Colombia")
          )
        }
      }
    }

    "it is FieldAccessor" should {
      "be encoded properly" in {
        val pa1: PostAggregation = FieldAccessPostAggregation("b")
        pa1.asJson.noSpaces shouldBe """{"fieldName":"b","name":null,"type":"fieldAccess"}"""

        val pa2: PostAggregation = FieldAccessPostAggregation("a", Some("b"))
        pa2.asJson.noSpaces shouldBe """{"fieldName":"a","name":"b","type":"fieldAccess"}"""
      }
    }

    "it is FinalizingFieldAccessor" should {
      "be encoded properly" in {
        val pa: PostAggregation = FinalizingFieldAccessPostAggregation("a")
        pa.asJson.noSpaces shouldBe """{"fieldName":"a","name":null,"type":"finalizingFieldAccess"}"""
      }
    }

    "it is Constant" should {
      "be encoded properly" in {
        val pa: PostAggregation = ConstantPostAggregation(100, Some("a"))
        pa.asJson.noSpaces shouldBe """{"value":100.0,"name":"a","type":"constant"}"""
      }

      "return items transformed with constant post-aggregation" in new TestContext {
        val request = baseRequest(
          ConstantPostAggregation(3.14159, Some("constant"))
        ).copy(
          having = Some(GreaterThanHaving("added", 18000))
        )

        whenReady(request.execute()) { response =>
          val list = response.list[ConstantlyAggregated]
          list shouldBe List(
            ConstantlyAggregated(3.14159, "Bulgaria"),
            ConstantlyAggregated(3.14159, "Colombia")
          )
        }
      }
    }

    "it is DoubleGreatestPA" should {
      "be encoded properly" in {
        val pa: PostAggregation =
          DoubleGreatestPostAggregation("greatest",
                                        Seq(ConstantPostAggregation(10.1),
                                            ConstantPostAggregation(10.2)))
        pa.asJson.noSpaces shouldBe """{"name":"greatest","fields":[{"value":10.1,"name":null,"type":"constant"},{"value":10.2,"name":null,"type":"constant"}],"type":"doubleGreatest"}"""
      }

      "return items transformed with doubleGreatest post-aggregation" in new TestContext {
        val request = baseRequest(
          DoubleGreatestPostAggregation("constant",
                                        Seq(ConstantPostAggregation(10.1),
                                            ConstantPostAggregation(10.2)))
        ).copy(
          having = Some(GreaterThanHaving("added", 18000))
        )

        whenReady(request.execute()) { response =>
          val list = response.list[ConstantlyAggregated]
          list shouldBe List(
            ConstantlyAggregated(10.2, "Bulgaria"),
            ConstantlyAggregated(10.2, "Colombia")
          )
        }
      }
    }

    "it is LongGreatestPA" should {
      "be encoded properly" in {
        val pa: PostAggregation =
          LongGreatestPostAggregation("greatest",
                                      Seq(ConstantPostAggregation(10), ConstantPostAggregation(12)))
        pa.asJson.noSpaces shouldBe """{"name":"greatest","fields":[{"value":10.0,"name":null,"type":"constant"},{"value":12.0,"name":null,"type":"constant"}],"type":"longGreatest"}"""
      }

      "return items transformed with longGreatest post-aggregation with long values" in new TestContext {
        val request = baseRequest(
          LongGreatestPostAggregation("constant",
                                      Seq(ConstantPostAggregation(10), ConstantPostAggregation(11)))
        ).copy(
          having = Some(GreaterThanHaving("added", 18000))
        )

        whenReady(request.execute()) { response =>
          val list = response.list[ConstantlyAggregated]
          list shouldBe List(
            ConstantlyAggregated(11, "Bulgaria"),
            ConstantlyAggregated(11, "Colombia")
          )
        }
      }

      "return items transformed with longGreatest post-aggregation with double values" in new TestContext {
        val request = baseRequest(
          LongGreatestPostAggregation("constant",
                                      Seq(ConstantPostAggregation(10.1),
                                          ConstantPostAggregation(10.2)))
        ).copy(
          having = Some(GreaterThanHaving("added", 18000))
        )

        whenReady(request.execute()) { response =>
          val list = response.list[ConstantlyAggregated]
          list shouldBe List(
            ConstantlyAggregated(10, "Bulgaria"),
            ConstantlyAggregated(10, "Colombia")
          )
        }
      }
    }

    "it is DoubleLeastPA" should {
      "be encoded properly" in {
        val pa: PostAggregation =
          DoubleLeastPostAggregation("least",
                                     Seq(ConstantPostAggregation(10.1),
                                         ConstantPostAggregation(10.2)))
        pa.asJson.noSpaces shouldBe """{"name":"least","fields":[{"value":10.1,"name":null,"type":"constant"},{"value":10.2,"name":null,"type":"constant"}],"type":"doubleLeast"}"""
      }

      "return items transformed with doubleLeast post-aggregation" in new TestContext {
        val request = baseRequest(
          DoubleLeastPostAggregation("constant",
                                     Seq(ConstantPostAggregation(10.1),
                                         ConstantPostAggregation(10.2)))
        ).copy(
          having = Some(GreaterThanHaving("added", 18000))
        )

        whenReady(request.execute()) { response =>
          val list = response.list[ConstantlyAggregated]
          list shouldBe List(
            ConstantlyAggregated(10.1, "Bulgaria"),
            ConstantlyAggregated(10.1, "Colombia")
          )
        }
      }
    }

    "it is LongLeastPA" should {
      "be encoded properly" in {
        val pa: PostAggregation =
          LongLeastPostAggregation("least",
                                   Seq(ConstantPostAggregation(10), ConstantPostAggregation(12)))
        pa.asJson.noSpaces shouldBe """{"name":"least","fields":[{"value":10.0,"name":null,"type":"constant"},{"value":12.0,"name":null,"type":"constant"}],"type":"longLeast"}"""
      }

      "return items transformed with longLeast post-aggregation with long values" in new TestContext {
        val request = baseRequest(
          LongLeastPostAggregation("constant",
                                   Seq(ConstantPostAggregation(10), ConstantPostAggregation(11)))
        ).copy(
          having = Some(GreaterThanHaving("added", 18000))
        )

        whenReady(request.execute()) { response =>
          val list = response.list[ConstantlyAggregated]
          list shouldBe List(
            ConstantlyAggregated(10, "Bulgaria"),
            ConstantlyAggregated(10, "Colombia")
          )
        }
      }

      "return items transformed with longLeast post-aggregation with double values" in new TestContext {
        val request = baseRequest(
          LongLeastPostAggregation(
            "constant",
            Seq(
              ConstantPostAggregation(10.1),
              ConstantPostAggregation(10.2)
            )
          )
        ).copy(
          having = Some(GreaterThanHaving("added", 18000))
        )

        whenReady(request.execute()) { response =>
          val list = response.list[ConstantlyAggregated]
          list shouldBe List(
            ConstantlyAggregated(10, "Bulgaria"),
            ConstantlyAggregated(10, "Colombia")
          )
        }
      }
    }

    "it is JavascriptPA" should {
      "be encoded properly" in {
        val pa: PostAggregation = JavascriptPostAggregation(
          "least",
          Seq("str1", "str2"),
          "function(a, b) { return a + b; }"
        )
        pa.asJson.noSpaces shouldBe
        """{"name":"least","fieldNames":["str1","str2"],"function":"function(a, b) { return a + b; }","type":"javascript"}"""
      }
    }
  }

  "ArithmeticOperators" when {
    sealed trait OperatorsContext {
      val a = ConstantPostAggregation(1.0)
      val b = ConstantPostAggregation(2.0)
      val c = ConstantPostAggregation(3.0)
    }

    "using +" should {
      "be encoded" in {
        val op: ArithmeticFunction = PLUS
        op.asJson.noSpaces shouldBe "\"+\""
      }

      "compose properly" in new OperatorsContext {
        a + b shouldBe ArithmeticPostAggregation("", PLUS, Seq(a, b))
      }
    }

    "using -" should {
      "be encoded" in {
        val op: ArithmeticFunction = MINUS
        op.asJson.noSpaces shouldBe "\"-\""
      }

      "compose properly" in new OperatorsContext {
        a - b shouldBe ArithmeticPostAggregation("", MINUS, Seq(a, b))
      }
    }

    "using *" should {
      "be encoded" in {
        val op: ArithmeticFunction = MULT
        op.asJson.noSpaces shouldBe "\"*" +
        "\""
      }

      "compose properly" in new OperatorsContext {
        a * b shouldBe ArithmeticPostAggregation("", MULT, Seq(a, b))
      }
    }

    "using /" should {
      "be encoded" in {
        val op: ArithmeticFunction = DIV
        op.asJson.noSpaces shouldBe "\"/\""
      }

      "compose properly" in new OperatorsContext {
        a / b shouldBe ArithmeticPostAggregation("", DIV, Seq(a, b))
      }
    }

    "using quot" should {
      "be encoded" in {
        val op: ArithmeticFunction = QUOT
        op.asJson.noSpaces shouldBe "\"quotient\""
      }

      "compose properly" in new OperatorsContext {
        a quot b shouldBe ArithmeticPostAggregation("", QUOT, Seq(a, b))
      }
    }

    "using multiple operators" should {
      "do precedence properly" in new OperatorsContext {
        a + b * c shouldBe
        ArithmeticPostAggregation("",
                                  PLUS,
                                  Seq(
                                    a,
                                    ArithmeticPostAggregation("", MULT, Seq(b, c))
                                  ))

        a * b + c shouldBe
        ArithmeticPostAggregation("",
                                  PLUS,
                                  Seq(
                                    ArithmeticPostAggregation("", MULT, Seq(a, b)),
                                    c
                                  ))

        (a + b) * c shouldBe
        ArithmeticPostAggregation("",
                                  MULT,
                                  Seq(
                                    ArithmeticPostAggregation("", PLUS, Seq(a, b)),
                                    c
                                  ))

        a quot b + c shouldBe
        ArithmeticPostAggregation("",
                                  QUOT,
                                  Seq(
                                    a,
                                    ArithmeticPostAggregation("", PLUS, Seq(b, c))
                                  ))
      }
    }
  }
}
