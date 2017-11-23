package ing.wbaa.druid.common.json

import org.scalatest._
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import ing.wbaa.druid.definitions.AggregationType

class AggregationTypeSerializerSpec extends WordSpec with Matchers {
  implicit val formats = Serialization.formats(NoTypeHints) + AggregationTypeSerializer
  "AggregationTypeSerializer" should {
    "serialize to json" in {
      val serialized: String = write(AggregationType.LongSum)
      serialized shouldBe """"longSum""""
    }

    "deserialize to a type" in {
      val deserialized = read[AggregationType](""""doubleMax"""")
      deserialized shouldBe AggregationType.DoubleMax
    }

    "throw an exception when the aggregation type is not known" in {
      val caught = intercept[MappingException] {
        read[AggregationType]("""""fakeType"""")
      }
      caught.getMessage should include("not a known aggregation type")
    }
  }
}
