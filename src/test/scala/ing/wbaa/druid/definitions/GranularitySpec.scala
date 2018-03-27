package ing.wbaa.druid.definitions

import org.scalatest._

import io.circe._
import io.circe.syntax._

class GranularitySpec extends WordSpec with Matchers {
  "Granularities" should {
    "be able to encode to json" in {
      implicit val granularityEncoder: Encoder[Granularity] = GranularityType.encoder

      val gran: Granularity = GranularityType.FifteenMinute
      gran.asJson.noSpaces shouldBe "\"fifteen_minute\""
    }

    "be able to decode json to a Granularity" in {
      implicit val granularityDecoder: Decoder[Granularity] = GranularityType.decoder
      val thirtyMinute                                      = "thirty_minute"
      thirtyMinute.asJson.as[Granularity] shouldBe Right(GranularityType.ThirtyMinute)

      val all = "all"
      all.asJson.as[Granularity] shouldBe Right(GranularityType.All)
    }

  }
}
