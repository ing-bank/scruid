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

import org.scalatest._

import io.circe._
import io.circe.syntax._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class GranularitySpec extends AnyWordSpec with Matchers {
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
