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

package ing.wbaa.druid
package definitions

import io.circe._
import ca.mrvisser.sealerate

sealed trait Granularity extends Enum with SnakeCaseEnumStringEncoder

object Granularity {
  implicit val granularityEncoder: Encoder[Granularity] = GranularityType.encoder
  implicit val granularityDecoder: Decoder[Granularity] = GranularityType.decoder
}

// We can't use the CompanionObject for the Enum instances due to an issue in circe (knownDirectSubclasses, https://github.com/circe/circe/issues/639)
// Read more here: https://github.com/circe/circe/blob/master/docs/src/main/tut/codec.md
object GranularityType extends EnumCodec[Granularity] {
  case object All           extends Granularity
  case object None          extends Granularity
  case object Second        extends Granularity
  case object Minute        extends Granularity
  case object FifteenMinute extends Granularity
  case object ThirtyMinute  extends Granularity
  case object Hour          extends Granularity
  case object Day           extends Granularity
  case object Week          extends Granularity
  case object Month         extends Granularity
  case object Quarter       extends Granularity
  case object Year          extends Granularity
  val values: Set[Granularity] = sealerate.values[Granularity]
}
