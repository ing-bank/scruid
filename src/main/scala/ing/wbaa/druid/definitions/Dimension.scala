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

import ca.mrvisser.sealerate
import io.circe.Encoder
import io.circe.generic.auto._
import io.circe.syntax._

sealed trait DimensionType extends Enum with CamelCaseEnumStringEncoder
object DimensionType extends EnumCodec[DimensionType] {

  case object Default    extends DimensionType
  case object Extraction extends DimensionType

  val values: Set[DimensionType] = sealerate.values[DimensionType]
}

sealed trait Dimension {
  val `type`: DimensionType
}

object Dimension {

  def apply(
      dimension: String,
      outputName: Option[String] = None,
      outputType: Option[String] = None
  ): DefaultDimension =
    DefaultDimension(dimension, outputName, outputType)

  implicit val encoder: Encoder[Dimension] = new Encoder[Dimension] {
    override def apply(dimension: Dimension) =
      (dimension match {
        case x: DefaultDimension    => x.asJsonObject
        case x: ExtractionDimension => x.asJson.asObject.get
      }).add("type", dimension.`type`.asJson).asJson
  }
}

case class DefaultDimension(
    dimension: String,
    outputName: Option[String] = None,
    outputType: Option[String] = None
) extends Dimension {
  override val `type` = DimensionType.Default
}

case class ExtractionDimension(
    dimension: String,
    outputName: Option[String] = None,
    outputType: Option[String] = None,
    extractionFn: ExtractionFn
) extends Dimension {
  override val `type` = DimensionType.Extraction
}
