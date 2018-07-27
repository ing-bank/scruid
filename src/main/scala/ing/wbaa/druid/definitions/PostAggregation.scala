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
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

sealed trait PostAggregationType extends Enum with CamelCaseEnumStringEncoder
object PostAggregationType extends EnumCodec[PostAggregationType] {
  case object Arithmetic extends PostAggregationType

  val values: Set[PostAggregationType] = sealerate.values[PostAggregationType]
}

trait PostAggregation {
  val `type`: PostAggregationType
  val name: String
}

object PostAggregation {
  implicit val encoder: Encoder[PostAggregation] = new Encoder[PostAggregation] {
    final def apply(pAgg: PostAggregation): Json =
      (pAgg match {
        case x: ArithmeticPostAggregation => x.asJsonObject
      }).add("type", pAgg.`type`.asJson).asJson
  }
}

case class ArithmeticPostAggregation(name: String,
                                     fn: String,
                                     fields: (PostAggregator, PostAggregator))
    extends PostAggregation {
  override val `type` = PostAggregationType.Arithmetic
}

sealed trait PostAggregatorType extends Enum with CamelCaseEnumStringEncoder
object PostAggregatorType extends EnumCodec[PostAggregatorType] {
  case object FieldAccess extends PostAggregatorType
  case object Constant    extends PostAggregatorType

  val values: Set[PostAggregatorType] = sealerate.values[PostAggregatorType]
}

trait PostAggregator {
  val `type`: PostAggregatorType
  val name: String
}

object PostAggregator {
  implicit val encoder: Encoder[PostAggregator] = new Encoder[PostAggregator] {
    final def apply(field: PostAggregator): Json =
      (field match {
        case x: FieldAccessPostAggregator => x.asJsonObject
        case x: ConstantPostAggregator    => x.asJsonObject
      }).add("type", field.`type`.asJson).asJson
  }
}
case class FieldAccessPostAggregator(name: String, fieldName: String) extends PostAggregator {
  override val `type`: PostAggregatorType = PostAggregatorType.FieldAccess
}
case class ConstantPostAggregator(name: String, value: Double) extends PostAggregator {
  override val `type`: PostAggregatorType = PostAggregatorType.Constant
}
