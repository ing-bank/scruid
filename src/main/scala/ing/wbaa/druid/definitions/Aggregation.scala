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

sealed trait AggregationType {
  private def decapitalize(input: String) = {
    val chars = input.toCharArray
    chars(0) = Character.toLowerCase(chars(0))
    chars.mkString
  }
  lazy val value: String = decapitalize(this.getClass.getSimpleName.replace("$", ""))
}
object AggregationType {
  case object Count extends AggregationType
  case object LongSum extends AggregationType
  case object DoubleSum extends AggregationType
  case object DoubleMax extends AggregationType
  case object DoubleMin extends AggregationType
  case object LongMin extends AggregationType
  case object LongMax extends AggregationType
  case object DoubleFirst extends AggregationType
  case object DoubleLast extends AggregationType
  case object LongFirst extends AggregationType
  case object LongLast extends AggregationType

  val values = Set(Count, LongSum, DoubleSum, DoubleMax, DoubleMin, LongMin, LongMax, DoubleFirst, DoubleLast, LongFirst, LongLast)
}

trait Aggregation {
  val `type`: AggregationType
  val name: String
}

trait SingleFieldAggregation extends Aggregation {
  val fieldName: String
}

case class CountAggregation(name: String) extends Aggregation{ val `type` = AggregationType.Count }
case class LongSumAggregation(name: String, fieldName: String) extends SingleFieldAggregation{ val `type` = AggregationType.LongSum }
case class DoubleSumAggregation(name: String, fieldName: String) extends SingleFieldAggregation{ val `type` = AggregationType.DoubleSum }
case class DoubleMaxAggregation(name: String, fieldName: String) extends SingleFieldAggregation{ val `type` = AggregationType.DoubleMax }
case class DoubleMinAggregation(name: String, fieldName: String) extends SingleFieldAggregation{ val `type` = AggregationType.DoubleMin }
case class LongMinAggregation(name: String, fieldName: String) extends SingleFieldAggregation{ val `type` = AggregationType.LongMin }
case class LongMaxAggregation(name: String, fieldName: String) extends SingleFieldAggregation{ val `type` = AggregationType.LongMax }
case class DoubleFirstAggregation(name: String, fieldName: String) extends SingleFieldAggregation{ val `type` = AggregationType.DoubleFirst }
case class DoubleLastAggregation(name: String, fieldName: String) extends SingleFieldAggregation{ val `type` = AggregationType.DoubleLast }
case class LongFirstAggregation(name: String, fieldName: String) extends SingleFieldAggregation{ val `type` = AggregationType.LongFirst }
case class LongLastAggregation(name: String, fieldName: String) extends SingleFieldAggregation{ val `type` = AggregationType.LongLast }
