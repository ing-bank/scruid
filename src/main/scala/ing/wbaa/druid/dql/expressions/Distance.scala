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

package ing.wbaa.druid.dql.expressions

object Distance {
  type Unit = DistanceUnit.Value

  final val DegreesToRadians = math.Pi / 180
  final val RadiansToDegrees = 1 / DegreesToRadians

  /**
    * See http://en.wikipedia.org/wiki/Earth_radius for details
    */
  final val EarthEquatorialRadiusKm = 6378.1370
  final val KmToMiles               = 0.621371192
  final val EarthEquatorialRadiusMi = EarthEquatorialRadiusKm * KmToMiles

  object DistanceUnit extends Enumeration {

    final val KM      = Value(0, "KM")
    final val MI      = Value(1, "MI")
    final val DEGREES = Value(2, "DEGREES")

    final val Kilometers = KM
    final val Miles      = MI
    final val Degrees    = DEGREES

  }

  def toDegrees(distance: Double, unit: DistanceUnit.Value): Double =
    unit match {
      case DistanceUnit.KM =>
        val radians = distance / EarthEquatorialRadiusKm
        radians * RadiansToDegrees

      case DistanceUnit.MI =>
        val radians = distance / EarthEquatorialRadiusMi

        radians * RadiansToDegrees

      case DistanceUnit.DEGREES =>
        distance

    }

}
