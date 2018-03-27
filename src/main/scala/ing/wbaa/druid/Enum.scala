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

import io.circe._
import io.circe.syntax._

trait Enum {
  override lazy val toString: String = this.getClass.getSimpleName.split("\\$")(0)
}

trait EnumCodec[T <: Enum with EnumStringEncoder] {
  val values: Set[T]
  def decode(input: String): Either[NoSuchElementException, T] =
    values.find { enum =>
      enum.encode == input
    } match {
      case Some(value) => Right(value)
      case None =>
        val transformedValues = values.map(v => v.encode).mkString("[", ",", "]")
        Left(new NoSuchElementException(s"'$input' is not a valid value in $transformedValues"))
    }

  implicit val encoder: Encoder[T] = new Encoder[T] {
    final def apply(t: T): Json = t.encode.asJson
  }
  implicit val decoder: Decoder[T] = new Decoder[T] {
    final def apply(c: HCursor): Decoder.Result[T] =
      for {
        enum <- c.as[String]
      } yield {
        decode(enum) match {
          case Left(e)  => throw e
          case Right(t) => t
        }
      }
  }
}

trait EnumStringEncoder { this: Enum =>
  def encode(): String
}

trait UpperCaseEnumStringEncoder extends EnumStringEncoder { this: Enum =>
  def encode() = toString
}

trait CamelCaseEnumStringEncoder extends EnumStringEncoder { this: Enum =>
  private def decapitalize(input: String) = input.head.toLower + input.tail
  def encode() = decapitalize(toString)
}

trait LispCaseEnumStringEncoder extends EnumStringEncoder { this: Enum =>
  def encode() = DelimiterSeparatedEnumEncoder("-").encode(toString)
}

trait SnakeCaseEnumStringEncoder extends EnumStringEncoder { this: Enum =>
  def encode() = DelimiterSeparatedEnumEncoder("_").encode(toString)
}

case class DelimiterSeparatedEnumEncoder(delimiter: String) {
  import java.util.Locale
  private val PASS1       = """([A-Z]+)([A-Z][a-z])""".r
  private val PASS2       = """([a-z\d])([A-Z])""".r
  private val REPLACEMENT = "$1" + delimiter + "$2"
  def encode(input: String) =
    PASS2.replaceAllIn(PASS1.replaceAllIn(input, REPLACEMENT), REPLACEMENT).toLowerCase(Locale.US)
}
