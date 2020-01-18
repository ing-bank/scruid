package ing.wbaa.druid
package definitions

import io.circe._
import ca.mrvisser.sealerate

sealed trait Boundary extends Enum with CamelCaseEnumStringEncoder

object Boundary {
  implicit val boundEncoder: Encoder[Boundary] = BoundaryType.encoder
  implicit val boundDecoder: Decoder[Boundary] = BoundaryType.decoder
}

object BoundaryType extends EnumCodec[Boundary] {
  case object MaxTime extends Boundary
  case object MinTime extends Boundary
  val values: Set[Boundary] = sealerate.values[Boundary]
}
