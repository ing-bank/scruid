package ing.wbaa.druid
package definitions

import io.circe._
import ca.mrvisser.sealerate

sealed trait Order extends Enum with LowerCaseEnumStringEncoder

object Order {
  implicit val orderEncoder: Encoder[Order] = OrderType.encoder
  implicit val orderDecoder: Decoder[Order] = OrderType.decoder
}

object OrderType extends EnumCodec[Order] {

  case object Ascending extends Order
  case object Decending extends Order
  case object None      extends Order

  val values: Set[Order] = sealerate.values[Order]
}
