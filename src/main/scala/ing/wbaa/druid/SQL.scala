package ing.wbaa.druid

import java.sql.Timestamp
import java.time.{ Instant, LocalDate, LocalDateTime }

import scala.language.implicitConversions

object SQL {

  implicit def char2Param(v: Char): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Char, v.toString)

  implicit def string2Param(v: String): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Varchar, v)

  implicit def byte2Param(v: Byte): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Tinyint, v.toString)

  implicit def short2Param(v: Short): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Smallint, v.toString)

  implicit def int2Param(v: Int): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Integer, v.toString)

  implicit def long2Param(v: Long): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Bigint, v.toString)

  implicit def float2Param(v: Float): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Float, v.toString)

  implicit def double2Param(v: Double): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Double, v.toString)

  implicit def boolean2Param(v: Boolean): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Boolean, v.toString)

  implicit def localDate2Param(v: LocalDate)(implicit config: DruidConfig =
                                               DruidConfig.DefaultConfig): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Date, v.format(config.FormatterDate))

  implicit def localDateTime2Param(
      v: LocalDateTime
  )(implicit config: DruidConfig = DruidConfig.DefaultConfig): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Timestamp, v.format(config.FormatterDateTime))

  implicit def timestamp2Param(v: Timestamp)(implicit config: DruidConfig =
                                               DruidConfig.DefaultConfig): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Timestamp, config.FormatterDateTime.format(v.toInstant))

  implicit def instant2Param(
      v: Instant
  )(implicit config: DruidConfig = DruidConfig.DefaultConfig): SQLQueryParameter =
    SQLQueryParameter(SQLQueryParameterType.Timestamp, config.FormatterDateTime.format(v))

  implicit class StringToSQL(val sc: StringContext) extends AnyVal {

    def dsql(parameters: SQLQueryParameter*)(
        implicit context: Map[String, String] = Map.empty,
        config: DruidConfig = DruidConfig.DefaultConfig
    ): SQLQuery = {
      sc.checkLengths(parameters)
      val query = sc.parts.map(StringContext.treatEscapes).mkString("?")
      SQLQuery(query, context, parameters)(config)
    }

  }

}
