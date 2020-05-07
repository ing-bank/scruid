package ing.wbaa.druid

object SQL {

  implicit class StringToSQL(val sc: StringContext) extends AnyVal {
    def dsql(query: Any*)(implicit context: Map[String, String] = Map.empty,
                          config: DruidConfig = DruidConfig.DefaultConfig): SQLQuery =
      SQLQuery(sc.s(query: _*), context)(config)
  }

}
