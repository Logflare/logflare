package app.logflare.sql

import gudusoft.gsqlparser.TBaseType
import gudusoft.gsqlparser.nodes.TTable

fun TTable.fullTableName(): String {
  val fullName = this.fullName
  return if (fullName != null) {
    TBaseType.getTextWithoutQuoted(fullName)
  } else {
    this.name
  }
}
