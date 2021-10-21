package app.logflare.sql

import gudusoft.gsqlparser.EDbObjectType
import gudusoft.gsqlparser.ETableSource
import gudusoft.gsqlparser.TBaseType
import gudusoft.gsqlparser.TSourceToken
import gudusoft.gsqlparser.nodes.*
import gudusoft.gsqlparser.stmt.TSelectSqlStatement
import java.util.*

internal abstract class TableVisitor : TParseTreeVisitor() {
    private val cte: MutableList<TCTEList> = mutableListOf()

    private fun isInCTE(name: String): Boolean {
        return cte.any {
            it.cteNames?.get(TBaseType.getTextWithoutQuoted(name).uppercase(Locale.getDefault())) != null
        }
    }

    override fun postVisit(node: TCTE?) {
        node!!.subquery.acceptChildren(this)
        super.postVisit(node)
    }

    override fun preVisit(node: TSelectSqlStatement?) {
        if (node!!.cteList != null) {
            cte.add(node.cteList)
        }
        super.preVisit(node)
    }

    override fun postVisit(node: TSelectSqlStatement?) {
        node!!.tables.forEach { table ->
            val name = table.tableName.tableString
            // if table is not coming from CTE
            if (!isInCTE(name)) {
                if (table.tableType == ETableSource.unnest) {
                    table.unnestClause.arrayExpr.acceptChildren(object : TParseTreeVisitor() {
                        override fun postVisit(stmt: TObjectName?) {
                            if (stmt!!.dbObjectType == EDbObjectType.column) {
                                if (stmt.sourceTable?.aliasClause == null) {
                                    stmt.objectToken.setString("`${stmt.sourceTable!!.fullTableName()}`")
                                } else {
                                    if (stmt.objectToken == null) {
                                        stmt.objectToken = TSourceToken()
                                    }
                                    stmt.objectToken.setString(stmt.sourceTable.aliasClause.toString())
                                }
                            }
                            super.postVisit(stmt)
                        }
                    })
                } else {
                    visit(table, node)
                }
            }
        }
        if (node.cteList != null) {
            if (cte.size > 0) {
                cte.removeLast()
            }
        }
        super.postVisit(node)
    }

    abstract fun visit(table: TTable?, node: TParseTreeNode)
}

