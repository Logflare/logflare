package app.logflare.sql

import gudusoft.gsqlparser.nodes.*
import gudusoft.gsqlparser.stmt.TSelectSqlStatement

internal class SandboxVisitor(private val statement: TSelectSqlStatement) : RestrictedPatternVisitor() {
    override fun visit(table: TTable?, node: TParseTreeNode) {
        if (!(isInCTE(table!!.fullTableName()) ||
                    (statement.cteList != null && isInCTE(listOf(statement.cteList), table.fullTableName())))) {
            throw SandboxRestrictionViolated(table.fullTableName())
        }
    }

}
