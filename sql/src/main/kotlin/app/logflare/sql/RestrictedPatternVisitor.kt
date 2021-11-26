package app.logflare.sql

import gudusoft.gsqlparser.nodes.TFunctionCall
import gudusoft.gsqlparser.nodes.TIntoClause
import gudusoft.gsqlparser.stmt.TSelectSqlStatement

internal abstract class RestrictedPatternVisitor : TableVisitor() {
    override fun postVisit(node: TSelectSqlStatement?) {
        val hasWildcard = node!!.resultColumnList.any {
            it.columnNameOnly == "*"
        }
        if (hasWildcard) {
            throw RestrictedWildcardResultColumn()
        }
        super.postVisit(node)
    }

    override fun postVisit(node: TFunctionCall?) {
        if (node!!.functionName.objectString.equals("external_query", ignoreCase = true) ||
            node.functionName.objectString.equals("session_user", ignoreCase = true)) {
            throw RestrictedFunctionCall(node.functionName.objectString)
        }
        super.postVisit(node)
    }

    override fun preVisit(node: TIntoClause?) {
        throw RestrictedIntoClause(node!!.toString())
    }
}
