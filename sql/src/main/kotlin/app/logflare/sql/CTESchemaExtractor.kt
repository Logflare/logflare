package app.logflare.sql

import gudusoft.gsqlparser.EExpressionType
import gudusoft.gsqlparser.EFunctionType
import gudusoft.gsqlparser.nodes.TAliasClause
import gudusoft.gsqlparser.nodes.TCTE
import gudusoft.gsqlparser.nodes.TExpression
import gudusoft.gsqlparser.nodes.TParseTreeVisitor
import gudusoft.gsqlparser.stmt.TSelectSqlStatement

class CTESchemaExtractor(): TParseTreeVisitor() {

    val schema: MutableMap<String, MutableMap<String, String>> = mutableMapOf()

    private var currentCte: String? = null

    override fun preVisit(node: TCTE?) {
        currentCte = node!!.tableName.objectString
        super.postVisit(node)
    }

    override fun postVisit(node: TCTE?) {
        node!!.subquery.acceptChildren(this)
        currentCte = null
        super.postVisit(node)
    }

    override fun postVisit(node: TSelectSqlStatement?) {
        if (currentCte != null) {
            val cteSchema = schema.getOrPut(currentCte!!) { mutableMapOf() }
            val columns = node!!.resultColumnList
            columns.forEach { col ->
                val name = col.columnNameOnly.ifEmpty { col.columnAlias }
                if (name.isNotEmpty()) {
                    if (col.expr is TExpression && col.expr.expressionType == EExpressionType.function_t &&
                            col.expr.functionCall.functionType == EFunctionType.cast_t) {
                        cteSchema[name] = col.expr.functionCall.typename.dataTypeName
                    } else {
                        cteSchema[name] = "any"
                    }
                }
            }
        }
        super.postVisit(node)
    }

}
