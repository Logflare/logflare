package app.logflare.sql

import gudusoft.gsqlparser.TBaseType
import gudusoft.gsqlparser.nodes.*
import gudusoft.gsqlparser.stmt.TSelectSqlStatement
import java.util.*

internal class TransformerVisitor(
    private val projectId: String,
    private val sourceResolver: SourceResolver,
    private val tableResolver: TableResolver<Source>,
    private val datasetResolver: DatasetResolver<Source>
) : TParseTreeVisitor() {

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
                val source = sourceResolver.resolve(name)
                val newName = "${projectId}.${datasetResolver.resolve(source)}.${tableResolver.resolve(source)}"
                table.tableName.setString(newName)
                val tableRenamer = object : TParseTreeVisitor() {
                    override fun postVisit(node: TObjectName?) {
                        if (node?.objectToken != null && node.tableString == name) {
                            node.objectToken.setString(newName)
                        }
                    }
                }
                node.acceptChildren(tableRenamer)
                // I don't know why, but GSP does not visit GROUP BY's
                // HAVING clause (or anything else beyond `items`, really)
                node.groupByClause?.havingClause?.acceptChildren(tableRenamer)
            }
        }
        if (node.cteList != null) {
            if (cte.size > 0) {
                cte.removeLast()
            }
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
