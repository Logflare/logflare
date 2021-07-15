package app.logflare.sql

import gudusoft.gsqlparser.nodes.*
import gudusoft.gsqlparser.stmt.TSelectSqlStatement
import java.util.*

internal class TransformerVisitor(
    private val projectId: String,
    private val sourceResolver: SourceResolver,
    private val tableResolver: TableResolver<Source>,
    private val datasetResolver: DatasetResolver<Source>
) : TableVisitor() {

    override fun postVisit(node: TSelectSqlStatement?) {
        val hasWildcard = node!!.resultColumnList.any {
            it.columnNameOnly == "*"
        }
        if (hasWildcard) {
            throw RestrictedWildcardResultColumn()
        }
        super.postVisit(node)
    }

    override fun visit(table: TTable?, select: TSelectSqlStatement) {
        val name = table!!.fullTableName()
        val source = sourceResolver.resolve(name)
        val newName = "`${projectId}.${datasetResolver.resolve(source)}.${tableResolver.resolve(source)}`"
        table.tableName.setString(newName)
        val tableRenamer = object : TParseTreeVisitor() {
            override fun postVisit(node: TObjectName?) {
                if (node?.objectToken != null && node.tableString == name) {
                    node.objectToken.setString(newName)
                }
            }
        }
        select.acceptChildren(tableRenamer)
        // I don't know why, but GSP does not visit GROUP BY's
        // HAVING clause (or anything else beyond `items`, really)
        select.groupByClause?.havingClause?.acceptChildren(tableRenamer)
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
