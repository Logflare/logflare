package app.logflare.sql

import gudusoft.gsqlparser.nodes.*
import gudusoft.gsqlparser.stmt.TSelectSqlStatement
import java.util.*

internal class SourceMappingVisitor(
    private val sourceMapping: Map<String, UUID>,
    private val sourceResolver: SourceResolver
) : TableVisitor() {

    override fun visit(table: TTable?, select: TSelectSqlStatement) {
        val originalName = table!!.tableName.tableString
        val newName = sourceResolver.findByUUID(sourceMapping[originalName]!!).name
        table.tableName.setString(newName)
        val tableRenamer = object : TParseTreeVisitor() {
            override fun postVisit(node: TObjectName?) {
                if (node?.objectToken != null && node.tableString == originalName) {
                    node.objectToken.setString(newName)
                }
            }
        }
        select.acceptChildren(tableRenamer)
        // I don't know why, but GSP does not visit GROUP BY's
        // HAVING clause (or anything else beyond `items`, really)
        select.groupByClause?.havingClause?.acceptChildren(tableRenamer)
    }

}

