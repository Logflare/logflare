package app.logflare.sql

import gudusoft.gsqlparser.nodes.*
import gudusoft.gsqlparser.stmt.TSelectSqlStatement
import java.util.*

internal class SourceMappingVisitor(
    private val sourceMapping: Map<String, UUID>,
    private val sourceResolver: SourceResolver
) : TableVisitor() {

    override fun visit(table: TTable?, node: TParseTreeNode) {
        val originalName = table!!.fullTableName()
        val newName = ensureValidName(sourceResolver.findByUUID(sourceMapping[originalName]!!).name)
        table.tableName.setString(newName)
        val tableRenamer = object : TParseTreeVisitor() {
            override fun postVisit(node: TObjectName?) {
                if (node?.objectToken != null && node.tableString == originalName) {
                    node.objectToken.setString(newName)
                }
            }
        }
        node.acceptChildren(tableRenamer)
        // I don't know why, but GSP does not visit GROUP BY's
        // HAVING clause (or anything else beyond `items`, really)
        if (node is TSelectSqlStatement) {
            node.groupByClause?.havingClause?.acceptChildren(tableRenamer)
        }
    }

    private fun ensureValidName(name: String): String {
        return if (name.matches(Regex("^[_a-zA-Z][_a-zA-Z0-9]*$"))) {
            name
        } else {
            "`${name}`"
        }
    }

}

