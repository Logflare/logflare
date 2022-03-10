package app.logflare.sql

import gudusoft.gsqlparser.TBaseType
import gudusoft.gsqlparser.nodes.*
import gudusoft.gsqlparser.stmt.TSelectSqlStatement

internal class TransformerVisitor(
    private val projectId: String,
    private val sourceResolver: SourceResolver,
    private val tableResolver: TableResolver<Source>,
    private val datasetResolver: DatasetResolver<Source>
) : RestrictedPatternVisitor() {

    private val visited = mutableSetOf<TTable>()

    override fun visit(table: TTable?, node: TParseTreeNode) {
        if (visited.contains(table!!)) {
            return;
        }
        visited.add(table)
        val name = table.fullTableName()
        val source = sourceResolver.resolve(name)
        val newName = "`${projectId}.${datasetResolver.resolve(source)}.${tableResolver.resolve(source)}`"
        table.tableName.setString(newName)
        val tableRenamer = object : TParseTreeVisitor() {
            override fun postVisit(node: TObjectName?) {
                if (node?.objectToken != null && TBaseType.getTextWithoutQuoted(node.tableString) == name) {
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

}
