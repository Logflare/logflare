package app.logflare.sql

import gudusoft.gsqlparser.EDbVendor
import gudusoft.gsqlparser.ETokenType
import gudusoft.gsqlparser.TGSqlParser
import gudusoft.gsqlparser.nodes.TParseTreeNode
import gudusoft.gsqlparser.nodes.TTable
import gudusoft.gsqlparser.stmt.TSelectSqlStatement
import java.util.*

/**
 * Main entry point to Logflare SQL functionality
 *
 * Provides extraction & transformation functionality for a given query
 */
class QueryProcessor(
    private val query: String,
    private val sandboxedQuery: String? = null,
    private val sourceResolver: SourceResolver,
    private val projectId: String,
    private val tableResolver: TableResolver<Source> = DefaultTableResolver,
    private val datasetResolver: DatasetResolver<Source>,
    private val dbVendor: EDbVendor = EDbVendor.dbvbigquery,
) {

    private val parser : TGSqlParser by lazy {
        val parser = TGSqlParser(dbVendor)
        parser.sqltext = query
        parser
    }

    private val sandboxedParser : TGSqlParser? by lazy {
        var parser: TGSqlParser? = null
        if (sandboxedQuery != null) {
            parser = TGSqlParser(dbVendor)
            parser.sqltext = sandboxedQuery
        }
        parser
    }

    private fun parse() {
        if (parser.parse() != 0) {
            throw QueryParseError(parser.sqltext, parser.syntaxErrors)
        }
        if (parser.sqlstatements.size() != 1) {
            throw SingularQueryRequired(parser.sqltext)
        }
        if (parser.sqlstatements[0] !is TSelectSqlStatement) {
            throw SelectQueryRequired(parser.sqltext)
        }

        if (sandboxedParser != null) {
            val p = sandboxedParser!!
            if (p.parse() != 0) {
                throw QueryParseError(p.sqltext, p.syntaxErrors)
            }
            if (p.sqlstatements.size() != 1) {
                throw SingularQueryRequired(p.sqltext)
            }
            if (p.sqlstatements[0] !is TSelectSqlStatement) {
                throw SelectQueryRequired(p.sqltext)
            }

        }
    }

    /**
     * Transforms the query to be executed on BigQuery backend
     */
    fun transformForExecution(): String {
        parse()
        val statement = parser.sqlstatements[0] as TSelectSqlStatement
        statement.acceptChildren(TransformerVisitor(projectId, sourceResolver, tableResolver, datasetResolver))
        if (sandboxedParser != null) {
            val p = sandboxedParser!!
            val sandboxSelect = p.sqlstatements[0] as TSelectSqlStatement
            sandboxSelect.acceptChildren(SandboxVisitor(statement))
            val sb = StringBuilder()
            if (statement.cteList != null) {
                sb.append("WITH ")
                sb.append(statement.cteList.toString())
                if (sandboxSelect.cteList != null) {
                    sb.append(", ")
                    sb.append(sandboxSelect.cteList.toString())
                    sandboxSelect.cteList = null
                    sandboxSelect.startToken = sandboxSelect.selectToken
                }
                sb.append(' ')
                sb.append(sandboxSelect.toString())
            } else {
                sb.append(sandboxSelect.toString())
            }
            return sb.toString()
        } else {
            return statement.toString()
        }
    }

    fun parameters(): Set<String> {
        parse()
        val statement = parser.sqlstatements[0]
        val extractor = ParameterExtractor()
        statement.acceptChildren(extractor)
        return extractor.parameters
    }

    fun sources(): Set<Source> {
        parse()
        val statement = parser.sqlstatements[0]
        val sources = mutableSetOf<Source>()
        statement.acceptChildren(object : TableVisitor() {
            override fun visit(table: TTable?, node: TParseTreeNode) {
                sources.add(sourceResolver.resolve(table!!.fullTableName()))
            }
        })
        return sources
    }

    fun mapSources(mapping: Map<String, UUID>): String {
        parse()
        val statement = parser.sqlstatements[0]
        statement.acceptChildren(SourceMappingVisitor(mapping, sourceResolver))
        return statement.toString()
    }

    fun cteSchema(): Map<String, Map<String, String>> {
        parse()
        val statement = parser.sqlstatements[0]
        val cteSchemaExtractor = CTESchemaExtractor()
        statement.cteList.acceptChildren(cteSchemaExtractor)
        return cteSchemaExtractor.schema
    }
}
