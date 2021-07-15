package app.logflare.sql

import gudusoft.gsqlparser.EDbVendor
import gudusoft.gsqlparser.TGSqlParser
import gudusoft.gsqlparser.nodes.TTable
import gudusoft.gsqlparser.stmt.TSelectSqlStatement

/**
 * Main entry point to Logflare SQL functionality
 *
 * Provides extraction & transformation functionality for a given query
 */
class QueryProcessor(
    private val query: String,
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

    private fun parse() {
        if (parser.parse() != 0) {
            throw QueryParseError(parser.errormessage)
        }
        if (parser.sqlstatements.size() != 1) {
            throw SingularQueryRequired()
        }
        if (parser.sqlstatements[0] !is TSelectSqlStatement) {
            throw SelectQueryRequired()
        }
    }

    /**
     * Transforms the query to be executed on BigQuery backend
     */
    fun transformForExecution(): String {
        parse()
        val statement = parser.sqlstatements[0]
        statement.acceptChildren(TransformerVisitor(projectId, sourceResolver, tableResolver, datasetResolver))
        return statement.toString()
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
            override fun visit(table: TTable?, select: TSelectSqlStatement) {
                sources.add(sourceResolver.resolve(table!!.fullTableName()))
            }
        })
        return sources
    }
}
