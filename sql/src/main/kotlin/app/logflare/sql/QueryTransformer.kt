package app.logflare.sql

import gudusoft.gsqlparser.EDbVendor
import gudusoft.gsqlparser.TGSqlParser
import gudusoft.gsqlparser.stmt.TSelectSqlStatement

/**
 * Main entry point to Logflare SQL functionality
 *
 * Translates given query to one that can be safely relayed to BigQuery
 */
class QueryTransformer(
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
     * Runs the transformation
     */
    fun transform(): String {
        parse()
        val statement = parser.sqlstatements[0]
        statement.acceptChildren(TransformerVisitor(projectId, sourceResolver, tableResolver, datasetResolver))
        return statement.toString()
    }
}
