package app.logflare.sql

import gudusoft.gsqlparser.EDbVendor
import org.junit.jupiter.api.assertThrows
import java.util.*
import kotlin.test.*

internal class QueryTransformerTest {

    val projectId = "project"
    val userId: Long = 1234

    private fun datasetResolver() =
        object : DatasetResolver<Source> {
            override fun resolve(t: Source): String =
                "${t.userId}_test"
        }

    private fun sourceResolver() =
        object : SourceResolver {
            override fun resolve(source: String): Source =
                Source(token = UUID.nameUUIDFromBytes(source.toByteArray()), userId = userId)
        }

    private fun tableName(sourceName: String): String  {
        val source = sourceResolver().resolve(sourceName)
        return "${projectId}.${datasetResolver().resolve(source)}.${DefaultTableResolver.resolve(source)}"
    }

    private fun queryTransformer(query: String, dbVendor: EDbVendor = EDbVendor.dbvbigquery): QueryTransformer {
        return QueryTransformer(
            query,
            sourceResolver = sourceResolver(),
            projectId = projectId,
            datasetResolver = datasetResolver(),
            dbVendor = dbVendor,
        )
    }

    @Test
    fun testTableNameSubstitutionWhereClause() {
        assertEquals(
            "SELECT a,b,c FROM ${tableName("source")} WHERE ${tableName("source")}.d > 4",
            queryTransformer("SELECT a,b,c FROM source WHERE source.d > 4").transform())
    }

    @Test
    fun testTableNameSubstitutionSelectClause() {
        assertEquals(
            "SELECT ${tableName("source")}.a,b,c FROM ${tableName("source")}",
            queryTransformer("SELECT source.a,b,c FROM source").transform())
    }

    @Test
    fun testTableNameSubstitutionOrderClause() {
        assertEquals(
            "SELECT a,b,c FROM ${tableName("source")} ORDER BY ${tableName("source")}.d",
            queryTransformer("SELECT a,b,c FROM source ORDER BY source.d").transform())
    }

    @Test
    fun testTableNameSubstitutionGroupClause() {
        assertEquals(
            "SELECT a,b,c FROM ${tableName("source")} GROUP BY ${tableName("source")}.d",
            queryTransformer("SELECT a,b,c FROM source GROUP BY source.d").transform())
    }

    @Test
    fun testTableNameSubstitutionHavingClause() {
        assertEquals(
            "SELECT a,b,c FROM ${tableName("source")} GROUP BY ${tableName("source")}.d HAVING COUNT(${tableName("source")}.e) > 3",
            queryTransformer("SELECT a,b,c FROM source GROUP BY source.d HAVING COUNT(source.e) > 3").transform())
    }

    @Test
    fun testTableNameSubstitutionWhereClauseWithAlias() {
        assertEquals(
            "SELECT a,b,c FROM ${tableName("source")} src WHERE src.d > 4",
            queryTransformer("SELECT a,b,c FROM source src WHERE src.d > 4").transform())
    }

    @Test
    fun testJoin() {
        assertEquals(
            "SELECT a, b, c FROM ${tableName("source")} LEFT JOIN ${tableName("anotherSource")} " +
                    "ON ${tableName("source")}.d = ${tableName("anotherSource")}.e",
            queryTransformer(
                "SELECT a, b, c FROM source LEFT JOIN anotherSource " +
                        "ON source.d = anotherSource.e"
            ).transform()
        )
    }

    @Test
    fun testSubQuery() {
        assertEquals(
            "SELECT a FROM (SELECT a FROM ${tableName("source")})",
            queryTransformer(
                    "SELECT a FROM (SELECT a FROM source)"
            ).transform())
    }

    @Test
    fun testCTE() {
        assertEquals(
            "WITH something AS (SELECT a,b,c FROM ${tableName("source")} WHERE d > 4) SELECT a FROM something UNION SELECT a FROM ${tableName("something1")}",
            queryTransformer(
                "WITH something AS (SELECT a,b,c FROM source WHERE d > 4) SELECT a FROM something UNION SELECT a FROM something1"
                ).transform())
    }

    @Test
    fun testRecursiveCTE() {
        assertEquals(
            "WITH something AS (SELECT a,b,c FROM ${tableName("source")} WHERE d > 4 UNION SELECT a FROM something) " +
                    "SELECT a FROM something",
            queryTransformer(
                "WITH something AS (SELECT a,b,c FROM source WHERE d > 4 UNION SELECT a FROM something) " +
                        "SELECT a FROM something"
            ).transform()
        )
    }
    @Test
    fun testReferenceCTE() {
        assertEquals(
            "WITH something AS (SELECT a,b,c FROM ${tableName("source")} WHERE d > 4), " +
                    "something1 AS (SELECT a FROM something) " +
                    "SELECT a FROM something UNION SELECT a FROM something1",
            queryTransformer(
                "WITH something AS (SELECT a,b,c FROM source WHERE d > 4), " +
                        "something1 AS (SELECT a FROM something) " +
                        "SELECT a FROM something UNION SELECT a FROM something1"
            ).transform())
    }


    @Test
    fun testSelectStmtOnly() {
        assertThrows<SelectQueryRequired> {
            queryTransformer("UPDATE a SET x = 1").transform()
        }
        assertThrows<SelectQueryRequired> {
            queryTransformer("DELETE a WHERE x = 1").transform()
        }
        assertThrows<SelectQueryRequired> {
            queryTransformer("DROP TABLE a").transform()
        }
    }

    @Test
    fun testOneStmtOnly() {
        assertThrows<SingularQueryRequired> {
            queryTransformer("SELECT a FROM a; SELECT a FROM b").transform()
        }
    }

    @Test
    fun testRestrictedFunctions() {
        assertThrows<RestrictedFunctionCall> {
            queryTransformer("SELECT SESSION_USER()").transform()
        }
        assertThrows<RestrictedFunctionCall> {
            queryTransformer("SELECT EXTERNAL_QUERY('','')").transform()
        }
    }

    @Test
    fun testSelectIntoRestricted() {
        assertThrows<RestrictedIntoClause> {
            // Using a different vendor here because BigQuery does not
            // support SELECT INTO, but if/when logflare grows to support
            // other syntaxes, one'd wish we wouldn't have forgotten
            // something like this
            queryTransformer("SELECT a FROM a INTO b", dbVendor = EDbVendor.dbvpostgresql).transform()
        }
    }

    @Test
    fun testRestrictedWildcardSelect() {
        assertThrows<RestrictedWildcardResultColumn> {
            queryTransformer("SELECT * FROM a").transform()
        }

        assertThrows<RestrictedWildcardResultColumn> {
            queryTransformer("SELECT a.* FROM a").transform()
        }

        assertThrows<RestrictedWildcardResultColumn> {
            queryTransformer("SELECT q, a.* FROM a").transform()
        }

        assertThrows<RestrictedWildcardResultColumn> {
            queryTransformer("SELECT a FROM (SELECT * FROM a)").transform()
        }

        assertThrows<RestrictedWildcardResultColumn> {
            queryTransformer("WITH q AS (SELECT a FROM a) SELECT * FROM q").transform()
        }

        assertThrows<RestrictedWildcardResultColumn> {
            queryTransformer("SELECT a FROM a UNION ALL SELECT * FROM b").transform()
        }
    }

}
