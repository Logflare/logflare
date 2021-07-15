package app.logflare.sql

import gudusoft.gsqlparser.EDbVendor
import org.junit.jupiter.api.assertThrows
import java.util.*
import kotlin.test.*

internal class QueryProcessorTest {

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
                Source(token = UUID.nameUUIDFromBytes(source.toByteArray()), name = source, userId = userId)
        }

    private fun tableName(sourceName: String): String  {
        val source = sourceResolver().resolve(sourceName)
        return "`${projectId}.${datasetResolver().resolve(source)}.${DefaultTableResolver.resolve(source)}`"
    }

    private fun queryProcessor(query: String, dbVendor: EDbVendor = EDbVendor.dbvbigquery): QueryProcessor {
        return QueryProcessor(
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
            queryProcessor("SELECT a,b,c FROM source WHERE source.d > 4").transformForExecution())
    }

    @Test
    fun testTableNameSubstitutionSelectClause() {
        assertEquals(
            "SELECT ${tableName("source")}.a,b,c FROM ${tableName("source")}",
            queryProcessor("SELECT source.a,b,c FROM source").transformForExecution())
    }

    @Test
    fun testTableNameSubstitutionOrderClause() {
        assertEquals(
            "SELECT a,b,c FROM ${tableName("source")} ORDER BY ${tableName("source")}.d",
            queryProcessor("SELECT a,b,c FROM source ORDER BY source.d").transformForExecution())
    }

    @Test
    fun testTableNameSubstitutionGroupClause() {
        assertEquals(
            "SELECT a,b,c FROM ${tableName("source")} GROUP BY ${tableName("source")}.d",
            queryProcessor("SELECT a,b,c FROM source GROUP BY source.d").transformForExecution())
    }

    @Test
    fun testTableNameSubstitutionHavingClause() {
        assertEquals(
            "SELECT a,b,c FROM ${tableName("source")} GROUP BY ${tableName("source")}.d HAVING COUNT(${tableName("source")}.e) > 3",
            queryProcessor("SELECT a,b,c FROM source GROUP BY source.d HAVING COUNT(source.e) > 3").transformForExecution())
    }

    @Test
    fun testTableNameSubstitutionWhereClauseWithAlias() {
        assertEquals(
            "SELECT a,b,c FROM ${tableName("source")} src WHERE src.d > 4",
            queryProcessor("SELECT a,b,c FROM source src WHERE src.d > 4").transformForExecution())
    }

    @Test
    fun testJoin() {
        assertEquals(
            "SELECT a, b, c FROM ${tableName("source")} LEFT JOIN ${tableName("anotherSource")} " +
                    "ON ${tableName("source")}.d = ${tableName("anotherSource")}.e",
            queryProcessor(
                "SELECT a, b, c FROM source LEFT JOIN anotherSource " +
                        "ON source.d = anotherSource.e"
            ).transformForExecution()
        )
    }

    @Test
    fun testSubQuery() {
        assertEquals(
            "SELECT a FROM (SELECT a FROM ${tableName("source")})",
            queryProcessor(
                    "SELECT a FROM (SELECT a FROM source)"
            ).transformForExecution())
    }

    @Test
    fun testCTE() {
        assertEquals(
            "WITH something AS (SELECT a,b,c FROM ${tableName("source")} WHERE d > 4) SELECT a FROM something UNION SELECT a FROM ${tableName("something1")}",
            queryProcessor(
                "WITH something AS (SELECT a,b,c FROM source WHERE d > 4) SELECT a FROM something UNION SELECT a FROM something1"
                ).transformForExecution())
    }

    @Test
    fun testRecursiveCTE() {
        assertEquals(
            "WITH something AS (SELECT a,b,c FROM ${tableName("source")} WHERE d > 4 UNION SELECT a FROM something) " +
                    "SELECT a FROM something",
            queryProcessor(
                "WITH something AS (SELECT a,b,c FROM source WHERE d > 4 UNION SELECT a FROM something) " +
                        "SELECT a FROM something"
            ).transformForExecution()
        )
    }
    @Test
    fun testReferenceCTE() {
        assertEquals(
            "WITH something AS (SELECT a,b,c FROM ${tableName("source")} WHERE d > 4), " +
                    "something1 AS (SELECT a FROM something) " +
                    "SELECT a FROM something UNION SELECT a FROM something1",
            queryProcessor(
                "WITH something AS (SELECT a,b,c FROM source WHERE d > 4), " +
                        "something1 AS (SELECT a FROM something) " +
                        "SELECT a FROM something UNION SELECT a FROM something1"
            ).transformForExecution())
    }


    @Test
    fun testSelectStmtOnly() {
        assertThrows<SelectQueryRequired> {
            queryProcessor("UPDATE a SET x = 1").transformForExecution()
        }
        assertThrows<SelectQueryRequired> {
            queryProcessor("DELETE a WHERE x = 1").transformForExecution()
        }
        assertThrows<SelectQueryRequired> {
            queryProcessor("DROP TABLE a").transformForExecution()
        }
    }

    @Test
    fun testOneStmtOnly() {
        assertThrows<SingularQueryRequired> {
            queryProcessor("SELECT a FROM a; SELECT a FROM b").transformForExecution()
        }
    }

    @Test
    fun testRestrictedFunctions() {
        assertThrows<RestrictedFunctionCall> {
            queryProcessor("SELECT SESSION_USER()").transformForExecution()
        }
        assertThrows<RestrictedFunctionCall> {
            queryProcessor("SELECT EXTERNAL_QUERY('','')").transformForExecution()
        }
    }

    @Test
    fun testSelectIntoRestricted() {
        assertThrows<RestrictedIntoClause> {
            // Using a different vendor here because BigQuery does not
            // support SELECT INTO, but if/when logflare grows to support
            // other syntaxes, one'd wish we wouldn't have forgotten
            // something like this
            queryProcessor("SELECT a FROM a INTO b", dbVendor = EDbVendor.dbvpostgresql).transformForExecution()
        }
    }

    @Test
    fun testRestrictedWildcardSelect() {
        assertThrows<RestrictedWildcardResultColumn> {
            queryProcessor("SELECT * FROM a").transformForExecution()
        }

        assertThrows<RestrictedWildcardResultColumn> {
            queryProcessor("SELECT a.* FROM a").transformForExecution()
        }

        assertThrows<RestrictedWildcardResultColumn> {
            queryProcessor("SELECT q, a.* FROM a").transformForExecution()
        }

        assertThrows<RestrictedWildcardResultColumn> {
            queryProcessor("SELECT a FROM (SELECT * FROM a)").transformForExecution()
        }

        assertThrows<RestrictedWildcardResultColumn> {
            queryProcessor("WITH q AS (SELECT a FROM a) SELECT * FROM q").transformForExecution()
        }

        assertThrows<RestrictedWildcardResultColumn> {
            queryProcessor("SELECT a FROM a UNION ALL SELECT * FROM b").transformForExecution()
        }
    }

    @Test
    fun testParameterExtraction() {
        assertEquals(queryProcessor("SELECT a, @a FROM b WHERE c = @c OR d > @c").parameters(), setOf("c", "a"))
    }

    @Test
    fun testSourceExtraction() {
        assertEquals(queryProcessor("SELECT a, b FROM a,b,c").sources(),
            setOf(sourceResolver().resolve("a"),
                sourceResolver().resolve("b"),
                sourceResolver().resolve("c")))
    }

    @Test
    fun testSensibleErrorMessages() {
        val exc = assertThrows<QueryParseError> {
            queryProcessor("ZZ").transformForExecution()
        }
        assertFalse(exc.message!!.contains("tokenlize"))
        assert(exc.message!!.contains("tokenizing"))
    }

}
