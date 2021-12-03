package app.logflare.sql

import gudusoft.gsqlparser.EDbVendor
import org.junit.jupiter.api.assertDoesNotThrow
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

            override fun findByUUID(uuid: UUID): Source {
                TODO("Not yet implemented")
            }
        }

    private fun tableName(sourceName: String): String  {
        val source = sourceResolver().resolve(sourceName)
        return "`${projectId}.${datasetResolver().resolve(source)}.${DefaultTableResolver.resolve(source)}`"
    }

    private fun queryProcessor(query: String, sandboxedQuery: String? = null, dbVendor: EDbVendor = EDbVendor.dbvbigquery): QueryProcessor {
        return QueryProcessor(
            query,
            sandboxedQuery,
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
    fun testTableNameSubstitutionBackquoted() {
        assertEquals(
            "SELECT a,b,c FROM ${tableName("source")} WHERE ${tableName("source")}.d > 4",
            queryProcessor("SELECT a,b,c FROM `source` WHERE `source`.d > 4").transformForExecution())
    }

    @Test
    fun testTableNameSubstitutionBackquotedMatchingCTEName() {
        assertEquals(
            """
                WITH src as (
                  SELECT
                  name
                  FROM ${tableName("first.src")}
                )
                SELECT value FROM src
                """.trimIndent(),
            queryProcessor(
                """
                WITH src as (
                  SELECT
                  name
                  FROM `first.src`
                )
                SELECT value FROM src
                """.trimIndent()
               ).transformForExecution())
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
    fun testSandboxedQuery() {
        assertEquals(
            "WITH something AS (SELECT a,b,c FROM ${tableName("source")}) SELECT a FROM something",
            queryProcessor("WITH something AS (SELECT a,b,c FROM source) SELECT a,b,c FROM something LIMIT 10",
                sandboxedQuery = "SELECT a FROM something").transformForExecution()
        )
        assertEquals(
            "WITH something AS (SELECT a,b,c FROM ${tableName("source")}), something1 AS (SELECT a,b,c FROM something where c > 1) SELECT a FROM something1",
            queryProcessor("WITH something AS (SELECT a,b,c FROM source) SELECT a,b,c FROM something LIMIT 10",
                sandboxedQuery = "WITH something1 AS (SELECT a,b,c FROM something where c > 1) SELECT a FROM something1").transformForExecution()
        )
    }

    @Test
    fun testSandboxedQuerySelectInto() {
        assertThrows<RestrictedIntoClause> {
            // Using a different vendor here because BigQuery does not
            // support SELECT INTO, but if/when logflare grows to support
            // other syntaxes, one'd wish we wouldn't have forgotten
            // something like this
            queryProcessor(
                "WITH something AS (SELECT a,b,c FROM source) SELECT a,b,c FROM something LIMIT 10",
                dbVendor = EDbVendor.dbvpostgresql,
                sandboxedQuery = "SELECT a FROM something INTO something"
            ).transformForExecution()
        }
    }

    @Test
    fun testSandboxedQueryWithWildcard() {
        assertThrows<RestrictedWildcardResultColumn> {
            queryProcessor(
                "WITH something AS (SELECT a,b,c FROM source) SELECT a,b,c FROM something LIMIT 10",
                sandboxedQuery = "SELECT * FROM something"
            ).transformForExecution()
        }
    }

    @Test
    fun testSandboxedQueryWithRestrictedSources() {
        assertThrows<SandboxRestrictionViolated> {
            queryProcessor(
                "WITH something AS (SELECT a,b,c FROM source) SELECT a,b,c FROM something LIMIT 10",
                sandboxedQuery = "SELECT a FROM source"
            ).transformForExecution()
        }
        assertThrows<SandboxRestrictionViolated> {
            queryProcessor(
                "WITH something AS (SELECT a,b,c FROM source) SELECT a,b,c FROM something LIMIT 10",
                sandboxedQuery = "WITH b AS (SELECT a,b,c FROM source) SELECT a FROM b"
            ).transformForExecution()
        }
    }

    @Test
    fun testSandboxRestrictedFunctions() {
        assertThrows<RestrictedFunctionCall> {
            queryProcessor(
                "WITH something AS (SELECT a,b,c FROM source) SELECT a,b,c FROM something LIMIT 10",
                sandboxedQuery = "SELECT SESSION_USER()").transformForExecution()
        }
        assertThrows<RestrictedFunctionCall> {
            queryProcessor(
                "WITH something AS (SELECT a,b,c FROM source) SELECT a,b,c FROM something LIMIT 10",
                sandboxedQuery = "SELECT EXTERNAL_QUERY('','')").transformForExecution()
        }
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
    fun testParameterExtractionNestedInCall() {
        assertEquals(queryProcessor("SELECT a, @a FROM b WHERE d > timestamp_add(@c, interval 7 day)").parameters(), setOf("c", "a"))
        assertEquals(queryProcessor("SELECT a, @a FROM b WHERE d > timestamp_sub(@c, interval 7 day)").parameters(), setOf("c", "a"))
    }

    @Test
    fun testParameterExtractionInCTE() {
        assertEquals(queryProcessor("with q as (SELECT a, @a FROM b WHERE char_length(@c) > 4) select 1").parameters(), setOf("c", "a"))
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

    @Test
    fun testSourceNamesWithDot() {
        val qp = queryProcessor("SELECT count(id) FROM `dev.dev` WHERE a < 1")
        assertEquals(qp.sources(), setOf(sourceResolver().resolve("dev.dev")))
        assertEquals(
            "SELECT count(id) FROM ${tableName("dev.dev")} WHERE a < 1",
            qp.transformForExecution())
    }

    @Test
    fun testSourceNamesWithDots() {
        val qp = queryProcessor("SELECT count(id) FROM `dev.dev.dev` WHERE a < 1")
        assertEquals(qp.sources(), setOf(sourceResolver().resolve("dev.dev.dev")))
        assertEquals(
            "SELECT count(id) FROM ${tableName("dev.dev.dev")} WHERE a < 1",
            qp.transformForExecution())
    }

    @Test
    fun testTransformJoinUnnest() {
        assertEquals("SELECT a FROM ${tableName("dev")} " +
                "INNER JOIN UNNEST(${tableName("dev")}.metadata) AS f1 ON TRUE",
            queryProcessor("SELECT a FROM dev INNER JOIN UNNEST(dev.metadata) AS f1 ON TRUE").transformForExecution())
        assertEquals("SELECT a FROM ${tableName("dev")} AS dev1 " +
                "INNER JOIN UNNEST(dev1.metadata) AS f1 ON TRUE",
            queryProcessor("SELECT a FROM dev AS dev1 INNER JOIN UNNEST(dev1.metadata) AS f1 ON TRUE").transformForExecution())
    }

    @Test
    fun testUnnestWithNoTableReference() {
        assertDoesNotThrow {
            queryProcessor(
                """
           SELECT
            t.timestamp,
            t.id,
            t.event_message,
            f1
          FROM
            `light-two-os-directions-test` as t
            INNER JOIN UNNEST(metadata) AS f1 ON TRUE
            WHERE t.timestamp > timestamp_sub(current_timestamp(), INTERVAL 7 DAY)
            AND t.timestamp < current_timestamp()
            AND f1.imei IS NOT NULL
          ORDER BY
            t.timestamp DESC
          LIMIT
          10000
       """
            ).transformForExecution()
        }
    }

    @Test
    fun testImplicitColumnExpansionCase1() {
        assertDoesNotThrow {
            queryProcessor("""
                with 
                arr as (
                  SELECT generate_array(1,2) as d
                ),
                dates as (
                  select d from arr, unnest(arr.d) as d
                ),
                logs as (
                  select 
                    timestamp,
                    f2.url as url
                  FROM
                    a
                    LEFT JOIN UNNEST(metadata) AS f1 ON TRUE
                    LEFT JOIN UNNEST(f1.request) AS f2 ON TRUE
                  WHERE
                    timestamp >= timestamp_sub(current_timestamp(), interval 7 day) 
                )
                
                SELECT
                  dates.d as d,
                FROM
                  dates left join logs on timestamp_trunc(logs.timestamp,hour) = dates.d
                GROUP BY
                  d
                ORDER BY
                  d DESC
            """.trimIndent()).transformForExecution()
        }
    }

    @Test
    fun testCTESchema() {
        assertEquals(
            mapOf("tab1" to mapOf("a" to "any", "a1" to "int"), "tab2" to mapOf("b" to "any")),
            queryProcessor("""
                WITH tab1 AS (
                SELECT a,
                       a + 1,
                       CAST(a AS INTEGER) AS a1
                       FROM src
                   ),
                   tab2 AS (
                       SELECT b from src1
                   )
                   SELECT 1;
                """.trimIndent()).cteSchema()
        )
    }



}
