package app.logflare.sql

import com.ericsson.otp.erlang.*
import com.google.api.gax.rpc.InvalidArgumentException
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*

object Main {

    internal data class BigQuery(val projectId: String, val datasetResolver: DatasetResolver<Source>)

    private fun getUserBigQuery(dataSource: HikariDataSource, userId: Long, default: BigQuery) : BigQuery {
        dataSource.connection.use { conn ->
            conn.prepareStatement("SELECT bigquery_project_id, bigquery_dataset_id FROM users WHERE id = ?").use { stmt ->
                stmt.setLong(1, userId)
                stmt.executeQuery().use { resultSet ->
                    if (!resultSet.next()) {
                        return default
                    }
                    val dataset = resultSet.getString(2)
                    var datasetResolver = default.datasetResolver
                    if (dataset != null) {
                       datasetResolver = object : DatasetResolver<Source> {
                           override fun resolve(t: Source): String = dataset
                       }
                    }
                    return BigQuery(
                        projectId = resultSet.getString(1) ?: default.projectId,
                        datasetResolver = datasetResolver,
                    )
                }
            }
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val logger = LoggerFactory.getLogger(this::class.java.canonicalName)
        logger.info("Starting")

        val projectId = System.getenv("PROJECT_ID")
        val hikariConfig = HikariConfig()
        hikariConfig.jdbcUrl = System.getenv("DATABASE_URL")
        hikariConfig.username = System.getenv("DB_USER")
        hikariConfig.password = System.getenv("DB_PASSWORD")
        val ds = HikariDataSource(hikariConfig)

        val datasetResolver = EnvDatasetResolver(System.getenv("LOGFLARE_ENV") ?: "dev")

        val node = OtpNode("logflare_sql")
        node.setCookie(System.getenv("COOKIE"))

        val nodeName = System.getenv("NODE_NAME") ?: "logflare@localhost"

        val mailbox = node.createMbox()
        mailbox.send("Elixir.Logflare.SQL", nodeName, OtpErlangTuple(
            listOf<OtpErlangObject>(OtpErlangAtom("ready"), mailbox.self()).toTypedArray()))

        logger.info("Ready")

        val bq = BigQuery(projectId = projectId, datasetResolver = datasetResolver)

        while (true) {
            val msg = mailbox.receive()
            processMessage(msg, ds, bq, mailbox, datasetResolver, projectId)
        }

    }

    private fun extractQuery(obj: OtpErlangObject) : String {
        return when (obj) {
            is OtpErlangBinary -> {
                obj.binaryValue().decodeToString()
            }
            is OtpErlangTuple -> {
                extractQuery(obj.elementAt(0))
            }
            else -> {
                ""
            }
        }
    }

    private fun extractSanboxedQuery(obj: OtpErlangObject) : String? {
        return when (obj) {
            is OtpErlangBinary -> {
                null
            }
            is OtpErlangTuple -> {
                extractQuery(obj.elementAt(1))
            }
            else -> {
               null
            }
        }
    }


    private fun processMessage(
        msg: OtpErlangObject?,
        ds: HikariDataSource,
        bq: BigQuery,
        mailbox: OtpMbox,
        datasetResolver: EnvDatasetResolver,
        projectId: String
    ) {
        if (msg is OtpErlangTuple) {
            val tag = msg.elementAt(0)
            if (isTransform(tag, msg)) {
                val sender = msg.elementAt(1)
                val ref = msg.elementAt(2)
                val query = msg.elementAt(3)
                val userId = msg.elementAt(4)
                if (sender is OtpErlangPid && userId is OtpErlangLong) {
                    val sourceResolver = DatabaseSourceResolver(dataSource = ds, userId = userId.longValue())
                    val userBq = getUserBigQuery(dataSource = ds, userId = userId.longValue(), default = bq)
                    try {
                        val transformed = QueryProcessor(
                            sourceResolver = sourceResolver, datasetResolver = userBq.datasetResolver,
                            projectId = userBq.projectId,
                            query = extractQuery(query), sandboxedQuery = extractSanboxedQuery(query)
                        ).transformForExecution()
                        mailbox.send(
                            sender, OtpErlangTuple(
                                listOf<OtpErlangObject>(
                                    OtpErlangAtom("ok"),
                                    ref,
                                    OtpErlangBinary(transformed.toByteArray())
                                ).toTypedArray()
                            )
                        )
                    } catch (e: Throwable) {
                        reportError(mailbox, sender, ref, e)
                    }
                }
            } else if (isSourceMapping(tag, msg)) {
                val sender = msg.elementAt(1)
                val ref = msg.elementAt(2)
                val query = msg.elementAt(3)
                val userId = msg.elementAt(4)
                val map = msg.elementAt(5)

                if (sender is OtpErlangPid && userId is OtpErlangLong && map is OtpErlangMap) {
                    val sourceResolver = DatabaseSourceResolver(dataSource = ds, userId = userId.longValue())
                    val mapping = mutableMapOf<String, UUID>()
                    map.entrySet().forEach {
                        if (it.key is OtpErlangBinary && it.value is OtpErlangBinary) {
                            mapping[(it.key as OtpErlangBinary).binaryValue().decodeToString()] =
                                UUID.fromString(
                                    (it.value as OtpErlangBinary)
                                        .binaryValue().decodeToString()
                                )
                        }
                    }

                    val userBq = getUserBigQuery(dataSource = ds, userId = userId.longValue(), default = bq)

                    try {
                        val mapped = QueryProcessor(
                            sourceResolver = sourceResolver, datasetResolver = userBq.datasetResolver,
                            projectId = userBq.projectId,
                            query = extractQuery(query), sandboxedQuery = extractSanboxedQuery(query)
                        ).mapSources(mapping)
                        mailbox.send(
                            sender, OtpErlangTuple(
                                listOf<OtpErlangObject>(
                                    OtpErlangAtom("ok"),
                                    ref,
                                    OtpErlangBinary(mapped.toByteArray())
                                ).toTypedArray()
                            )
                        )
                    } catch (e: Throwable) {
                        reportError(mailbox, sender, ref, e)
                    }
                }
            } else if (isParameters(tag, msg)) {
                val sender = msg.elementAt(1)
                val ref = msg.elementAt(2)
                val query = msg.elementAt(3)
                if (sender is OtpErlangPid) {
                    val sourceResolver = DatabaseSourceResolver(dataSource = ds, userId = 0)
                    try {
                        val parameters = QueryProcessor(
                            sourceResolver = sourceResolver, datasetResolver = datasetResolver,
                            projectId = projectId,
                            query = extractQuery(query), sandboxedQuery = extractSanboxedQuery(query)
                        ).parameters()
                        mailbox.send(
                            sender, OtpErlangTuple(
                                listOf<OtpErlangObject>(
                                    OtpErlangAtom("ok"),
                                    ref,
                                    OtpErlangList(parameters.map { OtpErlangBinary(it.toByteArray()) }
                                        .toTypedArray<OtpErlangObject>())
                                ).toTypedArray()
                            )
                        )
                    } catch (e: Throwable) {
                        reportError(mailbox, sender, ref, e)
                    }
                }
            } else if (isSources(tag, msg)) {
                val sender = msg.elementAt(1)
                val ref = msg.elementAt(2)
                val query = msg.elementAt(3)
                val userId = msg.elementAt(4)
                if (sender is OtpErlangPid && userId is OtpErlangLong) {
                    val sourceResolver = DatabaseSourceResolver(dataSource = ds, userId = userId.longValue())
                    val userBq = getUserBigQuery(dataSource = ds, userId = userId.longValue(), default = bq)

                    try {

                        val sources = QueryProcessor(
                            sourceResolver = sourceResolver, datasetResolver = userBq.datasetResolver,
                            projectId = userBq.projectId,
                            query = extractQuery(query), sandboxedQuery = extractSanboxedQuery(query)
                        ).sources()
                        val map = OtpErlangMap()
                        sources.forEach {
                            map.put(
                                OtpErlangBinary(it.name.toByteArray()),
                                OtpErlangBinary(it.token.toString().toByteArray())
                            )
                        }
                        mailbox.send(
                            sender, OtpErlangTuple(
                                listOf<OtpErlangObject>(
                                    OtpErlangAtom("ok"),
                                    ref,
                                    map
                                ).toTypedArray()
                            )
                        )
                    } catch (e: Throwable) {
                        reportError(mailbox, sender, ref, e)
                    }
                }
            }
        }
    }

    private fun reportError(
        mailbox: OtpMbox,
        sender: OtpErlangPid?,
        ref: OtpErlangObject,
        e: Throwable
    ) {
        mailbox.send(
            sender,
            OtpErlangTuple(
                listOf(
                    OtpErlangAtom("error"),
                    ref,
                    OtpErlangBinary(e.message?.toByteArray() ?: "unknown error".toByteArray())
                ).toTypedArray()
            )
        )
    }

    private fun isSources(
        tag: OtpErlangObject?,
        msg: OtpErlangTuple
    ) = tag is OtpErlangAtom && tag.atomValue().equals("sources") &&
            msg.elements().size == 5

    private fun isParameters(
        tag: OtpErlangObject?,
        msg: OtpErlangTuple
    ) = tag is OtpErlangAtom && tag.atomValue().equals("parameters") &&
            msg.elements().size == 4

    private fun isSourceMapping(
        tag: OtpErlangObject?,
        msg: OtpErlangTuple
    ) = tag is OtpErlangAtom && tag.atomValue().equals("sourceMapping") &&
            msg.elements().size == 6

    private fun isTransform(
        tag: OtpErlangObject?,
        msg: OtpErlangTuple
    ) = tag is OtpErlangAtom && tag.atomValue().equals("transform") &&
            msg.elements().size == 5

}
