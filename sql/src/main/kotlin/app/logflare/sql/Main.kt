package app.logflare.sql

import com.ericsson.otp.erlang.*
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory

object Main {

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

        val datasetResolver = EnvDatasetResolver(System.getenv("MIX_ENV") ?: "dev")

        val node = OtpNode("logflare_sql")
        node.setCookie(System.getenv("COOKIE"))

        val nodeName = System.getenv("NODE_NAME") ?: "logflare@localhost"

        val mailbox = node.createMbox()
        mailbox.send("Elixir.Logflare.SQL", nodeName, OtpErlangTuple(
            listOf<OtpErlangObject>(OtpErlangAtom("ready"), mailbox.self()).toTypedArray()))

        logger.info("Ready")

        while (true) {
            val msg = mailbox.receive()
            if (msg is OtpErlangTuple) {
                val tag = msg.elementAt(0)
                if (tag is OtpErlangAtom && tag.atomValue().equals("transform") &&
                    msg.elements().size == 5) {
                    val sender = msg.elementAt(1)
                    val ref = msg.elementAt(2)
                    val query = msg.elementAt(3)
                    val userId = msg.elementAt(4)
                    if (sender is OtpErlangPid && query is OtpErlangBinary && userId is OtpErlangLong) {
                        val sourceResolver = DatabaseSourceResolver(dataSource = ds, userId = userId.longValue())
                        try {
                            val transformed = QueryTransformer(
                                sourceResolver = sourceResolver, datasetResolver = datasetResolver,
                                projectId = projectId, query = query.binaryValue().decodeToString()
                            ).transform()
                            mailbox.send(sender, OtpErlangTuple(listOf<OtpErlangObject>(
                                OtpErlangAtom("ok"),
                                ref,
                                OtpErlangBinary(transformed.toByteArray())
                            ).toTypedArray()))
                        } catch (e: Throwable) {
                            mailbox.send(sender,
                                OtpErlangTuple(listOf<OtpErlangObject>(
                                    OtpErlangAtom("error"),
                                    ref,
                                    OtpErlangBinary(e.message?.toByteArray() ?: "unknown error".toByteArray())
                                ).toTypedArray()))
                        }
                    }
                }
            }
        }

    }

}
