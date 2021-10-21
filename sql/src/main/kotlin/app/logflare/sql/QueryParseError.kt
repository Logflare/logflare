package app.logflare.sql

import gudusoft.gsqlparser.TSyntaxError
import java.lang.StringBuilder
import kotlin.math.*

class QueryParseError(private val sql: String, private val errors: ArrayList<TSyntaxError>) : Error() {

    override val message: String
        get() {
            val error = errors.first()

            val hint = if (error.hint.isEmpty()) {
                "Syntax error"
            } else {
                // Remove useless ", state: NUMBER"
                error.hint.replace(Regex(",\\sstate:(\\d)+"),"")
            }

            if (error.lineNo == 0L && error.columnNo == 0L) {
               return transformErrorMessage(hint)
            } else {
                val line = sql.lines()[error.lineNo.toInt()-1]
                val startIndex = max(error.columnNo - 10, 0).toInt()
                val context = line.substring(
                    startIndex,
                    min(error.columnNo.toInt() + 10, line.length)
                )
                val sb = StringBuilder(context)
                sb.insert(error.columnNo.toInt() - 1 - startIndex, "[")
                sb.insert(min(error.columnNo.toInt() - startIndex + error.tokentext.length, line.length + 1), "]")
                return "${transformErrorMessage(hint)} at line ${error.lineNo} column ${error.columnNo} near \"${error.tokentext}\" in \"$sb\""
            }
        }
}

internal fun transformErrorMessage(message: String): String =
    message.replace("tokenlize", "tokenizing", ignoreCase = true)

