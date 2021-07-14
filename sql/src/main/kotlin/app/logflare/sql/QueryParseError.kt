package app.logflare.sql

class QueryParseError(message: String) : Error(transformErrorMessage(message)) {
}

internal fun transformErrorMessage(message: String): String =
    message.replace("tokenlize", "tokenizing", ignoreCase = true)

