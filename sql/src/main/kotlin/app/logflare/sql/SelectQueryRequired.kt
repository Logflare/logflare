package app.logflare.sql

class SelectQueryRequired(private val query: String) : Throwable() {
    override val message: String?
        get() = "Only SELECT queries allowed (${query})"
}
