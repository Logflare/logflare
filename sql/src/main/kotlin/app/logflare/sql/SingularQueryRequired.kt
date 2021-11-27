package app.logflare.sql

class SingularQueryRequired(private val query: String) : Throwable() {
    override val message: String?
        get() = "Only singular query allowed (${query})"
}
