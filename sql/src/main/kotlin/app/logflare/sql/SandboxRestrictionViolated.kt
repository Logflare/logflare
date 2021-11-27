package app.logflare.sql

class SandboxRestrictionViolated(private val tableName: String) : Throwable() {

    override val message: String?
        get() = "Sandboxed query attempting access outside of sandbox (${tableName})"

}
