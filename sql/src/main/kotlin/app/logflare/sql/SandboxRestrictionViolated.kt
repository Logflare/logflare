package app.logflare.sql

class SandboxRestrictionViolated(private val tableName: String) : Throwable() {

    override val message: String?
        get() = "Table not found in CTE: (${tableName})"

}
