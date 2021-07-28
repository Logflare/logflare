package app.logflare.sql

/**
 * Resolves object to its BigQuery table name
 */
interface TableResolver<T> {
    fun resolve(t: T): String
}
