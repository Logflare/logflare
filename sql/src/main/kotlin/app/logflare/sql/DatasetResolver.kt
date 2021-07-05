package app.logflare.sql

/**
 * Resolves object's BigQuery dataset name
 */
interface DatasetResolver<T> {
    fun resolve(t: T): String
}
