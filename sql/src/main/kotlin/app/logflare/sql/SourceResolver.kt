package app.logflare.sql

/**
 * Resolves source name to a Source object
 */
interface SourceResolver {
    fun resolve(source: String): Source
}
