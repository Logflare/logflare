package app.logflare.sql

import java.util.*

/**
 * Resolves source name to a Source object
 */
interface SourceResolver {
    fun resolve(source: String): Source
    fun findByUUID(uuid: UUID): Source
}
