package app.logflare.sql

/**
 * Default table resolver for Source
 *
 * Replaces dashes with underscores in source's UUID
 * (mimicking Logflare's behavior)
 */
object DefaultTableResolver : TableResolver<Source> {
    override fun resolve(t: Source): String =
        t.token.toString().replace('-', '_')

}
