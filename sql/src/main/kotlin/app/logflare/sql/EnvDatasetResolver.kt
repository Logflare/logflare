package app.logflare.sql

class EnvDatasetResolver(private val env: String) : DatasetResolver<Source> {
    override fun resolve(t: Source): String =
        "${t.userId}_${env}"
}
