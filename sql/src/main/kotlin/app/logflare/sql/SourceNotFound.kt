package app.logflare.sql

class SourceNotFound(source: String, userId: Long) : Throwable("can't find source $source")
