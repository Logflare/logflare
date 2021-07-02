package app.logflare.sql

class RestrictedIntoClause(clause: String) : Throwable("restricted INTO clause: $clause")
