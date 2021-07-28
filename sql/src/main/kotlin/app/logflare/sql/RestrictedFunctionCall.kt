package app.logflare.sql

class RestrictedFunctionCall(function: String) : Throwable("Restricted function $function")
