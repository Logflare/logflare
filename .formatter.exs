# Used by "mix format"
[
  inputs: ["{.iex,mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"],
  import_deps: [
    :ecto_sql,
    :ecto,
    :phoenix,
    :phoenix_live_view,
    :plug,
    :typed_struct,
    :nimble_parsec,
    :typed_ecto_schema
  ]
]
