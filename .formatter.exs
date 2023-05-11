# Used by "mix format"
[
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{heex,ex,exs}"],
  import_deps: [
    :ecto_sql,
    :ecto,
    :phoenix,
    :phoenix_live_view,
    :plug,
    :typed_struct,
    :nimble_parsec,
    :typed_ecto_schema,
    :open_api_spex
  ],
  plugins: [Phoenix.LiveView.HTMLFormatter],
]
