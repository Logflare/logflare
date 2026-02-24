# Used by "mix format"
[
  inputs: ["{mix,.formatter,.dialyzer_ignore}.exs", "{config,lib}/**/*.{heex,ex,exs}"],
  subdirectories: ["test", "priv/repo"],
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
  heex_line_length: 300
]
