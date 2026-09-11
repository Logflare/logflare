defmodule Logflare.Support.PglogicalPrimaryRepo do
  @moduledoc """
  Plain Postgres repo pointed at the pglogical primary, used by
  `Logflare.Integration.PglogicalDdlReplicationTest` to run migrations that must
  NOT be replicated.

  `Logflare.Repo` cannot be used for this: it runs under `Ecto.Adapters.SQL.Sandbox`
  in test env and targets the main test database, not the pglogical primary, so a
  migration through it would never reach the provider node at all.

  Started explicitly by the test; never added to `:ecto_repos`.
  """
  use Ecto.Repo,
    otp_app: :logflare,
    adapter: Ecto.Adapters.Postgres
end
