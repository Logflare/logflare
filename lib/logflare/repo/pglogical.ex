defmodule Logflare.Repo.Pglogical do
  @moduledoc """
  Migration-only repo that routes DDL through `pglogical.replicate_ddl_command/2`
  via `Logflare.Ecto.Adapters.PglogicalPostgres`.

  Never added to `:ecto_repos`; never started as part of the normal
  application supervision tree. Only used by `Logflare.Repo.Migrator` when
  `LOGFLARE_PGLOGICAL_REPLICATE_DDL_COMMANDS_SETS` is set.

  `:migrator` is set explicitly because Oban picks its migration flavor by matching on
  the repo's adapter module, which this custom adapter does not match.
  """
  use Ecto.Repo,
    otp_app: :logflare,
    adapter: Logflare.Ecto.Adapters.PglogicalPostgres

  @impl true
  def init(_type, config) do
    config =
      Logflare.Repo.config()
      |> Keyword.merge(config)
      |> Keyword.put(:priv, "priv/repo")
      |> Keyword.put_new(:migrator, Oban.Migrations.Postgres)

    {:ok, config}
  end
end
