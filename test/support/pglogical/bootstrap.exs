Application.ensure_all_started(:postgrex)

primary_url =
  System.get_env(
    "PGLOGICAL_PRIMARY_URL",
    "postgresql://postgres:postgres@localhost:5433/logflare_test"
  )

replica_url =
  System.get_env(
    "PGLOGICAL_REPLICA_URL",
    "postgresql://postgres:postgres@localhost:5434/logflare_test"
  )

replication_set = System.get_env("PGLOGICAL_TEST_REPLICATION_SET", "my_set")
repl_role = System.get_env("PGLOGICAL_REPL_ROLE", "logflare_repl")
repl_password = System.get_env("PGLOGICAL_REPL_PASSWORD", "logflare_repl")

defmodule Logflare.Pglogical.Bootstrap do
  @moduledoc false

  require Logger

  @duplicate_codes [:duplicate_object, :unique_violation]
  @duplicate_messages ["already exists", "already subscribes"]

  def connect!(url) do
    opts = Ecto.Repo.Supervisor.parse_url(url)
    {:ok, conn} = Postgrex.start_link(opts)
    conn
  end

  def query!(conn, sql) do
    case Postgrex.query(conn, sql, []) do
      {:ok, result} ->
        result

      {:error, error} ->
        raise "pglogical bootstrap failed for statement #{inspect(sql)}: #{Exception.message(error)}"
    end
  end

  def idempotent_query!(conn, sql) do
    case Postgrex.query(conn, sql, []) do
      {:ok, result} -> result
      {:error, error} -> handle_idempotent_error!(sql, error)
    end
  end

  defp handle_idempotent_error!(sql, %Postgrex.Error{postgres: postgres} = error) do
    if already_exists?(postgres) do
      Logger.warning(
        "pglogical bootstrap skipping already-existing object for statement " <>
          "#{inspect(sql)}: #{Exception.message(error)}"
      )

      :ok
    else
      raise_bootstrap_error!(sql, error)
    end
  end

  defp handle_idempotent_error!(sql, error), do: raise_bootstrap_error!(sql, error)

  defp already_exists?(%{code: code}) when code in @duplicate_codes, do: true

  # pglogical's create_node/create_replication_set/create_subscription signal
  # duplicates with a bare elog(ERROR) rather than a duplicate-object SQLSTATE
  # (XX000 for an existing node, 22023 for an already-subscribed set), so the
  # message is the only reliable discriminator.
  defp already_exists?(%{message: message}),
    do: String.contains?(message, @duplicate_messages)

  defp already_exists?(_postgres), do: false

  defp raise_bootstrap_error!(sql, error) do
    raise "pglogical bootstrap failed for statement #{inspect(sql)}: #{Exception.message(error)}"
  end
end

alias Logflare.Pglogical.Bootstrap

primary = Bootstrap.connect!(primary_url)
replica = Bootstrap.connect!(replica_url)

primary_dsn =
  "host=pglogical_primary port=5432 dbname=logflare_test user=#{repl_role} password=#{repl_password} sslmode=disable"

replica_dsn =
  "host=pglogical_replica port=5432 dbname=logflare_test user=#{repl_role} password=#{repl_password} sslmode=disable"

# A dedicated replication role is used (rather than the superuser) to mirror
# production pglogical setups, where the apply worker reconnects using this
# role's real credentials (see docs/pglogical replication runbook).
Bootstrap.idempotent_query!(
  primary,
  "CREATE ROLE #{repl_role} WITH LOGIN REPLICATION PASSWORD '#{repl_password}'"
)

Bootstrap.idempotent_query!(
  replica,
  "CREATE ROLE #{repl_role} WITH LOGIN REPLICATION PASSWORD '#{repl_password}'"
)

Bootstrap.query!(primary, "CREATE EXTENSION IF NOT EXISTS pglogical")
Bootstrap.query!(replica, "CREATE EXTENSION IF NOT EXISTS pglogical")

# The subscriber connects back to the provider as this role to read
# pglogical.node_info during create_subscription, so it needs to reach the
# extension's own catalogs, not just public.
Bootstrap.query!(primary, "GRANT USAGE ON SCHEMA pglogical TO #{repl_role}")
Bootstrap.query!(primary, "GRANT SELECT ON ALL TABLES IN SCHEMA pglogical TO #{repl_role}")

# Required so the replication role can read table contents for initial data
# sync; DDL replication itself doesn't need this, but it's the same
# prerequisite production setups require, so it's kept for parity.
Bootstrap.query!(primary, "GRANT SELECT ON ALL TABLES IN SCHEMA public TO #{repl_role}")

Bootstrap.query!(
  primary,
  "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO #{repl_role}"
)

Bootstrap.idempotent_query!(
  primary,
  "SELECT pglogical.create_node(node_name := 'primary', dsn := '#{primary_dsn}')"
)

Bootstrap.idempotent_query!(
  replica,
  "SELECT pglogical.create_node(node_name := 'replica', dsn := '#{replica_dsn}')"
)

Bootstrap.idempotent_query!(
  primary,
  "SELECT pglogical.create_replication_set(set_name := '#{replication_set}')"
)

# replication_sets is mandatory here: omitting it silently subscribes to
# pglogical's defaults (ARRAY['default','default_insert_only','ddl_sql']),
# none of which contain our replication set, and the subscription reports
# "replicating" while doing nothing.
Bootstrap.idempotent_query!(
  replica,
  "SELECT pglogical.create_subscription(" <>
    "subscription_name := 'test_sub', " <>
    "provider_dsn := '#{primary_dsn}', " <>
    "replication_sets := ARRAY['#{replication_set}'], " <>
    "synchronize_structure := false, " <>
    "synchronize_data := false)"
)

IO.puts("pglogical bootstrap complete")
