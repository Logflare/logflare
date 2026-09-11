## Continuously demonstrates pglogical DDL replication end to end.
##
## Every cycle runs three real Ecto migrations through
## `Logflare.Repo.Pglogical` (so the DDL goes through
## `Logflare.Ecto.Adapters.PglogicalPostgres.Connection.execute_ddl/1` and
## `pglogical.replicate_ddl_command/2`, exactly as a production migration
## would), then polls the replica over a separate connection until the change
## shows up, reporting the propagation latency.
##
## Requires the containers from `docker-compose.pglogical.yml`, already
## bootstrapped via `test/support/pglogical/bootstrap.exs`.
##
##     mix run --no-start test/support/pglogical/demo.exs
##
## Ctrl-C twice to stop. Tunables: PGLOGICAL_PRIMARY_URL, PGLOGICAL_REPLICA_URL,
## PGLOGICAL_TEST_REPLICATION_SET, PGLOGICAL_DEMO_INTERVAL_MS,
## PGLOGICAL_DEMO_TIMEOUT_MS.

Application.ensure_all_started(:postgrex)
Application.ensure_all_started(:ecto_sql)

defmodule Logflare.Pglogical.Demo.CreateTable do
  @moduledoc false
  use Ecto.Migration

  def up do
    create table(:pglogical_demo) do
      add(:name, :string)
    end
  end

  def down do
    drop(table(:pglogical_demo))
  end
end

defmodule Logflare.Pglogical.Demo.AddColumn do
  @moduledoc false
  use Ecto.Migration

  def up do
    alter table(:pglogical_demo) do
      add(:extra_column, :string)
    end
  end
end

defmodule Logflare.Pglogical.Demo do
  @moduledoc false

  alias Logflare.Pglogical.Demo.AddColumn
  alias Logflare.Pglogical.Demo.CreateTable

  @table "pglogical_demo"
  @column "extra_column"
  @create_version 1
  @alter_version 2
  @poll_ms 25

  def run(state) do
    IO.puts("")
    IO.puts(header(state))

    reset_migrations!(state.primary)
    Enum.each(steps(), &step(state, &1))

    Process.sleep(state.interval_ms)
    run(%{state | cycle: state.cycle + 1})
  end

  defp steps do
    [
      {"CREATE TABLE", CreateTable, :up, @create_version, :table_absent, :table_present},
      {"ALTER TABLE ADD COLUMN", AddColumn, :up, @alter_version, :column_absent, :column_present},
      {"DROP TABLE", CreateTable, :down, @create_version, :table_present, :table_absent}
    ]
  end

  defp step(state, {label, migration, direction, version, precondition, expectation}) do
    # Without this the demo could report success on state left by the previous
    # cycle rather than on anything that actually replicated just now.
    unless satisfied?(state, precondition) do
      IO.puts("  #{yellow("warn")} #{pad(label)} replica was not in the expected starting state")
    end

    Ecto.Migrator.run(state.repo, [{version, migration}], direction, all: true, log: false)

    case await(state, expectation) do
      {:ok, elapsed} ->
        IO.puts("  #{green("ok")}   #{pad(label)} replicated in #{elapsed}ms")

      {:timeout, elapsed} ->
        IO.puts("  #{red("FAIL")} #{pad(label)} not seen on replica after #{elapsed}ms")
    end
  end

  defp await(state, expectation) do
    await(state, expectation, System.monotonic_time(:millisecond))
  end

  defp await(state, expectation, started_at) do
    elapsed = System.monotonic_time(:millisecond) - started_at

    cond do
      satisfied?(state, expectation) ->
        {:ok, elapsed}

      elapsed >= state.timeout_ms ->
        {:timeout, elapsed}

      true ->
        Process.sleep(@poll_ms)
        await(state, expectation, started_at)
    end
  end

  defp satisfied?(state, :table_present), do: table_exists?(state.replica)
  defp satisfied?(state, :table_absent), do: not table_exists?(state.replica)
  defp satisfied?(state, :column_present), do: column_exists?(state.replica)
  defp satisfied?(state, :column_absent), do: not column_exists?(state.replica)

  defp table_exists?(conn) do
    query_any?(
      conn,
      "SELECT 1 FROM information_schema.tables WHERE table_name = $1",
      [@table]
    )
  end

  defp column_exists?(conn) do
    query_any?(
      conn,
      "SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = $2",
      [@table, @column]
    )
  end

  defp query_any?(conn, sql, params) do
    case Postgrex.query(conn, sql, params) do
      {:ok, %{rows: rows}} -> rows != []
      {:error, _error} -> false
    end
  end

  defp header(state) do
    timestamp = DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_string()

    "cycle #{state.cycle} @ #{timestamp}  subscription=#{subscription_status(state.replica)}"
  end

  defp subscription_status(conn) do
    case Postgrex.query(conn, "SELECT status FROM pglogical.show_subscription_status()", []) do
      {:ok, %{rows: [[status] | _]}} -> colorize_status(status)
      _other -> red("unknown")
    end
  end

  defp colorize_status("replicating"), do: green("replicating")
  defp colorize_status(status), do: red(status)

  # Migrations are rerun with the same versions every cycle, so the recorded
  # versions have to go or `Ecto.Migrator` would treat each one as already
  # applied and silently do nothing.
  def reset_migrations!(conn) do
    Postgrex.query(conn, "DELETE FROM schema_migrations", [])
  end

  # Unreplicated cleanup on both nodes, so a leftover table from an interrupted
  # run cannot make the first cycle report a false positive.
  def reset_table!(conn) do
    Postgrex.query!(conn, "DROP TABLE IF EXISTS #{@table} CASCADE", [])
  end

  defp pad(label), do: String.pad_trailing(label, 24)
  defp green(text), do: IO.ANSI.green() <> text <> IO.ANSI.reset()
  defp red(text), do: IO.ANSI.red() <> text <> IO.ANSI.reset()
  defp yellow(text), do: IO.ANSI.yellow() <> text <> IO.ANSI.reset()
end

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
interval_ms = String.to_integer(System.get_env("PGLOGICAL_DEMO_INTERVAL_MS", "2000"))
timeout_ms = String.to_integer(System.get_env("PGLOGICAL_DEMO_TIMEOUT_MS", "10000"))

# Drives the same config the release path reads, so the adaptor wraps DDL in
# pglogical.replicate_ddl_command for this replication set.
Application.put_env(:logflare, Logflare.Repo.Migrator,
  replication_sets: [replication_set],
  search_path: "public"
)

{:ok, primary} = Postgrex.start_link(Ecto.Repo.Supervisor.parse_url(primary_url))
{:ok, replica} = Postgrex.start_link(Ecto.Repo.Supervisor.parse_url(replica_url))
{:ok, _repo_pid} = Logflare.Repo.Pglogical.start_link(url: primary_url, pool_size: 2)

Logflare.Pglogical.Demo.reset_table!(primary)
Logflare.Pglogical.Demo.reset_table!(replica)
Logflare.Pglogical.Demo.reset_migrations!(primary)

IO.puts("""
pglogical DDL replication demo
  primary          #{primary_url}
  replica          #{replica_url}
  replication set  #{replication_set}
  migration repo   #{inspect(Logflare.Repo.Migrator.migration_repo_for(Logflare.Repo))}

Each cycle migrates the primary and waits for the replica to catch up.
Ctrl-C twice to stop.\
""")

Logflare.Pglogical.Demo.run(%{
  repo: Logflare.Repo.Pglogical,
  primary: primary,
  replica: replica,
  cycle: 1,
  interval_ms: interval_ms,
  timeout_ms: timeout_ms
})
