defmodule Logflare.Ecto.Adapters.PglogicalPostgres.Connection do
  @moduledoc """
  Delegates to `Ecto.Adapters.Postgres.Connection`, wrapping DDL with
  `pglogical.replicate_ddl_command/2` for replication.

  Replication-set names are validated in `config/runtime.exs` because DDL
  cannot use bind parameters.
  """
  @behaviour Ecto.Adapters.SQL.Connection

  alias Ecto.Adapters.Postgres.Connection, as: PG
  alias Logflare.Repo.Migrator

  @impl true
  defdelegate child_spec(opts), to: PG
  @impl true
  defdelegate prepare_execute(conn, name, statement, params, opts), to: PG
  @impl true
  defdelegate execute(conn, cached, params, opts), to: PG
  @impl true
  defdelegate query(conn, statement, params, opts), to: PG
  @impl true
  defdelegate query_many(conn, statement, params, opts), to: PG
  @impl true
  defdelegate stream(conn, statement, params, opts), to: PG
  @impl true
  defdelegate to_constraints(exception, opts), to: PG
  @impl true
  defdelegate all(query), to: PG
  @impl true
  defdelegate update_all(query), to: PG
  @impl true
  defdelegate delete_all(query), to: PG
  @impl true
  defdelegate insert(prefix, table, header, rows, on_conflict, returning, placeholders), to: PG
  @impl true
  defdelegate update(prefix, table, fields, filters, returning), to: PG
  @impl true
  defdelegate delete(prefix, table, filters, returning), to: PG
  @impl true
  defdelegate explain_query(conn, query, params, opts), to: PG
  @impl true
  defdelegate ddl_logs(result), to: PG
  @impl true
  defdelegate table_exists_query(table), to: PG

  @impl true
  def execute_ddl(command) do
    replication_sets = Migrator.replication_sets()

    command
    |> PG.execute_ddl()
    |> List.wrap()
    |> Enum.map(&wrap_in_pglogical(&1, replication_sets))
  end

  defp wrap_in_pglogical(sql, []), do: sql

  defp wrap_in_pglogical(sql, replication_sets) when is_list(sql) do
    sql |> IO.iodata_to_binary() |> wrap_in_pglogical(replication_sets)
  end

  defp wrap_in_pglogical(sql, replication_sets) when is_binary(sql) do
    sets_array = replication_sets |> Enum.map(&"'#{&1}'") |> Enum.join(", ")

    # pglogical applies replicated DDL with an empty search_path on both
    # provider and subscriber, so unqualified names (including Ecto's own
    # schema_migrations) fail with "no schema has been selected to create in".
    search_path = "SET LOCAL search_path = #{Migrator.search_path()}; "

    "SELECT pglogical.replicate_ddl_command(" <>
      "$lf_ddl$#{search_path}#{sql}$lf_ddl$, ARRAY[#{sets_array}]::text[])"
  end
end
