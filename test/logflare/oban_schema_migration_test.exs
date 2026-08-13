defmodule Logflare.ObanSchemaMigrationTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Logflare.Repo
  alias Logflare.Repo.Migrations.AddObanJobsTable
  alias Logflare.Repo.Migrations.AddObanTablesToConfiguredSchema

  @historical_migration Path.expand(
                          "../../priv/repo/migrations/20260216120000_add_oban_jobs_table.exs",
                          __DIR__
                        )
  @upgrade_migration Path.expand(
                       "../../priv/repo/migrations/20260814000000_add_oban_tables_to_configured_schema.exs",
                       __DIR__
                     )
  @repo_name Logflare.ObanSchemaMigrationTest.Repo

  setup_all do
    require_migration(AddObanJobsTable, @historical_migration)
    require_migration(AddObanTablesToConfiguredSchema, @upgrade_migration)

    repo =
      start_supervised!(
        Supervisor.child_spec(
          {Repo, name: @repo_name, pool: DBConnection.ConnectionPool, pool_size: 2},
          id: @repo_name
        )
      )

    %{repo: repo}
  end

  test "creates Oban tables in the configured schema for a fresh installation", %{repo: repo} do
    schema = unique_schema("fresh")

    run_migration(repo, AddObanJobsTable, schema, fn ->
      assert table_exists?(repo, schema, "oban_jobs")
      assert table_exists?(repo, schema, "oban_peers")
    end)
  end

  test "creates Oban tables in the configured schema without moving existing public jobs", %{
    repo: repo
  } do
    schema = unique_schema("upgrade")
    worker = "LegacyWorker#{System.unique_integer([:positive])}"
    legacy_job_id = insert_public_job(repo, worker)

    try do
      run_migration(repo, AddObanTablesToConfiguredSchema, schema, fn ->
        assert table_exists?(repo, schema, "oban_jobs")
        assert table_exists?(repo, schema, "oban_peers")
        assert job_count(repo, "public", worker) == 1
        assert job_count(repo, schema, worker) == 0
      end)
    after
      SQL.query!(repo, "DELETE FROM public.oban_jobs WHERE id = $1", [legacy_job_id])
    end
  end

  defp run_migration(repo, migration, schema, assertions) do
    version = 9_000_000_000_000_000 + System.unique_integer([:positive])
    previous_schema = System.get_env("DB_SCHEMA")
    System.put_env("DB_SCHEMA", schema)

    try do
      assert :ok = Ecto.Migrator.up(Repo, version, migration, log: false, dynamic_repo: repo)
      assertions.()
    after
      SQL.query!(repo, ~s(DROP SCHEMA IF EXISTS "#{schema}" CASCADE))
      SQL.query!(repo, "DELETE FROM schema_migrations WHERE version = $1", [version])
      restore_db_schema(previous_schema)
    end
  end

  defp unique_schema(scenario) do
    "oban_schema_#{scenario}_#{System.unique_integer([:positive])}"
  end

  defp table_exists?(repo, schema, table) do
    %{rows: [[table_name]]} =
      SQL.query!(repo, "SELECT to_regclass($1)::text", ["#{schema}.#{table}"])

    table_name == "#{schema}.#{table}"
  end

  defp insert_public_job(repo, worker) do
    %{rows: [[id]]} =
      SQL.query!(
        repo,
        "INSERT INTO public.oban_jobs (worker, args) VALUES ($1, '{}'::jsonb) RETURNING id",
        [worker]
      )

    id
  end

  defp job_count(repo, schema, worker) do
    %{rows: [[count]]} =
      SQL.query!(repo, ~s|SELECT count(*) FROM "#{schema}".oban_jobs WHERE worker = $1|, [
        worker
      ])

    count
  end

  defp restore_db_schema(nil), do: System.delete_env("DB_SCHEMA")
  defp restore_db_schema(schema), do: System.put_env("DB_SCHEMA", schema)

  defp require_migration(migration, path) do
    unless Code.ensure_loaded?(migration), do: Code.require_file(path)
  end
end
