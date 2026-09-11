defmodule Logflare.ObanSchemaMigrationTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Ecto.Adapters.SQL.Sandbox
  alias Logflare.Repo
  alias Logflare.Repo.Migrations.AddObanJobsTable
  alias Logflare.Repo.Migrations.AddObanTablesToConfiguredSchema

  @historical_migration Path.expand(
                          "../../priv/repo/migrations/20260216120000_add_oban_jobs_table.exs",
                          __DIR__
                        )
  @upgrade_migration Path.expand(
                       "../../priv/repo/migrations/20260831120000_add_oban_tables_to_configured_schema.exs",
                       __DIR__
                     )

  setup do
    Sandbox.mode(Repo, :auto)
    require_migration(AddObanJobsTable, @historical_migration)
    require_migration(AddObanTablesToConfiguredSchema, @upgrade_migration)

    on_exit(fn -> Sandbox.mode(Repo, :manual) end)

    %{repo_config: Application.get_env(:logflare, Repo, [])}
  end

  test "creates Oban tables in the configured schema for a fresh installation", %{
    repo_config: repo_config
  } do
    schema = unique_schema("fresh")
    public_tables = oban_tables(Repo, "public")

    run_migration(repo_config, AddObanJobsTable, schema, fn ->
      assert oban_tables(Repo, schema) == ["oban_jobs", "oban_peers"]
      assert oban_tables(Repo, "public") == public_tables
    end)
  end

  test "creates Oban tables in the configured schema without moving existing public jobs", %{
    repo_config: repo_config
  } do
    schema = unique_schema("upgrade")
    worker = "LegacyWorker#{System.unique_integer([:positive])}"
    public_tables = oban_tables(Repo, "public")
    legacy_job_id = insert_public_job(Repo, worker)

    try do
      run_migration(repo_config, AddObanTablesToConfiguredSchema, schema, fn ->
        assert oban_tables(Repo, schema) == ["oban_jobs", "oban_peers"]
        assert oban_tables(Repo, "public") == public_tables
        assert job_count(Repo, "public", worker) == 1
        assert job_count(Repo, schema, worker) == 0
      end)
    after
      SQL.query!(Repo, "DELETE FROM public.oban_jobs WHERE id = $1", [legacy_job_id])
    end
  end

  defp run_migration(repo_config, migration, schema, assertions) do
    version = 9_000_000_000_000_000 + System.unique_integer([:positive])
    Application.put_env(:logflare, Repo, Keyword.put(repo_config, :schema, schema))

    try do
      assert :ok = Ecto.Migrator.up(Repo, version, migration, log: false)
      assertions.()
    after
      SQL.query!(Repo, ~s(DROP SCHEMA IF EXISTS "#{schema}" CASCADE))
      SQL.query!(Repo, "DELETE FROM schema_migrations WHERE version = $1", [version])
      Application.put_env(:logflare, Repo, repo_config)
    end
  end

  defp unique_schema(scenario) do
    "oban_schema_#{scenario}_#{System.unique_integer([:positive])}"
  end

  defp oban_tables(repo, schema) do
    %{rows: rows} =
      SQL.query!(
        repo,
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = $1 AND table_name IN ('oban_jobs', 'oban_peers')
        ORDER BY table_name
        """,
        [schema]
      )

    List.flatten(rows)
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
      SQL.query!(repo, ~s|SELECT count(*) FROM "#{schema}".oban_jobs WHERE worker = $1|, [worker])

    count
  end

  defp require_migration(migration, path) do
    unless Code.ensure_loaded?(migration), do: Code.require_file(path)
  end
end
