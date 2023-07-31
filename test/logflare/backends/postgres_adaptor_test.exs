defmodule Logflare.Backends.Adaptor.PostgresAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.PostgresAdaptor

  import Ecto.Query

  import ExUnit.CaptureLog

  setup do
    repo = Application.get_env(:logflare, Logflare.Repo)

    url =
      "postgresql://#{repo[:username]}:#{repo[:password]}@#{repo[:hostname]}/#{repo[:database]}"

    config = %{
      "url" => url,
      "schema" => nil
    }

    source = insert(:source, user: insert(:user))

    source_backend = insert(:source_backend, type: :postgres, source: source, config: config)

    %{source_backend: source_backend, source: source, postgres_url: url}
  end

  describe "with postgres repo" do
    setup %{source_backend: source_backend} do
      pid = start_supervised!({PostgresAdaptor, source_backend})

      on_exit(fn ->
        PostgresAdaptor.rollback_migrations(source_backend)
        PostgresAdaptor.drop_migrations_table(source_backend)
      end)

      %{pid: pid}
    end

    test "ingest/2 and execute_query/2 dispatched message", %{
      pid: pid,
      source_backend: source_backend
    } do
      log_event = build(:log_event, source: source_backend.source, test: "data")

      assert :ok = PostgresAdaptor.ingest(pid, [log_event])

      # TODO: replace with a timeout retry func
      :timer.sleep(1_500)

      # query by Ecto.Query
      query = from(l in PostgresAdaptor.table_name(source_backend), select: l.body)

      assert {:ok, [%{"test" => "data"}]} = PostgresAdaptor.execute_query(source_backend, query)

      # query by string
      assert {:ok, [%{"body" => %{"test" => "data"}}]} =
               PostgresAdaptor.execute_query(
                 source_backend,
                 "select body from #{PostgresAdaptor.table_name(source_backend)}"
               )

      # query by string with parameter
      assert {:ok, [%{"value" => "data"}]} =
               PostgresAdaptor.execute_query(
                 source_backend,
                 {"select body ->> $1 as value from #{PostgresAdaptor.table_name(source_backend)}",
                  ["test"]}
               )
    end
  end

  describe "repo module" do
    test "create_repo/1 creates a new Ecto.Repo for given source_backend", %{
      source_backend: source_backend
    } do
      repo = PostgresAdaptor.create_repo(source_backend)
      assert Keyword.get(repo.__info__(:attributes), :behaviour) == [Ecto.Repo]
      env = Application.get_env(:logflare, repo)

      # module name should have a prefix
      assert "Elixir.Logflare.Repo.Postgres.Adaptor" <> _ =  Atom.to_string(repo)

      assert env[:migration_source] == PostgresAdaptor.migrations_table_name(source_backend)
    end

    test "custom schema", %{source: source, postgres_url: url} do
      config = %{
        "url" => url,
        "schema" => "my_schema"
      }

      source_backend = insert(:source_backend, type: :postgres, source: source, config: config)
      PostgresAdaptor.create_repo(source_backend)

      assert :ok = PostgresAdaptor.connect_to_repo(source_backend)

      assert {:ok, [%{"schema_name" => "my_schema"}]} =
               PostgresAdaptor.execute_query(
                 source_backend,
                 "select schema_name from information_schema.schemata where schema_name = 'my_schema'"
               )

      assert :ok = PostgresAdaptor.create_log_events_table(source_backend)

      log_event = build(:log_event, source: source_backend.source, test: "data")
      assert {:ok, %_{}} = PostgresAdaptor.insert_log_event(source_backend, log_event)
    end

    test "create_log_events_table/3 creates the table for a given source", %{
      source_backend: source_backend
    } do
      repo = PostgresAdaptor.create_repo(source_backend)
      assert :ok = PostgresAdaptor.connect_to_repo(source_backend)
      assert :ok = PostgresAdaptor.create_log_events_table(source_backend)
      query = from(l in PostgresAdaptor.table_name(source_backend), select: l.body)
      assert repo.all(query) == []
    end

    test "handle migration errors", %{source_backend: source_backend} do
      PostgresAdaptor.create_repo(source_backend)
      assert :ok = PostgresAdaptor.connect_to_repo(source_backend)
      bad_migrations = [{0, BadMigration}]

      assert capture_log(fn ->
               assert {:error, :failed_migration} =
                        PostgresAdaptor.create_log_events_table(
                          source_backend,
                          bad_migrations
                        )
             end) =~ "[error]"
    end
  end
end

defmodule BadMigration do
  @moduledoc false
  use Ecto.Migration

  def up do
    alter table(:none) do
    end
  end
end
