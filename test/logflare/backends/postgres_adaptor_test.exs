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

    backend = insert(:backend, type: :postgres, sources: [source], config: config)

    %{backend: backend, source: source, postgres_url: url}
  end

  describe "with postgres repo" do
    setup %{backend: backend, source: source} do
      pid = start_supervised!({PostgresAdaptor, {source, backend}})

      on_exit(fn ->
        PostgresAdaptor.rollback_migrations({source, backend})
        PostgresAdaptor.drop_migrations_table({source, backend})
      end)

      %{pid: pid}
    end

    test "ingest/2 and execute_query/2 dispatched message", %{
      pid: pid,
      backend: backend,
      source: source
    } do
      log_event = build(:log_event, source: source, test: "data")

      assert :ok = PostgresAdaptor.ingest(pid, [log_event])

      # TODO: replace with a timeout retry func
      :timer.sleep(1_500)

      # query by Ecto.Query
      query = from(l in PostgresAdaptor.table_name(source), select: l.body)

      assert {:ok, [%{"test" => "data"}]} = PostgresAdaptor.execute_query(backend, query)

      # query by string
      assert {:ok, [%{"body" => [%{"test" => "data"}]}]} =
               PostgresAdaptor.execute_query(
                 backend,
                 "select body from #{PostgresAdaptor.table_name(source)}"
               )

      # query by string with parameter
      assert {:ok, [%{"value" => "data"}]} =
               PostgresAdaptor.execute_query(
                 backend,
                 {"select body ->> $1 as value from #{PostgresAdaptor.table_name(source)}",
                  ["test"]}
               )
    end

    test "ingest/2 and execute_query/2 dispatched message with metadata transformation into list",
         %{
           pid: pid,
           backend: backend,
           source: source
         } do
      log_event =
        build(:log_event,
          source: source,
          message: "some msg",
          nested: %{
            "host" => "db-default",
            "parsed" => %{
              "elements" => [%{"meta" => %{"data" => "date"}}]
            }
          }
        )

      assert :ok = PostgresAdaptor.ingest(pid, [log_event])

      # TODO: replace with a timeout retry func
      :timer.sleep(1_500)

      # query by string
      assert {:ok,
              [
                %{
                  "body" => [
                    %{
                      "event_message" => "some msg",
                      "nested" => [
                        %{
                          "host" => "db-default",
                          "parsed" => [
                            %{
                              "elements" => [%{"meta" => [%{"data" => "date"}]}]
                            }
                          ]
                        }
                      ]
                    }
                  ]
                }
              ]} =
               PostgresAdaptor.execute_query(
                 backend,
                 "select body from #{PostgresAdaptor.table_name(source)}"
               )

      # non map results are not impacted by metadata transformations
      query = from(l in PostgresAdaptor.table_name(source), select: count(l.id))
      assert {:ok, [1]} = PostgresAdaptor.execute_query(backend, query)

      # struct results are not impacted by metadata transformations
      query = from(l in PostgresAdaptor.table_name(source), select: l.timestamp)

      assert {:ok, [%NaiveDateTime{}]} = PostgresAdaptor.execute_query(backend, query)
    end
  end

  describe "repo module" do
    test "connected?/1 true when repo is connected", %{backend: backend} do
      PostgresAdaptor.create_repo(backend)
      assert :ok = PostgresAdaptor.connected?(backend)
    end

    test "create_repo/1 creates a new Ecto.Repo for given backend and connects", %{
      backend: backend
    } do
      repo = PostgresAdaptor.create_repo(backend)
      assert :ok = PostgresAdaptor.connected?(backend)
      assert Keyword.get(repo.__info__(:attributes), :behaviour) == [Ecto.Repo]

      # module name should have a prefix
      assert "Elixir.Logflare.Repo.Postgres.Adaptor" <> _ = Atom.to_string(repo)
    end

    test "custom schema", %{source: source, postgres_url: url} do
      config = %{
        "url" => url,
        "schema" => "my_schema"
      }

      backend = insert(:backend, type: :postgres, sources: [source], config: config)
      PostgresAdaptor.create_repo(backend)

      assert :ok = PostgresAdaptor.connected?(backend)

      assert {:ok, [%{"schema_name" => "my_schema"}]} =
               PostgresAdaptor.execute_query(
                 backend,
                 "select schema_name from information_schema.schemata where schema_name = 'my_schema'"
               )

      assert :ok = PostgresAdaptor.create_log_events_table({source, backend})

      log_event = build(:log_event, source: source, test: "data")
      assert {:ok, %_{}} = PostgresAdaptor.insert_log_event(backend, log_event)
    end

    test "create_log_events_table/3 creates the table for a given source", %{
      backend: backend,
      source: source
    } do
      repo = PostgresAdaptor.create_repo(backend)
      assert :ok = PostgresAdaptor.connected?(backend)
      assert :ok = PostgresAdaptor.create_log_events_table({source, backend})
      query = from(l in PostgresAdaptor.table_name(source), select: l.body)
      assert repo.all(query) == []
    end

    test "handle migration errors", %{source: source, backend: backend} do
      PostgresAdaptor.create_repo(backend)
      assert :ok = PostgresAdaptor.connected?(backend)
      bad_migrations = [{0, BadMigration}]

      assert capture_log(fn ->
               assert {:error, :failed_migration} =
                        PostgresAdaptor.create_log_events_table(
                          {source, backend},
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
