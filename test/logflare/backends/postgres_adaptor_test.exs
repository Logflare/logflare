defmodule Logflare.Backends.Adaptor.PostgresAdaptorTest do
  use Logflare.DataCase, async: false
  import ExUnit.CaptureLog
  alias Logflare.Backends.Adaptor.PostgresAdaptor

  setup do
    repo = Application.get_env(:logflare, Logflare.Repo)

    url =
      "postgresql://#{repo[:username]}:#{repo[:password]}@#{repo[:hostname]}/#{repo[:database]}"

    config = %{
      url: url,
      schema: nil
    }

    source = insert(:source, user: insert(:user))

    backend = insert(:backend, type: :postgres, sources: [source], config: config)

    on_exit(fn ->
      PostgresAdaptor.destroy_instance({source, backend})
    end)

    %{backend: backend, source: source, postgres_url: url}
  end

  describe "with postgres repo" do
    setup %{backend: backend, source: source} do
      pid = start_supervised!({PostgresAdaptor, {source, backend}})
      %{pid: pid}
    end

    test "ingest/2 and execute_query/2 dispatched message", %{
      pid: pid,
      backend: backend,
      source: source
    } do
      log_event = build(:log_event, source: source, test: "data")

      assert :ok =
               PostgresAdaptor.ingest(pid, [log_event],
                 source_id: source.id,
                 backend_id: backend.id
               )

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

      assert :ok =
               PostgresAdaptor.ingest(pid, [log_event],
                 backend_id: backend.id,
                 source_id: source.id
               )

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

  describe "separate config fields" do
    test "special characters as password", %{source: source} do
      config = %{
        schema: nil,
        username: "some-invalid",
        password: "!@#$",
        hostname: "localhost",
        port: 5555
      }

      backend = insert(:backend, type: :postgres, sources: [source], config: config)
      assert {:ok, _pid} = start_supervised({PostgresAdaptor, {source, backend}})
    end

    test "cannot connect to invalid ", %{source: source} do
      config = %{
        username: "some-invalid",
        password: "!@#$",
        hostname: "localhost",
        database: "other_db",
        port: 1234
      }

      backend = insert(:backend, type: :postgres, sources: [source], config: config)
      log_event = build(:log_event, source: source, test: "data")

      capture_log(fn ->
        assert {:ok, _pid} = start_supervised({PostgresAdaptor, {source, backend}})

        assert {:error, :cannot_connect} =
                 PostgresAdaptor.insert_log_event(source, backend, log_event)
      end)
    end
  end

  describe "repo module" do
    test "custom schema", %{source: source, postgres_url: url} do
      config = %{
        url: url,
        schema: "my_schema"
      }

      backend = insert(:backend, type: :postgres, sources: [source], config: config)
      PostgresAdaptor.create_repo(backend)

      assert {:ok, [%{"schema_name" => "my_schema"}]} =
               PostgresAdaptor.execute_query(
                 backend,
                 "select schema_name from information_schema.schemata where schema_name = 'my_schema'"
               )

      assert :ok = PostgresAdaptor.create_events_table({source, backend})

      log_event = build(:log_event, source: source, test: "data")
      assert {:ok, 1} = PostgresAdaptor.insert_log_event(source, backend, log_event)
    end

    test "create_events_table/1 creates the table for a given source", %{
      backend: backend,
      source: source
    } do
      repo = PostgresAdaptor.create_repo(backend)
      assert :ok = PostgresAdaptor.create_events_table({source, backend})
      query = from(l in PostgresAdaptor.table_name(source), select: l.body)
      assert repo.all(query) == []
    end
  end

  test "bug: cast_config/1 and validate_config/1 postgresql url variations" do
    assert cast_config(url: "postgresql://localhost:5432").valid?
    assert cast_config(url: "postgres://localhost:5432").valid?

    refute cast_config(url: "://localhost:5432").valid?
    refute cast_config(url: "//localhost:5432").valid?
    refute cast_config(url: "/localhost:5432").valid?
    refute cast_config(url: "postgres//localhost:5432").valid?
  end

  defp cast_config(attrs) do
    attrs
    |> Map.new()
    |> PostgresAdaptor.cast_config()
    |> PostgresAdaptor.validate_config()
  end
end
