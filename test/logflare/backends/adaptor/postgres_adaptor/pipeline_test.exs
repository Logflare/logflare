defmodule Logflare.Backends.Adaptor.PostgresAdaptor.PipelineTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor.LogEvent
  alias Logflare.Backends.Adaptor.PostgresAdaptor.Pipeline
  alias Logflare.Backends.Adaptor.PostgresAdaptor.Repo
  alias Logflare.Buffers.MemoryBuffer

  import Ecto.Query

  setup do
    %{username: username, password: password, database: database, hostname: hostname} =
      Application.get_env(:logflare, Logflare.Repo) |> Map.new()

    url = "postgresql://#{username}:#{password}@#{hostname}/#{database}"

    source = insert(:source, user: insert(:user))
    source_backend = insert(:source_backend, type: :postgres, config: %{url: url}, source: source)
    repository_module = Repo.new_repository_for_source_backend(source_backend)
    pipeline_name = Backends.via_source_backend(source_backend, Pipeline)
    memory_buffer_pid = start_supervised!(MemoryBuffer)

    state = %PostgresAdaptor{
      buffer_module: MemoryBuffer,
      buffer_pid: memory_buffer_pid,
      config: source_backend.config,
      source_backend: source_backend,
      pipeline_name: pipeline_name,
      repository_module: repository_module
    }

    Repo.connect_to_source_backend(repository_module, source_backend,
      pool: Ecto.Adapters.SQL.Sandbox
    )

    Ecto.Adapters.SQL.Sandbox.mode(repository_module, :auto)
    Repo.create_log_event_table(repository_module, source_backend)

    on_exit(fn ->
      Ecto.Migrator.run(repository_module, Repo.migrations(source_backend), :down, all: true)
      migration_table = Keyword.get(repository_module.config(), :migration_source)
      Ecto.Adapters.SQL.query!(repository_module, "DROP TABLE IF EXISTS #{migration_table}")
    end)

    {:ok, _} = Pipeline.start_link(state)

    %{
      memory_buffer_pid: memory_buffer_pid,
      repository_module: repository_module,
      source_backend: source_backend
    }
  end

  describe "postgres ingestion" do
    test "ingests dispatched message", %{
      memory_buffer_pid: memory_buffer_pid,
      repository_module: repository_module,
      source_backend: source_backend
    } do
      log_event =
        build(:log_event,
          token: TestUtils.random_string(),
          source: source_backend.source,
          body: %{"data" => "data"}
        )

      MemoryBuffer.add(memory_buffer_pid, log_event)

      fetcher = fn ->
        repository_module.all(
          from(l in Repo.table_name(source_backend),
            select: %LogEvent{
              id: l.id,
              body: l.body,
              event_message: l.event_message,
              timestamp: l.timestamp
            }
          )
        )
      end

      asserts = fn
        [] ->
          :retry

        [res_log_event] ->
          assert log_event.id == res_log_event.id
          assert log_event.body == res_log_event.body
          assert log_event.body["event_message"] == res_log_event.event_message

          expected_timestamp =
            res_log_event.timestamp
            |> DateTime.from_naive!("Etc/UTC")
            |> DateTime.to_unix(:microsecond)

          assert log_event.body["timestamp"] == expected_timestamp
      end

      TestUtils.retry_fetch(fetcher, asserts)
      assert_received(:done)
    end
  end
end
