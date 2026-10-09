defmodule Logflare.DataCase do
  @moduledoc """
  This module defines the setup for tests requiring
  access to the application's data layer.

  You may define functions here to be used as helpers in
  your tests.

  Finally, if the test case interacts with the database,
  it cannot be async. For this reason, every test runs
  inside a transaction which is reset at the beginning
  of the test unless the test case is marked as async.
  """

  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.EndpointUtils
  alias Logflare.Backends.Backend
  alias Logflare.Backends.ConsolidatedSup

  using do
    quote do
      alias Logflare.Repo
      alias Logflare.TestUtils
      alias Logflare.TestUtilsGrpc
      alias Logflare.Backends.Adaptor.ClickHouseAdaptor
      alias Logflare.Backends.IngestEventQueue
      alias Logflare.PubSubRates
      require TestUtils

      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import Logflare.DataCase
      import Logflare.Factory
      use Mimic

      setup context do
        Mimic.verify_on_exit!(context)
        stub(Logflare.Mailer)
        stub(Goth, :fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)

        stub(Logflare.Cluster.Utils, :rpc_call, fn _node, func ->
          func.()
        end)

        caches = Logflare.ContextCache.Supervisor.list_caches()
        Enum.each(caches, &Cachex.reset(&1, hooks: [Cachex.Stats]))

        on_exit(fn ->
          # Deterministic, not timer-based: IngestEventQueue's generation-store
          # tables are owned by its own long-lived GenServer, not by this (ephemeral)
          # test process, so nothing but an explicit :ets.delete ever reclaims them —
          # unlike delete_all_mappings/0 below, which only clears the mapper's rows.
          # Without this, that data is a global, never-restarted-between-tests
          # singleton that only GenerationJanitor's production-tuned timer ever
          # sweeps, letting residue from every prior test accumulate for the rest of
          # the suite run. Pruning here, keyed off the same liveness check
          # GenerationJanitor's own pruning uses, converges that to near-zero
          # regardless of how that timer happens to be tuned. Order matters: this
          # must run before delete_all_mappings/0 wipes the mapper, so "live" is
          # judged against genuinely-still-live state — including other concurrently
          # running tests' own queues_keys — not a mapper this same callback is about
          # to clear out from under them.
          for queues_key <- IngestEventQueue.list_generation_queues_keys(),
              IngestEventQueue.list_queues(queues_key) == [] do
            IngestEventQueue.prune_generations(queues_key)
          end

          IngestEventQueue.delete_all_mappings()
          PubSubRates.Cache.clear()
          ClickHouseAdaptor.QueryConnectionSup.terminate_all()
        end)

        :ok
      end
    end
  end

  setup tags do
    setup_sandbox(tags)
    setup_mocking(tags)

    :ok
  end

  @doc """
  Sets up the sandbox based on the test tags.
  """
  def setup_sandbox(tags) do
    pid = SQL.Sandbox.start_owner!(Logflare.Repo, shared: not tags[:async])
    on_exit(fn -> SQL.Sandbox.stop_owner(pid) end)
  end

  @doc """
  Sets up mocking configuration based on the test tags.
  """
  def setup_mocking(tags) do
    if !tags[:async] do
      # for global Mimic mocks
      Mimic.set_mimic_global(tags)
    end
  end

  @doc """
  A helper that transforms changeset errors to a map of messages.

  Delegates to `LogflareWeb.Utils.changeset_errors/1`.

      assert {:error, changeset} = Accounts.create_user(%{password: "short"})
      assert "password is too short" in errors_on(changeset).password
      assert %{password: ["password is too short"]} = errors_on(changeset)

  """
  defdelegate errors_on(changeset), to: LogflareWeb.Utils, as: :changeset_errors

  @doc """
  Sets up a ClickHouse test environment with automatic cleanup.

  Returns `{source, backend}` tuple. Registers cleanup via `on_exit/1`.

  ## Options
  - `:config` - Custom ClickHouse configuration (merged with defaults)
  - `:user` - Existing user to use (creates one if not provided)
  - `:source` - Existing source to use (creates one if not provided)
  - `:default_ingest_backend?` - Whether to set the backend as the default ingest backend (requires a source to be provided with the default ingest backend option set to true)
  - `:cleanup?` - Whether to drop the backend's ClickHouse tables on exit (defaults to true)
  """
  def setup_clickhouse_test(opts \\ []) do
    config = Keyword.get(opts, :config, %{})
    default_ingest_backend? = Keyword.get(opts, :default_ingest_backend?, false)

    if not (is_map_key(config, :url) or is_map_key(config, :port)) do
      ensure_clickhouse_reachable!()
    end

    user =
      case Keyword.get(opts, :user) do
        nil ->
          Logflare.Factory.insert(:user)

        existing_user ->
          existing_user
      end

    source = Keyword.get(opts, :source) || Logflare.Factory.insert(:source, user: user)

    default_config = %{
      url: "http://localhost:8123",
      database: "logflare_test",
      username: "logflare",
      password: "logflare",
      port: 8123,
      ingest_pool_size: 5,
      read_pool_size: 3,
      labeled_read_pool_size: 3
    }

    backend =
      Logflare.Factory.insert(:backend,
        type: :clickhouse,
        config: Map.merge(default_config, config),
        default_ingest?: default_ingest_backend?,
        user: user,
        sources: [source]
      )

    if Keyword.get(opts, :cleanup?, true) do
      on_exit(fn -> cleanup_clickhouse_tables(backend) end)
    end

    {source, backend}
  end

  @spec ensure_clickhouse_reachable!() :: :ok
  defp ensure_clickhouse_reachable! do
    case :gen_tcp.connect(~c"localhost", 8123, [:binary, active: false], 500) do
      {:ok, socket} ->
        :gen_tcp.close(socket)

      {:error, reason} ->
        raise "ClickHouse is not reachable on localhost:8123 (#{inspect(reason)}). " <>
                "Start it with `docker compose up -d clickhouse`."
    end
  end

  @doc """
  Builds ClickHouse connection options for testing.
  """
  def build_clickhouse_connection_opts(source, backend, type) when type in [:ingest, :query] do
    base_opts = [
      scheme: "http",
      hostname: "localhost",
      port: 8123,
      database: "logflare_test",
      username: "logflare",
      password: "logflare"
    ]

    type_specific_opts =
      case type do
        :ingest -> [pool_size: 5, timeout: 15_000]
        :query -> [pool_size: 3, timeout: 60_000]
      end

    connection_name =
      case type do
        :ingest -> ClickHouseAdaptor.connection_pool_via({source, backend})
        :query -> ClickHouseAdaptor.connection_pool_via(backend)
      end

    base_opts
    |> Keyword.merge(type_specific_opts)
    |> Keyword.put(:name, connection_name)
  end

  @doc """
  Cleanup ClickHouse tables for a given `Backend`.

  Drops all type-specific tables (`_logs`, `_metrics`, `_traces`).
  """
  @spec cleanup_clickhouse_tables(Backend.t()) :: :ok
  def cleanup_clickhouse_tables(%Backend{config: config} = backend) do
    # Stop ingestion before dropping tables to avoid writes racing with cleanup.
    ConsolidatedSup.stop_pipeline(backend.id)

    {scheme, hostname, port} = EndpointUtils.origin(config.url, Map.get(config, :port))

    connection_opts = [
      scheme: scheme,
      hostname: hostname,
      port: port,
      database: config.database,
      username: config.username,
      password: config.password,
      pool_size: 1
    ]

    {:ok, conn} = DBConnection.start_link(Ch.Connection, connection_opts)

    # Normal caller exits do not stop this linked pool, so close it explicitly.
    try do
      Enum.each([:log, :metric, :trace], fn type ->
        table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, type)

        Ch.query!(conn, "DROP TABLE IF EXISTS {table:Identifier}", %{"table" => table_name},
          timeout: to_timeout(second: 1)
        )
      end)
    after
      GenServer.stop(conn)
    end
  end

  def allow_context_cache_sandbox do
    Logflare.ContextCache.Supervisor.list_caches()
    |> Enum.each(fn cache ->
      allow_sandbox(cache)
      allow_sandbox(:"#{cache}_courier")
    end)
  end

  defp allow_sandbox(process_name) do
    if pid = Process.whereis(process_name) do
      Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, self(), pid)
    end
  end
end
