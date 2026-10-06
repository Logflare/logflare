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
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.Backends.ConsolidatedSup
  alias Logflare.TestUtils

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
        Enum.each(caches, & &1.reset())

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
          IngestEventQueue.delete_stale_mappings()

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
  Creates ClickHouse source and backend fixtures.

  Use `provision_clickhouse_tables!/1` to create tables with automatic teardown,
  or `drop_clickhouse_tables_on_exit/1` when testing provisioning or ingestion startup.

  ## Options
  - `:config` - Custom ClickHouse configuration (merged with defaults)
  - `:user` - Existing user to use (creates one if not provided)
  - `:source` - Existing source to use (creates one if not provided)
  - `:default_ingest?` - Whether to set the backend as the default ingest backend
  """
  @spec setup_clickhouse_test(keyword()) ::
          {Logflare.Sources.Source.t(), Logflare.Backends.Backend.t()}
  def setup_clickhouse_test(opts \\ []) do
    config = Keyword.get(opts, :config, %{})

    if not (is_map_key(config, :url) or is_map_key(config, :port)) do
      ensure_clickhouse_reachable!()
    end

    user = Keyword.get(opts, :user) || Logflare.Factory.insert(:user)
    source = Keyword.get(opts, :source) || Logflare.Factory.insert(:source, user: user)

    backend =
      Logflare.Factory.insert(:backend,
        type: :clickhouse,
        config: Map.merge(TestUtils.clickhouse_config(), config),
        default_ingest?: Keyword.get(opts, :default_ingest?, false),
        user: user,
        sources: [source]
      )

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

  @spec provision_clickhouse_tables!(Backend.t()) :: :ok
  def provision_clickhouse_tables!(backend) do
    drop_clickhouse_tables_on_exit(backend)
    assert :ok = ClickHouseAdaptor.provision_ingest_tables(backend)
  end

  @spec drop_clickhouse_tables_on_exit(Backend.t()) :: :ok
  def drop_clickhouse_tables_on_exit(backend) do
    on_exit(fn -> cleanup_clickhouse_tables(backend) end)
  end

  @spec build_clickhouse_connection_opts(Logflare.Sources.Source.t() | nil, Backend.t(), atom()) ::
          keyword()
  def build_clickhouse_connection_opts(source, backend, type) when type in [:ingest, :query] do
    base_opts =
      TestUtils.clickhouse_config()
      |> Map.delete(:url)
      |> Map.to_list()
      |> Keyword.put(:scheme, "http")
      |> Keyword.put(:hostname, "localhost")

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

  Stops ingestion before dropping each type table on the shared test instance.
  A dedicated connection bypasses per-test read routes, credentials and query mocks.
  """
  @spec cleanup_clickhouse_tables(Logflare.Backends.Backend.t()) :: :ok
  def cleanup_clickhouse_tables(backend) do
    case ConsolidatedSup.stop_pipeline(backend.id) do
      :ok -> :ok
      {:error, :not_found} -> :ok
    end

    opts =
      build_clickhouse_connection_opts(nil, backend, :query)
      |> Keyword.merge(pool_size: 1, name: Backends.via_backend(backend, __MODULE__))

    driver = if Mimic.Module.copied?(Ch), do: Mimic.Module.original(Ch), else: Ch
    {:ok, conn} = driver.start_link(opts)

    try do
      failures =
        [:log, :metric, :trace]
        |> Enum.map(fn type ->
          table = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, type)

          {table,
           driver.query(conn, "DROP TABLE IF EXISTS #{table} SYNC", [],
             decode: false,
             timeout: 5_000
           )}
        end)
        |> Enum.reject(&match?({_table, {:ok, _result}}, &1))

      case failures do
        [] -> :ok
        failures -> raise "ClickHouse fixture cleanup failed: #{inspect(failures)}"
      end
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
