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

  @spec provision_clickhouse_tables!(Backend.t()) :: :ok
  def provision_clickhouse_tables!(backend) do
    drop_clickhouse_tables_on_exit(backend)
    assert :ok = ClickHouseAdaptor.provision_ingest_tables(backend)
  end

  @doc """
  Registers teardown to synchronously drop a backend's typed tables after a test,
  stopping its pipeline first. Uses the primary connection, not read routing.
  """
  @spec drop_clickhouse_tables_on_exit(Backend.t()) :: :ok
  def drop_clickhouse_tables_on_exit(%Backend{config: config} = backend) do
    tables =
      Enum.map(
        [:log, :metric, :trace],
        &ClickHouseAdaptor.clickhouse_ingest_table_name(backend, &1)
      )

    {scheme, hostname, port} = EndpointUtils.origin(config.url, config[:port])

    opts = [
      scheme: scheme,
      hostname: hostname,
      port: port,
      database: config.database,
      username: config[:username],
      password: config[:password],
      pool_size: 1,
      timeout: 60_000
    ]

    on_exit(fn ->
      ConsolidatedSup.stop_pipeline(backend.id)
      drop_clickhouse_tables(opts, tables)
    end)
  end

  @spec drop_clickhouse_tables(keyword(), [String.t()]) :: :ok
  defp drop_clickhouse_tables(opts, tables) do
    {:ok, conn} = Ch.start_link(opts)

    try do
      Enum.each(tables, fn table ->
        Ch.query!(conn, "DROP TABLE IF EXISTS #{table} SYNC", [],
          timeout: 60_000,
          pool_timeout: 60_000
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
