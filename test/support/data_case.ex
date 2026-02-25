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

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor

  using do
    quote do
      alias Logflare.Repo
      alias Logflare.TestUtils
      alias Logflare.TestUtilsGrpc
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
          Logflare.Backends.IngestEventQueue.delete_all_mappings()
          Logflare.PubSubRates.Cache.clear()
          Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryConnectionSup.terminate_all()
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
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Logflare.Repo, shared: not tags[:async])
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)
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
  A helper that transform changeset errors to a map of messages.

      assert {:error, changeset} = Accounts.create_user(%{password: "short"})
      assert "password is too short" in errors_on(changeset).password
      assert %{password: ["password is too short"]} = errors_on(changeset)

  """
  def errors_on(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {message, opts} ->
      Enum.reduce(opts, message, fn {key, value}, acc ->
        String.replace(acc, "%{#{key}}", to_string(value))
      end)
    end)
  end

  @doc """
  Sets up a ClickHouse test environment with automatic cleanup.

  Returns `{source, backend, cleanup_fn}` tuple.

  ## Options
  - `:config` - Custom ClickHouse configuration (merged with defaults)
  - `:user` - Existing user to use (creates one if not provided)
  - `:source` - Existing source to use (creates one if not provided)
  - `:default_ingest_backend?` - Whether to set the backend as the default ingest backend (requires a source to be provided with the default ingest backend option set to true)
  """
  def setup_clickhouse_test(opts \\ []) do
    config = Keyword.get(opts, :config, %{})
    default_ingest_backend? = Keyword.get(opts, :default_ingest_backend?, false)

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
      query_pool_size: 3
    }

    backend =
      Logflare.Factory.insert(:backend,
        type: :clickhouse,
        config: Map.merge(default_config, config),
        default_ingest?: default_ingest_backend?,
        user: user,
        sources: [source]
      )

    cleanup_fn = fn -> cleanup_clickhouse_tables(backend) end

    {source, backend, cleanup_fn}
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
  def cleanup_clickhouse_tables(backend) do
    tables =
      Enum.flat_map([:log, :metric, :trace], fn type ->
        [
          ClickHouseAdaptor.clickhouse_ingest_table_name(backend, type),
          ClickHouseAdaptor.simple_clickhouse_ingest_table_name(backend, type)
        ]
      end)

    for table_name <- tables do
      try do
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "DROP TABLE IF EXISTS #{table_name}"
        )
      rescue
        _ -> :ok
      catch
        :exit, _ -> :ok
      end
    end
  end
end
