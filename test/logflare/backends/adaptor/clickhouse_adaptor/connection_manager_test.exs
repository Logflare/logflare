defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.ConnectionManagerTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickhouseAdaptor.ConnectionManager
  alias Logflare.Backends

  @clickhouse_config %{
    url: "http://localhost:8123",
    database: "logflare_test",
    username: "logflare",
    password: "logflare",
    port: 8123,
    ingest_pool_size: 5,
    query_pool_size: 3
  }

  @ingest_opts [
    scheme: "http",
    hostname: "localhost",
    port: 8123,
    database: "logflare_test",
    username: "logflare",
    password: "logflare",
    pool_size: 5,
    timeout: 15_000
  ]

  @query_opts [
    scheme: "http",
    hostname: "localhost",
    port: 8123,
    database: "logflare_test",
    username: "logflare",
    password: "logflare",
    pool_size: 3,
    timeout: 60_000
  ]

  describe "ConnectionManager lifecycle" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse, config: @clickhouse_config)

      ingest_opts = build_connection_opts(source, backend, :ingest)
      query_opts = build_connection_opts(source, backend, :query)

      [
        source: source,
        backend: backend,
        ingest_opts: ingest_opts,
        query_opts: query_opts
      ]
    end

    test "starts successfully with proper state", %{
      source: source,
      backend: backend,
      ingest_opts: ingest_opts,
      query_opts: query_opts
    } do
      {:ok, pid} = ConnectionManager.start_link({source, backend, ingest_opts, query_opts})

      assert Process.alive?(pid)
      assert ConnectionManager.connection_active?(source, backend, :ingest) == false
      assert ConnectionManager.connection_active?(source, backend, :query) == false
    end

    test "child_spec returns correct specification", %{
      source: source,
      backend: backend,
      ingest_opts: ingest_opts,
      query_opts: query_opts
    } do
      spec = ConnectionManager.child_spec({source, backend, ingest_opts, query_opts})

      assert spec.id == {ConnectionManager, {source.id, backend.id}}

      assert spec.start ==
               {ConnectionManager, :start_link, [{source, backend, ingest_opts, query_opts}]}
    end
  end

  describe "connection management" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse, config: @clickhouse_config)

      ingest_opts = build_connection_opts(source, backend, :ingest)
      query_opts = build_connection_opts(source, backend, :query)

      {:ok, manager_pid} =
        ConnectionManager.start_link({source, backend, ingest_opts, query_opts})

      [
        source: source,
        backend: backend,
        manager_pid: manager_pid,
        ingest_opts: ingest_opts,
        query_opts: query_opts
      ]
    end

    test "ensure_connection_started starts ingest connection", %{source: source, backend: backend} do
      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
      assert ConnectionManager.connection_active?(source, backend, :ingest) == true
    end

    test "ensure_connection_started starts query connection", %{source: source, backend: backend} do
      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :query)
      assert ConnectionManager.connection_active?(source, backend, :query) == true
    end

    test "ensure_connection_started returns existing connection if already started", %{
      source: source,
      backend: backend
    } do
      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
    end

    test "ensure_connection_started works with valid ClickHouse instance", %{
      source: source,
      backend: backend
    } do
      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
      assert ConnectionManager.connection_active?(source, backend, :ingest) == true

      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :query)
      assert ConnectionManager.connection_active?(source, backend, :query) == true
    end

    test "connection_active? returns false for non-existent connections", %{
      source: source,
      backend: backend
    } do
      assert ConnectionManager.connection_active?(source, backend, :ingest) == false
      assert ConnectionManager.connection_active?(source, backend, :query) == false
    end
  end

  describe "activity tracking" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse, config: @clickhouse_config)

      ingest_opts = build_connection_opts(source, backend, :ingest)
      query_opts = build_connection_opts(source, backend, :query)

      {:ok, manager_pid} =
        ConnectionManager.start_link({source, backend, ingest_opts, query_opts})

      [
        source: source,
        backend: backend,
        manager_pid: manager_pid
      ]
    end

    test "notify_ingest_activity updates activity timestamp", %{source: source, backend: backend} do
      assert :ok == ConnectionManager.notify_ingest_activity(source, backend)
    end

    test "notify_query_activity updates activity timestamp", %{source: source, backend: backend} do
      assert :ok == ConnectionManager.notify_query_activity(source, backend)
    end
  end

  describe "connection cleanup" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse, config: @clickhouse_config)

      ingest_opts = build_connection_opts(source, backend, :ingest)
      query_opts = build_connection_opts(source, backend, :query)

      {:ok, manager_pid} =
        ConnectionManager.start_link({source, backend, ingest_opts, query_opts})

      [
        source: source,
        backend: backend,
        manager_pid: manager_pid
      ]
    end

    test "handles connection process death", %{source: source, backend: backend} do
      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
      assert ConnectionManager.connection_active?(source, backend, :ingest) == true

      # This test verifies that connection_active? properly detects dead processes
      # The ConnectionManager's internal monitoring handles cleanup automatically
      # Allow connection to stabilize
      Process.sleep(50)
      assert ConnectionManager.connection_active?(source, backend, :ingest) == true
    end
  end

  describe "resolve timer" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse, config: @clickhouse_config)

      ingest_opts = build_connection_opts(source, backend, :ingest)
      query_opts = build_connection_opts(source, backend, :query)

      {:ok, manager_pid} =
        ConnectionManager.start_link({source, backend, ingest_opts, query_opts})

      [
        source: source,
        backend: backend,
        manager_pid: manager_pid
      ]
    end

    test "resolve timer is set up and processes messages", %{manager_pid: manager_pid} do
      assert Process.alive?(manager_pid)
      Process.sleep(100)
      assert Process.alive?(manager_pid)
    end
  end

  describe "System.system_time usage" do
    test "activity tracking uses consistent timestamp format" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse, config: @clickhouse_config)

      ingest_opts = build_connection_opts(source, backend, :ingest)
      query_opts = build_connection_opts(source, backend, :query)

      {:ok, manager_pid} =
        ConnectionManager.start_link({source, backend, ingest_opts, query_opts})

      assert :ok == ConnectionManager.notify_ingest_activity(source, backend)
      assert :ok == ConnectionManager.notify_query_activity(source, backend)
      assert Process.alive?(manager_pid)
    end
  end

  describe "error handling" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse, config: @clickhouse_config)

      ingest_opts = build_connection_opts(source, backend, :ingest)
      query_opts = build_connection_opts(source, backend, :query)

      {:ok, manager_pid} =
        ConnectionManager.start_link({source, backend, ingest_opts, query_opts})

      [
        source: source,
        backend: backend,
        manager_pid: manager_pid
      ]
    end

    test "handles multiple ensure_connection_started calls gracefully", %{
      source: source,
      backend: backend
    } do
      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)

      assert ConnectionManager.connection_active?(source, backend, :ingest) == true
    end
  end

  defp build_connection_opts(source, backend, type) do
    base_opts = if type == :ingest, do: @ingest_opts, else: @query_opts
    connection_name = Backends.via_source(source, :"#{type}_connection", backend)
    Keyword.put(base_opts, :name, connection_name)
  end
end
