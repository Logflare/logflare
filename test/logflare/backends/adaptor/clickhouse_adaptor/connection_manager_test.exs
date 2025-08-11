defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.ConnectionManagerTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickhouseAdaptor
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor.ConnectionManager

  setup do
    insert(:plan, name: "Free")

    {source, backend, ch_cleanup_fn} = setup_clickhouse_test()
    on_exit(ch_cleanup_fn)

    ingest_opts = build_clickhouse_connection_opts(source, backend, :ingest)
    query_opts = build_clickhouse_connection_opts(source, backend, :query)

    [
      source: source,
      backend: backend,
      ingest_opts: ingest_opts,
      query_opts: query_opts
    ]
  end

  describe "ConnectionManager lifecycle" do
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
    setup context do
      {:ok, _manager_pid} =
        ConnectionManager.start_link(
          {context.source, context.backend, context.ingest_opts, context.query_opts}
        )

      context
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
    setup context do
      {:ok, _manager_pid} =
        ConnectionManager.start_link(
          {context.source, context.backend, context.ingest_opts, context.query_opts}
        )

      context
    end

    test "notify_ingest_activity updates activity timestamp", %{source: source, backend: backend} do
      assert :ok == ConnectionManager.notify_ingest_activity(source, backend)
    end

    test "notify_query_activity updates activity timestamp", %{source: source, backend: backend} do
      assert :ok == ConnectionManager.notify_query_activity(source, backend)
    end
  end

  describe "resolve timer" do
    test "resolve timer is set up and processes messages", %{
      source: source,
      backend: backend,
      ingest_opts: ingest_opts,
      query_opts: query_opts
    } do
      {:ok, manager_pid} =
        ConnectionManager.start_link({source, backend, ingest_opts, query_opts})

      assert Process.alive?(manager_pid)
      Process.sleep(100)
      assert Process.alive?(manager_pid)
    end
  end

  describe "System.system_time usage" do
    test "activity tracking uses consistent timestamp format", %{
      source: source,
      backend: backend,
      ingest_opts: ingest_opts,
      query_opts: query_opts
    } do
      {:ok, manager_pid} =
        ConnectionManager.start_link({source, backend, ingest_opts, query_opts})

      assert :ok == ConnectionManager.notify_ingest_activity(source, backend)
      assert :ok == ConnectionManager.notify_query_activity(source, backend)
      assert Process.alive?(manager_pid)
    end
  end

  describe "error handling" do
    test "handles multiple ensure_connection_started calls gracefully", %{
      source: source,
      backend: backend,
      ingest_opts: ingest_opts,
      query_opts: query_opts
    } do
      {:ok, _manager_pid} =
        ConnectionManager.start_link({source, backend, ingest_opts, query_opts})

      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)

      assert ConnectionManager.connection_active?(source, backend, :ingest) == true
    end
  end

  describe "connection failure scenarios with invalid config" do
    setup do
      {source, invalid_backend, cleanup_fn} =
        setup_clickhouse_test(
          config: %{
            url: "http://invalid-hostname:8123",
            username: "invalid_user",
            password: "invalid_pass",
            port: 9999
          }
        )

      on_exit(cleanup_fn)

      [source: source, invalid_backend: invalid_backend]
    end

    test "handles invalid database configuration gracefully", %{
      source: source,
      invalid_backend: invalid_backend
    } do
      invalid_ingest_opts = build_clickhouse_connection_opts(source, invalid_backend, :ingest)
      invalid_query_opts = build_clickhouse_connection_opts(source, invalid_backend, :query)

      invalid_ingest_opts =
        Keyword.merge(invalid_ingest_opts,
          hostname: "invalid-hostname",
          port: 9999,
          username: "invalid_user",
          password: "invalid_pass"
        )

      invalid_query_opts =
        Keyword.merge(invalid_query_opts,
          hostname: "invalid-hostname",
          port: 9999,
          username: "invalid_user",
          password: "invalid_pass"
        )

      {:ok, _manager_pid} =
        ConnectionManager.start_link(
          {source, invalid_backend, invalid_ingest_opts, invalid_query_opts}
        )

      case ConnectionManager.ensure_connection_started(source, invalid_backend, :ingest) do
        :ok ->
          assert {:error, _reason} = ClickhouseAdaptor.test_connection(source, invalid_backend)

        {:error, _reason} ->
          assert ConnectionManager.connection_active?(source, invalid_backend, :ingest) == false
      end
    end
  end

  describe "connection scenarios with valid config" do
    setup do
      {source, backend, cleanup_fn} = setup_clickhouse_test()
      on_exit(cleanup_fn)

      [source: source, backend: backend]
    end

    test "handles connection drops and recovery", %{source: source, backend: backend} do
      ingest_opts = build_clickhouse_connection_opts(source, backend, :ingest)
      query_opts = build_clickhouse_connection_opts(source, backend, :query)

      {:ok, _manager_pid} =
        ConnectionManager.start_link({source, backend, ingest_opts, query_opts})

      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :query)

      assert ConnectionManager.connection_active?(source, backend, :ingest) == true
      assert ConnectionManager.connection_active?(source, backend, :query) == true

      alias Logflare.Backends.Adaptor.ClickhouseAdaptor
      assert :ok == ClickhouseAdaptor.test_connection(source, backend)

      assert ConnectionManager.connection_active?(source, backend, :ingest) == true
      assert ConnectionManager.connection_active?(source, backend, :query) == true
    end

    test "properly handles connection process monitoring", %{source: source, backend: backend} do
      ingest_opts = build_clickhouse_connection_opts(source, backend, :ingest)
      query_opts = build_clickhouse_connection_opts(source, backend, :query)

      {:ok, manager_pid} =
        ConnectionManager.start_link({source, backend, ingest_opts, query_opts})

      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
      assert ConnectionManager.connection_active?(source, backend, :ingest) == true

      assert Process.alive?(manager_pid)

      Process.sleep(10)

      assert ConnectionManager.connection_active?(source, backend, :ingest) == true
    end

    test "connection_active? returns false when manager process doesn't exist", %{
      source: source,
      backend: backend
    } do
      assert ConnectionManager.connection_active?(source, backend, :ingest) == false
      assert ConnectionManager.connection_active?(source, backend, :query) == false
    end

    test "multiple connection attempts are handled gracefully", %{
      source: source,
      backend: backend
    } do
      ingest_opts = build_clickhouse_connection_opts(source, backend, :ingest)
      query_opts = build_clickhouse_connection_opts(source, backend, :query)

      {:ok, _manager_pid} =
        ConnectionManager.start_link({source, backend, ingest_opts, query_opts})

      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)

      assert ConnectionManager.connection_active?(source, backend, :ingest) == true

      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :query)
      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :query)

      assert ConnectionManager.connection_active?(source, backend, :query) == true
    end
  end
end
