defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.ConnectionManagerTest do
  use Logflare.DataCase, async: false
  import Mimic

  alias Logflare.Backends.Adaptor.ClickhouseAdaptor.ConnectionManager
  alias Logflare.Backends

  setup :set_mimic_global
  setup :verify_on_exit!

  @ingest_opts [
    scheme: "http",
    hostname: "localhost",
    port: 8123,
    database: "test_db",
    username: "test_user",
    password: "test_pass",
    pool_size: 5,
    timeout: 15_000
  ]

  @query_opts [
    scheme: "http",
    hostname: "localhost",
    port: 8123,
    database: "test_db",
    username: "test_user",
    password: "test_pass",
    pool_size: 3,
    timeout: 60_000
  ]

  describe "ConnectionManager lifecycle" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :clickhouse,
          config: %{
            url: "http://localhost:8123",
            database: "test_db",
            username: "test_user",
            password: "test_pass",
            port: 8123,
            ingest_pool_size: 5,
            query_pool_size: 3
          }
        )

      ingest_opts =
        Keyword.put(@ingest_opts, :name, Backends.via_source(source, :ingest_connection, backend))

      query_opts =
        Keyword.put(@query_opts, :name, Backends.via_source(source, :query_connection, backend))

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

      backend =
        insert(:backend,
          type: :clickhouse,
          config: %{
            url: "http://localhost:8123",
            database: "test_db",
            username: "test_user",
            password: "test_pass",
            port: 8123,
            ingest_pool_size: 5,
            query_pool_size: 3
          }
        )

      ingest_opts =
        Keyword.put(@ingest_opts, :name, Backends.via_source(source, :ingest_connection, backend))

      query_opts =
        Keyword.put(@query_opts, :name, Backends.via_source(source, :query_connection, backend))

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
      mock_pid = spawn(fn -> Process.sleep(1000) end)

      expect(Ch, :start_link, fn opts ->
        assert opts[:name] == Backends.via_source(source, :ingest_connection, backend)
        {:ok, mock_pid}
      end)

      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
      assert ConnectionManager.connection_active?(source, backend, :ingest) == true

      if Process.alive?(mock_pid), do: Process.exit(mock_pid, :kill)
    end

    test "ensure_connection_started starts query connection", %{source: source, backend: backend} do
      mock_pid = spawn(fn -> Process.sleep(1000) end)

      expect(Ch, :start_link, fn opts ->
        assert opts[:name] == Backends.via_source(source, :query_connection, backend)
        {:ok, mock_pid}
      end)

      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :query)
      assert ConnectionManager.connection_active?(source, backend, :query) == true

      if Process.alive?(mock_pid), do: Process.exit(mock_pid, :kill)
    end

    test "ensure_connection_started returns existing connection if already started", %{
      source: source,
      backend: backend
    } do
      mock_pid = spawn(fn -> Process.sleep(1000) end)

      expect(Ch, :start_link, fn _opts -> {:ok, mock_pid} end)

      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)

      if Process.alive?(mock_pid), do: Process.exit(mock_pid, :kill)
    end

    test "ensure_connection_started returns error on failure", %{source: source, backend: backend} do
      expect(Ch, :start_link, fn _opts -> {:error, :connection_failed} end)

      assert {:error, :connection_failed} ==
               ConnectionManager.ensure_connection_started(source, backend, :ingest)

      assert ConnectionManager.connection_active?(source, backend, :ingest) == false
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

      backend =
        insert(:backend,
          type: :clickhouse,
          config: %{
            url: "http://localhost:8123",
            database: "test_db",
            username: "test_user",
            password: "test_pass",
            port: 8123,
            ingest_pool_size: 5,
            query_pool_size: 3
          }
        )

      ingest_opts =
        Keyword.put(@ingest_opts, :name, Backends.via_source(source, :ingest_connection, backend))

      query_opts =
        Keyword.put(@query_opts, :name, Backends.via_source(source, :query_connection, backend))

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

      backend =
        insert(:backend,
          type: :clickhouse,
          config: %{
            url: "http://localhost:8123",
            database: "test_db",
            username: "test_user",
            password: "test_pass",
            port: 8123,
            ingest_pool_size: 5,
            query_pool_size: 3
          }
        )

      ingest_opts =
        Keyword.put(@ingest_opts, :name, Backends.via_source(source, :ingest_connection, backend))

      query_opts =
        Keyword.put(@query_opts, :name, Backends.via_source(source, :query_connection, backend))

      {:ok, manager_pid} =
        ConnectionManager.start_link({source, backend, ingest_opts, query_opts})

      [
        source: source,
        backend: backend,
        manager_pid: manager_pid
      ]
    end

    test "handles connection process death", %{source: source, backend: backend} do
      mock_pid = spawn(fn -> Process.sleep(50) end)

      expect(Ch, :start_link, fn _opts -> {:ok, mock_pid} end)

      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
      assert ConnectionManager.connection_active?(source, backend, :ingest) == true

      Process.exit(mock_pid, :kill)
      Process.sleep(100)

      assert ConnectionManager.connection_active?(source, backend, :ingest) == false
    end
  end

  describe "resolve timer" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :clickhouse,
          config: %{
            url: "http://localhost:8123",
            database: "test_db",
            username: "test_user",
            password: "test_pass",
            port: 8123,
            ingest_pool_size: 5,
            query_pool_size: 3
          }
        )

      ingest_opts =
        Keyword.put(@ingest_opts, :name, Backends.via_source(source, :ingest_connection, backend))

      query_opts =
        Keyword.put(@query_opts, :name, Backends.via_source(source, :query_connection, backend))

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

      backend =
        insert(:backend,
          type: :clickhouse,
          config: %{
            url: "http://localhost:8123",
            database: "test_db",
            username: "test_user",
            password: "test_pass",
            port: 8123,
            ingest_pool_size: 5,
            query_pool_size: 3
          }
        )

      ingest_opts =
        Keyword.put(@ingest_opts, :name, Backends.via_source(source, :ingest_connection, backend))

      query_opts =
        Keyword.put(@query_opts, :name, Backends.via_source(source, :query_connection, backend))

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

      backend =
        insert(:backend,
          type: :clickhouse,
          config: %{
            url: "http://localhost:8123",
            database: "test_db",
            username: "test_user",
            password: "test_pass",
            port: 8123,
            ingest_pool_size: 5,
            query_pool_size: 3
          }
        )

      ingest_opts =
        Keyword.put(@ingest_opts, :name, Backends.via_source(source, :ingest_connection, backend))

      query_opts =
        Keyword.put(@query_opts, :name, Backends.via_source(source, :query_connection, backend))

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
      mock_pid = spawn(fn -> Process.sleep(1000) end)

      expect(Ch, :start_link, fn _opts -> {:ok, mock_pid} end)

      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)

      assert ConnectionManager.connection_active?(source, backend, :ingest) == true

      if Process.alive?(mock_pid), do: Process.exit(mock_pid, :kill)
    end

    test "handles dead connection detection", %{source: source, backend: backend} do
      dead_pid = spawn(fn -> :ok end)

      expect(Ch, :start_link, fn _opts -> {:ok, dead_pid} end)

      assert :ok == ConnectionManager.ensure_connection_started(source, backend, :ingest)
      Process.sleep(100)
      assert ConnectionManager.connection_active?(source, backend, :ingest) == false
    end
  end
end
