defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.ConnectionManagerTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.ConnectionManager

  @resolve_interval :timer.seconds(1)
  @timeout_interval @resolve_interval * 2

  setup do
    insert(:plan, name: "Free")

    {_source, backend} = setup_clickhouse_test()

    [backend: backend]
  end

  describe "child_spec/1" do
    test "generates correct child spec for `Backend`", %{backend: backend} do
      child_spec = ConnectionManager.child_spec(backend)

      assert %{
               id: {ConnectionManager, backend_id},
               start: {ConnectionManager, :start_link, [^backend]}
             } = child_spec

      assert backend_id == backend.id
    end

    test "child specs have unique IDs for different backends" do
      backend1 = insert(:backend, type: :clickhouse)
      backend2 = insert(:backend, type: :clickhouse)

      child_spec1 = ConnectionManager.child_spec(backend1)
      child_spec2 = ConnectionManager.child_spec(backend2)

      assert child_spec1.id != child_spec2.id
      assert child_spec1.id == {ConnectionManager, backend1.id}
      assert child_spec2.id == {ConnectionManager, backend2.id}
    end

    test "child specs work with `Supervisor.start_link`", %{backend: backend} do
      child_spec = ConnectionManager.child_spec(backend)

      {:ok, sup} = Supervisor.start_link([child_spec], strategy: :one_for_one)

      children = Supervisor.which_children(sup)
      assert length(children) == 1

      [{child_id, child_pid, :worker, [ConnectionManager]}] = children
      assert child_id == {ConnectionManager, backend.id}
      assert Process.alive?(child_pid)

      Supervisor.stop(sup)
    end
  end

  describe "connection manager lifecycle" do
    test "starts successfully when provided a backend", %{backend: backend} do
      {:ok, manager_pid} = ConnectionManager.start_link(backend)

      assert Process.alive?(manager_pid)
      refute ConnectionManager.pool_active?(backend)
    end
  end

  describe "query pool connection management" do
    setup context do
      {:ok, _manager_pid} = ConnectionManager.start_link(context.backend)

      context
    end

    test "`ensure_pool_started` starts a new query pool", %{backend: backend} do
      refute ConnectionManager.pool_active?(backend)
      assert :ok == ConnectionManager.ensure_pool_started(backend)
      assert ConnectionManager.pool_active?(backend)
    end

    test "`ensure_pool_started` returns existing connection if already started", %{
      backend: backend
    } do
      assert :ok == ConnectionManager.ensure_pool_started(backend)
      assert :ok == ConnectionManager.ensure_pool_started(backend)
      assert ConnectionManager.pool_active?(backend)
    end

    test "`pool_active?` returns false for non-existent connection pools", %{
      backend: backend
    } do
      refute ConnectionManager.pool_active?(backend)
    end
  end

  describe "query activity tracking" do
    setup context do
      {:ok, _manager_pid} = ConnectionManager.start_link(context.backend)

      context
    end

    test "`notify_activity` updates activity timestamp", %{backend: backend} do
      assert :ok == ConnectionManager.notify_activity(backend)
    end
  end

  describe "connection pool lifecycle resolution" do
    setup context do
      config = Application.get_env(:logflare, ConnectionManager)
      Application.put_env(:logflare, ConnectionManager, resolve_interval: @resolve_interval)

      {:ok, _manager_pid} = ConnectionManager.start_link(context.backend)

      on_exit(fn -> Application.put_env(:logflare, ConnectionManager, config) end)

      context
    end

    test "updates activity timestamp when notified", %{backend: backend} do
      assert :ok == ConnectionManager.ensure_pool_started(backend)

      activity_before = ConnectionManager.get_last_activity(backend)

      Process.sleep(@timeout_interval)

      assert :ok == ConnectionManager.notify_activity(backend)

      activity_after = ConnectionManager.get_last_activity(backend)
      assert activity_after > activity_before
      assert ConnectionManager.pool_active?(backend)
    end

    test "initializes activity timestamp on resolve when not previously set", %{backend: backend} do
      assert :ok == ConnectionManager.ensure_pool_started(backend)
      assert ConnectionManager.pool_active?(backend)

      assert :ok == ConnectionManager.set_last_activity(backend, nil)

      Process.sleep(@timeout_interval)

      assert is_integer(ConnectionManager.get_last_activity(backend))
      assert ConnectionManager.pool_active?(backend)
    end

    test "stops pool after exceeding inactivity timeout", %{backend: backend} do
      assert :ok == ConnectionManager.ensure_pool_started(backend)
      assert ConnectionManager.pool_active?(backend)

      old_timestamp = System.system_time(:millisecond) - :timer.minutes(10)
      assert :ok == ConnectionManager.set_last_activity(backend, old_timestamp)

      Process.sleep(@timeout_interval)

      refute ConnectionManager.pool_active?(backend)
    end
  end

  describe "query pool error handling" do
    setup do
      {_source, invalid_backend} =
        setup_clickhouse_test(
          config: %{
            url: "http://localhost",
            username: "invalid_user",
            password: "invalid_pass",
            port: 19_999
          }
        )

      [invalid_backend: invalid_backend]
    end

    test "handles invalid database configuration", %{invalid_backend: invalid_backend} do
      _manager_pid = start_supervised!({ConnectionManager, invalid_backend})

      case ConnectionManager.ensure_pool_started(invalid_backend) do
        :ok ->
          assert {:error, _reason} = ClickHouseAdaptor.test_connection(invalid_backend)

        {:error, _reason} ->
          refute ConnectionManager.pool_active?(invalid_backend)
      end
    end
  end
end
