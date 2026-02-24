defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolManagerTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Pool
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolManager
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolSup

  @inactivity_timeout :timer.minutes(5)

  setup do
    insert(:plan, name: "Free")

    {_source, backend, ch_cleanup_fn} = setup_clickhouse_test()
    on_exit(ch_cleanup_fn)

    [backend: backend]
  end

  describe "child_spec/1" do
    test "generates correct child spec for `Backend`", %{backend: backend} do
      child_spec = PoolManager.child_spec(backend)

      assert %{
               id: {PoolManager, backend_id},
               start: {PoolManager, :start_link, [^backend]}
             } = child_spec

      assert backend_id == backend.id
    end

    test "child specs have unique IDs for different backends" do
      backend1 = insert(:backend, type: :clickhouse)
      backend2 = insert(:backend, type: :clickhouse)

      child_spec1 = PoolManager.child_spec(backend1)
      child_spec2 = PoolManager.child_spec(backend2)

      assert child_spec1.id != child_spec2.id
      assert child_spec1.id == {PoolManager, backend1.id}
      assert child_spec2.id == {PoolManager, backend2.id}
    end
  end

  describe "pool manager lifecycle" do
    test "starts successfully, pool not active initially", %{backend: backend} do
      {:ok, manager_pid} = PoolManager.start_link(backend)

      assert Process.alive?(manager_pid)
      refute PoolManager.pool_active?(backend)
    end
  end

  describe "pool management" do
    setup context do
      {:ok, _manager_pid} = PoolManager.start_link(context.backend)

      context
    end

    test "`ensure_pool_started/1` starts pool and records activity", %{backend: backend} do
      refute PoolManager.pool_active?(backend)
      assert :ok == PoolManager.ensure_pool_started(backend)
      assert PoolManager.pool_active?(backend)

      state = get_manager_state(backend)
      assert is_integer(state.last_activity)
      assert is_pid(state.pool_pid)
    end

    test "`ensure_pool_started/1` is idempotent", %{backend: backend} do
      assert :ok == PoolManager.ensure_pool_started(backend)
      assert :ok == PoolManager.ensure_pool_started(backend)
      assert PoolManager.pool_active?(backend)
    end

    test "`pool_active?/1` returns false when no pool running", %{backend: backend} do
      refute PoolManager.pool_active?(backend)
    end
  end

  describe "activity tracking" do
    setup context do
      {:ok, _manager_pid} = PoolManager.start_link(context.backend)

      context
    end

    test "`notify_activity/1` updates the last_activity timestamp", %{backend: backend} do
      :ok = PoolManager.ensure_pool_started(backend)
      state_before = get_manager_state(backend)

      Process.sleep(10)
      :ok = PoolManager.notify_activity(backend)

      state_after = get_manager_state(backend)
      assert state_after.last_activity > state_before.last_activity
    end

    test "`ensure_pool_started/1` refreshes activity on subsequent calls", %{backend: backend} do
      :ok = PoolManager.ensure_pool_started(backend)
      state_before = get_manager_state(backend)

      Process.sleep(10)
      :ok = PoolManager.ensure_pool_started(backend)

      state_after = get_manager_state(backend)
      assert state_after.last_activity > state_before.last_activity
    end
  end

  describe "inactivity shutdown" do
    setup context do
      {:ok, manager_pid} = PoolManager.start_link(context.backend)
      :ok = PoolManager.ensure_pool_started(context.backend)

      Map.put(context, :manager_pid, manager_pid)
    end

    test "pool is stopped when inactive beyond the timeout", %{
      backend: backend,
      manager_pid: manager_pid
    } do
      assert PoolManager.pool_active?(backend)

      expire_activity(backend)
      send(manager_pid, :resolve_pool_state)
      wait_for_genserver(manager_pid)

      refute PoolManager.pool_active?(backend)
    end

    test "pool is kept alive when activity is within the timeout", %{
      backend: backend,
      manager_pid: manager_pid
    } do
      assert PoolManager.pool_active?(backend)

      send(manager_pid, :resolve_pool_state)
      wait_for_genserver(manager_pid)

      assert PoolManager.pool_active?(backend)
    end

    test "`notify_activity/1` prevents inactivity shutdown", %{
      backend: backend,
      manager_pid: manager_pid
    } do
      assert PoolManager.pool_active?(backend)

      expire_activity(backend)
      :ok = PoolManager.notify_activity(backend)

      send(manager_pid, :resolve_pool_state)
      wait_for_genserver(manager_pid)

      assert PoolManager.pool_active?(backend)
    end
  end

  describe "pool crash recovery" do
    setup context do
      {:ok, _manager_pid} = PoolManager.start_link(context.backend)
      :ok = PoolManager.ensure_pool_started(context.backend)

      context
    end

    test "pool crash sets pool_pid to nil, next ensure re-creates", %{backend: backend} do
      assert PoolManager.pool_active?(backend)

      pool_pid = GenServer.whereis(Pool.via(backend))
      assert is_pid(pool_pid)

      PoolSup.stop_pool(backend)
      Process.sleep(50)

      refute PoolManager.pool_active?(backend)

      assert :ok == PoolManager.ensure_pool_started(backend)
      assert PoolManager.pool_active?(backend)
    end
  end

  defp get_manager_state(backend) do
    backend
    |> manager_via()
    |> :sys.get_state()
  end

  defp expire_activity(backend) do
    via = manager_via(backend)

    :sys.replace_state(via, fn state ->
      %{state | last_activity: System.system_time(:millisecond) - @inactivity_timeout - 1}
    end)
  end

  defp manager_via(backend) do
    Backends.via_backend(backend, PoolManager)
  end

  defp wait_for_genserver(pid) do
    _ = :sys.get_state(pid)
    :ok
  end
end
