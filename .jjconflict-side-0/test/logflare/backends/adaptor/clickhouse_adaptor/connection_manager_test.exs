defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.ConnectionManagerTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.ConnectionManager

  setup do
    insert(:plan, name: "Free")

    {_source, backend, ch_cleanup_fn} = setup_clickhouse_test()
    on_exit(ch_cleanup_fn)

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

  describe "query pool error handling" do
    setup do
      {_source, invalid_backend, cleanup_fn} =
        setup_clickhouse_test(
          config: %{
            url: "http://invalid-hostname:8123",
            username: "invalid_user",
            password: "invalid_pass",
            port: 9999
          }
        )

      on_exit(cleanup_fn)

      [invalid_backend: invalid_backend]
    end

    test "handles invalid database configuration", %{invalid_backend: invalid_backend} do
      {:ok, _manager_pid} = ConnectionManager.start_link(invalid_backend)

      case ConnectionManager.ensure_pool_started(invalid_backend) do
        :ok ->
          assert {:error, _reason} = ClickHouseAdaptor.test_connection(invalid_backend)

        {:error, _reason} ->
          refute ConnectionManager.pool_active?(invalid_backend)
      end
    end
  end
end
