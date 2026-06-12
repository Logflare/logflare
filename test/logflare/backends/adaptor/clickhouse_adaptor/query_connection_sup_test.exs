defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryConnectionSupTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.ConnectionManager
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryConnectionSup

  setup do
    insert(:plan, name: "Free")

    {_source, backend} = setup_clickhouse_test()

    [backend: backend]
  end

  describe "list_query_connection_managers/0" do
    test "lists managers for multiple backends", %{backend: backend} do
      {_source, other_backend} = setup_clickhouse_test()

      {:ok, manager_pid} =
        QueryConnectionSup.start_connection_manager(ConnectionManager.child_spec(backend))

      {:ok, other_manager_pid} =
        QueryConnectionSup.start_connection_manager(ConnectionManager.child_spec(other_backend))

      managers = QueryConnectionSup.list_query_connection_managers()

      assert {backend.id, manager_pid} in managers
      assert {other_backend.id, other_manager_pid} in managers
    end

    test "does not include terminated managers", %{backend: backend} do
      {:ok, manager_pid} =
        QueryConnectionSup.start_connection_manager(ConnectionManager.child_spec(backend))

      assert {backend.id, manager_pid} in QueryConnectionSup.list_query_connection_managers()

      QueryConnectionSup.terminate_all()

      TestUtils.retry_assert(fn ->
        refute {backend.id, manager_pid} in QueryConnectionSup.list_query_connection_managers()
      end)
    end
  end

  describe "recycle_backend_local/1" do
    test "returns an error when no manager is running for the backend", %{backend: backend} do
      assert {:error, :no_manager} == QueryConnectionSup.recycle_backend_local(backend.id)
    end

    test "returns an error when the manager has no active pool", %{backend: backend} do
      {:ok, _manager_pid} =
        QueryConnectionSup.start_connection_manager(ConnectionManager.child_spec(backend))

      assert {:error, :no_pool} == QueryConnectionSup.recycle_backend_local(backend.id)
    end

    test "recycles the backend's active pool", %{backend: backend} do
      {:ok, _manager_pid} =
        QueryConnectionSup.start_connection_manager(ConnectionManager.child_spec(backend))

      assert :ok == ConnectionManager.ensure_pool_started(backend)
      assert :ok == QueryConnectionSup.recycle_backend_local(backend.id)
      assert ConnectionManager.pool_active?(backend)
    end
  end

  describe "recycle_backend/1" do
    test "recycles the backend's pool on every node and returns per-node results", %{
      backend: backend
    } do
      {:ok, _manager_pid} =
        QueryConnectionSup.start_connection_manager(ConnectionManager.child_spec(backend))

      assert :ok == ConnectionManager.ensure_pool_started(backend)

      assert QueryConnectionSup.recycle_backend(backend) == %{Node.self() => :ok}
      assert ConnectionManager.pool_active?(backend)
    end

    test "does not touch other backends' pools", %{backend: backend} do
      {_source, other_backend} = setup_clickhouse_test()

      {:ok, _manager_pid} =
        QueryConnectionSup.start_connection_manager(ConnectionManager.child_spec(backend))

      {:ok, _other_manager_pid} =
        QueryConnectionSup.start_connection_manager(ConnectionManager.child_spec(other_backend))

      assert :ok == ConnectionManager.ensure_pool_started(backend)
      assert :ok == ConnectionManager.ensure_pool_started(other_backend)

      other_scheduled_at = ConnectionManager.get_next_recycle_at(other_backend)

      assert QueryConnectionSup.recycle_backend(backend.id) == %{Node.self() => :ok}

      assert ConnectionManager.get_next_recycle_at(other_backend) == other_scheduled_at
    end
  end

  describe "refresh_backend_local/1" do
    test "returns ok when no manager is running", %{backend: backend} do
      assert :ok == QueryConnectionSup.refresh_backend_local(backend.id)
    end

    test "stops the backend's active pool", %{backend: backend} do
      {:ok, _manager_pid} =
        QueryConnectionSup.start_connection_manager(ConnectionManager.child_spec(backend))

      assert :ok == ConnectionManager.ensure_pool_started(backend)
      assert ConnectionManager.pool_active?(backend)

      assert :ok == QueryConnectionSup.refresh_backend_local(backend.id)

      refute ConnectionManager.pool_active?(backend)
    end
  end

  describe "refresh_backend/1" do
    test "stops the backend's pool across the cluster", %{backend: backend} do
      {:ok, _manager_pid} =
        QueryConnectionSup.start_connection_manager(ConnectionManager.child_spec(backend))

      assert :ok == ConnectionManager.ensure_pool_started(backend)

      assert :ok == QueryConnectionSup.refresh_backend(backend)

      TestUtils.retry_assert(fn ->
        refute ConnectionManager.pool_active?(backend)
      end)
    end
  end

  describe "terminate_backend_local/1" do
    test "returns ok when no manager is running", %{backend: backend} do
      assert :ok == QueryConnectionSup.terminate_backend_local(backend.id)
    end

    test "terminates the backend's manager and pool", %{backend: backend} do
      {:ok, manager_pid} =
        QueryConnectionSup.start_connection_manager(ConnectionManager.child_spec(backend))

      assert :ok == ConnectionManager.ensure_pool_started(backend)

      assert :ok == QueryConnectionSup.terminate_backend_local(backend.id)

      refute Process.alive?(manager_pid)
      refute ConnectionManager.pool_active?(backend)

      TestUtils.retry_assert(fn ->
        refute {backend.id, manager_pid} in QueryConnectionSup.list_query_connection_managers()
      end)
    end
  end

  describe "terminate_backend/1" do
    test "terminates managers across the cluster", %{backend: backend} do
      {:ok, manager_pid} =
        QueryConnectionSup.start_connection_manager(ConnectionManager.child_spec(backend))

      assert :ok == QueryConnectionSup.terminate_backend(backend)

      TestUtils.retry_assert(fn ->
        refute Process.alive?(manager_pid)
      end)
    end
  end
end
