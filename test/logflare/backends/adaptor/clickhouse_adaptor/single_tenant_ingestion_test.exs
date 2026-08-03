defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.SingleTenantIngestionTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.ConnectionManager
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Provisioner
  alias Logflare.SingleTenant
  alias Logflare.SystemMetrics.AllLogsLogged

  TestUtils.setup_single_tenant(
    backend_type: :clickhouse,
    seed_user: true,
    clickhouse_backend_adapter_opts: [
      url: "http://localhost:8123",
      database: "logflare_test",
      username: "logflare",
      password: "logflare",
      port: 8123,
      ingest_pool_size: 5,
      query_pool_size: 3
    ]
  )

  setup do
    insert(:plan, name: "Free")

    user = SingleTenant.get_default_user()
    source = insert(:source, user: user)
    backend = Logflare.Backends.get_default_backend(user)

    start_supervised!(AllLogsLogged)
    {:ok, connection_manager_pid} = ConnectionManager.start_link(backend)
    {:ok, supervisor_pid} = ClickHouseAdaptor.start_link(backend)

    cleanup_single_tenant_tables(backend)

    on_exit(fn ->
      cleanup_single_tenant_tables(backend)

      if Process.alive?(supervisor_pid) do
        Process.exit(supervisor_pid, :shutdown)
      end

      if Process.alive?(connection_manager_pid) do
        Process.exit(connection_manager_pid, :shutdown)
      end
    end)

    [source: source, backend: backend]
  end

  test "the pipeline consumes normally ingested events", %{
    source: source,
    backend: backend
  } do
    {:ok, provisioner_pid} = Provisioner.start_link(backend)
    provisioner_ref = Process.monitor(provisioner_pid)
    assert_receive {:DOWN, ^provisioner_ref, :process, ^provisioner_pid, :normal}, 5_000

    message = "synthetic pipeline #{System.unique_integer([:positive])}"
    assert {:ok, 1} = Logflare.Backends.ingest_logs([%{"message" => message}], source)

    table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :log)

    TestUtils.retry_assert([duration: 10_000], fn ->
      assert {:ok, {[%{"count" => 1}], _bytes}} =
               ClickHouseAdaptor.execute_ch_query(
                 backend,
                 "SELECT count(*) AS count FROM #{table_name} WHERE event_message = {message:String}",
                 %{message: message}
               )
    end)
  end

  defp cleanup_single_tenant_tables(backend) do
    {manager_pid, stop_manager?} =
      case ConnectionManager.start_link(backend) do
        {:ok, pid} ->
          Process.unlink(pid)
          {pid, true}

        {:error, {:already_started, pid}} ->
          {pid, false}
      end

    try do
      for event_type <- [:log, :metric, :trace] do
        table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, event_type)
        ClickHouseAdaptor.execute_ch_query(backend, "DROP TABLE IF EXISTS #{table_name}")
      end
    after
      if stop_manager? and Process.alive?(manager_pid) do
        GenServer.stop(manager_pid)
      end
    end

    :ok
  end
end
