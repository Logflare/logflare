defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.SingleTenantIngestionTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Provisioner
  alias Logflare.SingleTenant
  alias Logflare.SystemMetrics.AllLogsLogged

  TestUtils.setup_single_tenant(backend_type: :clickhouse, seed_user: true)

  setup do
    insert(:plan, name: "Free")

    user = SingleTenant.get_default_user()
    source = insert(:source, user: user)
    backend = Logflare.Backends.get_default_backend(user)

    drop_clickhouse_tables_on_exit(backend)
    start_supervised!(AllLogsLogged)
    start_supervised!({ClickHouseAdaptor, backend})

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
end
