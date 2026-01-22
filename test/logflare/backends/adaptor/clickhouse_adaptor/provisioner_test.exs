defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.ProvisionerTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Provisioner

  setup do
    insert(:plan, name: "Free")

    {source, backend, cleanup_fn} = setup_clickhouse_test()
    on_exit(cleanup_fn)

    {:ok, supervisor_pid} = ClickHouseAdaptor.start_link(backend)

    on_exit(fn ->
      if Process.alive?(supervisor_pid) do
        Process.exit(supervisor_pid, :shutdown)
      end
    end)

    [source: source, backend: backend]
  end

  describe "child_spec/1" do
    test "returns correct child specification", %{backend: backend} do
      spec = Provisioner.child_spec(backend)

      assert spec.id == Provisioner
      assert spec.restart == :transient
      assert spec.start == {Provisioner, :start_link, [backend]}
    end
  end

  describe "successful provisioning flow" do
    test "provisions successfully and terminates normally", %{backend: backend} do
      {:ok, pid} = Provisioner.start_link(backend)
      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend)

      {:ok, result} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "EXISTS TABLE #{table_name}"
        )

      assert [%{"result" => 1}] = result
    end

    test "creates all required tables and views", %{backend: backend} do
      {:ok, pid} = Provisioner.start_link(backend)
      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

      ingest_table = ClickHouseAdaptor.clickhouse_ingest_table_name(backend)

      {:ok, ingest_exists} =
        ClickHouseAdaptor.execute_ch_query(backend, "EXISTS TABLE #{ingest_table}")

      assert [%{"result" => 1}] = ingest_exists
    end
  end

  describe "connection test failure handling" do
    test "fails initialization when ClickHouse is unavailable" do
      {_source, invalid_backend, cleanup_fn} =
        setup_clickhouse_test(
          config: %{
            url: "http://invalid-host:9999",
            username: "invalid_user",
            password: "invalid_pass",
            port: 9999
          }
        )

      on_exit(cleanup_fn)

      {:ok, pid} =
        Task.start(fn ->
          ClickHouseAdaptor.start_link(invalid_backend)
        end)

      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 5_000
    end
  end

  describe "provisioning idempotency" do
    test "can run multiple times without errors", %{backend: backend} do
      {:ok, pid1} = Provisioner.start_link(backend)
      ref1 = Process.monitor(pid1)
      assert_receive {:DOWN, ^ref1, :process, ^pid1, :normal}, 5_000

      {:ok, pid2} = Provisioner.start_link(backend)
      ref2 = Process.monitor(pid2)
      assert_receive {:DOWN, ^ref2, :process, ^pid2, :normal}, 5_000

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend)

      {:ok, result} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT count(*) as count FROM #{table_name}"
        )

      assert [%{"count" => 0}] = result
    end
  end

  describe "process lifecycle" do
    test "terminates after successful provisioning", %{backend: backend} do
      {:ok, pid} = Provisioner.start_link(backend)

      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000
    end

    test "can insert data after provisioning completes", %{source: source, backend: backend} do
      {:ok, pid} = Provisioner.start_link(backend)
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

      log_event = build(:log_event, source: source, message: "Test after provisioning")
      :ok = ClickHouseAdaptor.insert_log_events(backend, [log_event])

      Process.sleep(100)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend)

      {:ok, result} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT count(*) as count FROM #{table_name}"
        )

      assert [%{"count" => 1}] = result
    end
  end

  describe "table schema verification" do
    test "creates tables with correct schema", %{backend: backend} do
      {:ok, pid} = Provisioner.start_link(backend)
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend)

      {:ok, columns} =
        ClickHouseAdaptor.execute_ch_query(backend, "DESCRIBE TABLE #{table_name}")

      column_names = Enum.map(columns, & &1["name"])
      assert "id" in column_names
      assert "source_uuid" in column_names
      assert "body" in column_names
      assert "timestamp" in column_names

      id_column = Enum.find(columns, &(&1["name"] == "id"))
      assert %{"type" => "UUID"} = id_column

      source_uuid_column = Enum.find(columns, &(&1["name"] == "source_uuid"))
      assert %{"type" => "UUID"} = source_uuid_column

      timestamp_column = Enum.find(columns, &(&1["name"] == "timestamp"))
      assert %{"type" => "DateTime64(6)"} = timestamp_column
    end
  end
end
