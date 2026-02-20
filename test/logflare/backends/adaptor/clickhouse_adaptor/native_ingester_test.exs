defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngesterTest do
  @moduledoc false

  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Connection
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryTemplates

  @connect_opts [
    host: "localhost",
    port: 9000,
    database: "logflare_test",
    username: "logflare",
    password: "logflare"
  ]

  @test_tables [
    "native_insert_basic",
    "native_insert_types",
    "native_insert_large",
    "native_insert_reuse",
    "native_otel_logs",
    "native_otel_metrics",
    "native_otel_traces"
  ]

  setup do
    insert(:plan, name: "Free")
    {_source, backend, cleanup_fn} = setup_clickhouse_test()
    start_supervised!({ClickHouseAdaptor, backend})

    on_exit(fn ->
      for table <- @test_tables do
        ClickHouseAdaptor.execute_ch_query(backend, "DROP TABLE IF EXISTS #{table}")
      end

      cleanup_fn.()
    end)

    [backend: backend]
  end

  describe "insert/4" do
    test "inserts basic types and verifies data via HTTP", %{backend: backend} do
      {:ok, _} =
        ClickHouseAdaptor.execute_ch_query(backend, """
        CREATE TABLE IF NOT EXISTS native_insert_basic (
          id UInt64,
          name String,
          value Float64,
          active Bool
        ) ENGINE = MergeTree() ORDER BY id
        """)

      {:ok, conn} = Connection.connect(@connect_opts)

      columns = [
        {"id", "UInt64", [1, 2, 3]},
        {"name", "String", ["alice", "bob", "charlie"]},
        {"value", "Float64", [1.5, 2.7, 3.14]},
        {"active", "Bool", [true, false, true]}
      ]

      assert {:ok, conn} = NativeIngester.insert(conn, "native_insert_basic", columns)

      # Verify data via HTTP read
      {:ok, rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT id, name, value, active FROM native_insert_basic ORDER BY id"
        )

      assert length(rows) == 3

      assert Enum.at(rows, 0)["id"] == 1
      assert Enum.at(rows, 0)["name"] == "alice"
      assert_in_delta Enum.at(rows, 0)["value"], 1.5, 0.001
      assert Enum.at(rows, 0)["active"] == true

      assert Enum.at(rows, 1)["id"] == 2
      assert Enum.at(rows, 1)["name"] == "bob"
      assert_in_delta Enum.at(rows, 1)["value"], 2.7, 0.001
      assert Enum.at(rows, 1)["active"] == false

      assert Enum.at(rows, 2)["id"] == 3
      assert Enum.at(rows, 2)["name"] == "charlie"
      assert_in_delta Enum.at(rows, 2)["value"], 3.14, 0.001
      assert Enum.at(rows, 2)["active"] == true

      assert :ok = Connection.close(conn)
    end

    test "inserts DateTime, DateTime64, and UUID types", %{backend: backend} do
      {:ok, _} =
        ClickHouseAdaptor.execute_ch_query(backend, """
        CREATE TABLE IF NOT EXISTS native_insert_types (
          id UInt64,
          uuid UUID,
          ts DateTime,
          ts_precise DateTime64(9)
        ) ENGINE = MergeTree() ORDER BY id
        """)

      {:ok, conn} = Connection.connect(@connect_opts)

      uuid1 = Ecto.UUID.generate()
      uuid2 = Ecto.UUID.generate()
      {:ok, uuid1_raw} = Ecto.UUID.dump(uuid1)
      {:ok, uuid2_raw} = Ecto.UUID.dump(uuid2)

      # DateTime as Unix epoch seconds
      ts1 = 1_700_000_000
      ts2 = 1_700_000_060

      # DateTime64(9) as nanoseconds since epoch
      ts64_1 = 1_700_000_000_123_456_789
      ts64_2 = 1_700_000_060_987_654_321

      columns = [
        {"id", "UInt64", [1, 2]},
        {"uuid", "UUID", [uuid1_raw, uuid2_raw]},
        {"ts", "DateTime", [ts1, ts2]},
        {"ts_precise", "DateTime64(9)", [ts64_1, ts64_2]}
      ]

      assert {:ok, conn} = NativeIngester.insert(conn, "native_insert_types", columns)

      {:ok, rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT id, toString(uuid) as uuid_str, toUnixTimestamp(ts) as ts FROM native_insert_types ORDER BY id"
        )

      assert length(rows) == 2

      assert Enum.at(rows, 0)["id"] == 1
      assert String.downcase(Enum.at(rows, 0)["uuid_str"]) == String.downcase(uuid1)
      assert Enum.at(rows, 0)["ts"] == ts1

      assert Enum.at(rows, 1)["id"] == 2
      assert String.downcase(Enum.at(rows, 1)["uuid_str"]) == String.downcase(uuid2)
      assert Enum.at(rows, 1)["ts"] == ts2

      # Verify DateTime64 precision separately with nanosecond extraction
      {:ok, ts_rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT id, toUnixTimestamp64Nano(ts_precise) as ts_ns FROM native_insert_types ORDER BY id"
        )

      assert Enum.at(ts_rows, 0)["ts_ns"] == ts64_1
      assert Enum.at(ts_rows, 1)["ts_ns"] == ts64_2

      assert :ok = Connection.close(conn)
    end

    test "returns exception when column name does not exist", %{backend: backend} do
      {:ok, _} =
        ClickHouseAdaptor.execute_ch_query(backend, """
        CREATE TABLE IF NOT EXISTS native_insert_basic (
          id UInt64,
          name String,
          value Float64,
          active Bool
        ) ENGINE = MergeTree() ORDER BY id
        """)

      {:ok, conn} = Connection.connect(@connect_opts)

      # Wrong column name: "wrong_name" doesn't exist in the table
      # ClickHouse rejects this at the SQL level before our validation runs
      columns = [
        {"id", "UInt64", [1]},
        {"wrong_name", "String", ["test"]},
        {"value", "Float64", [1.0]},
        {"active", "Bool", [true]}
      ]

      assert {:error, {:exception, _code, message}} =
               NativeIngester.insert(conn, "native_insert_basic", columns)

      assert message =~ "wrong_name"

      Connection.close(conn)
    end

    test "returns column mismatch error when column types differ", %{backend: backend} do
      {:ok, _} =
        ClickHouseAdaptor.execute_ch_query(backend, """
        CREATE TABLE IF NOT EXISTS native_insert_basic (
          id UInt64,
          name String,
          value Float64,
          active Bool
        ) ENGINE = MergeTree() ORDER BY id
        """)

      {:ok, conn} = Connection.connect(@connect_opts)

      # Wrong type: "Int32" instead of "Float64" for value
      columns = [
        {"id", "UInt64", [1]},
        {"name", "String", ["test"]},
        {"value", "Int32", [1]},
        {"active", "Bool", [true]}
      ]

      assert {:error, {:column_mismatch, opts}} =
               NativeIngester.insert(conn, "native_insert_basic", columns)

      assert Keyword.get(opts, :got) == [
               {"id", "UInt64"},
               {"name", "String"},
               {"value", "Int32"},
               {"active", "Bool"}
             ]

      Connection.close(conn)
    end

    test "inserts large batch with sub-block splitting", %{backend: backend} do
      {:ok, _} =
        ClickHouseAdaptor.execute_ch_query(backend, """
        CREATE TABLE IF NOT EXISTS native_insert_large (
          id UInt64,
          name String,
          value Float64
        ) ENGINE = MergeTree() ORDER BY id
        """)

      {:ok, conn} = Connection.connect(@connect_opts)

      # 25K rows — triggers sub-block splitting (threshold is 10K)
      num_rows = 25_000

      columns = [
        {"id", "UInt64", Enum.to_list(1..num_rows)},
        {"name", "String", Enum.map(1..num_rows, &"row_#{&1}")},
        {"value", "Float64", Enum.map(1..num_rows, &(&1 * 0.1))}
      ]

      assert {:ok, conn} = NativeIngester.insert(conn, "native_insert_large", columns)

      # Verify row count via HTTP
      {:ok, rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT count() as cnt FROM native_insert_large"
        )

      assert Enum.at(rows, 0)["cnt"] == num_rows

      # Spot-check first and last rows
      {:ok, rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT id, name, value FROM native_insert_large ORDER BY id LIMIT 1"
        )

      assert Enum.at(rows, 0)["id"] == 1
      assert Enum.at(rows, 0)["name"] == "row_1"

      {:ok, rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT id, name, value FROM native_insert_large ORDER BY id DESC LIMIT 1"
        )

      assert Enum.at(rows, 0)["id"] == num_rows
      assert Enum.at(rows, 0)["name"] == "row_#{num_rows}"

      assert :ok = Connection.close(conn)
    end

    test "returns exception for non-existent table" do
      {:ok, conn} = Connection.connect(@connect_opts)

      columns = [{"id", "UInt64", [1]}]

      assert {:error, {:exception, _code, message}} =
               NativeIngester.insert(conn, "non_existent_table_xyz", columns)

      assert message =~ "non_existent_table_xyz"

      Connection.close(conn)
    end

    test "supports multiple inserts on the same connection", %{backend: backend} do
      {:ok, _} =
        ClickHouseAdaptor.execute_ch_query(backend, """
        CREATE TABLE IF NOT EXISTS native_insert_reuse (
          id UInt64,
          name String
        ) ENGINE = MergeTree() ORDER BY id
        """)

      {:ok, conn} = Connection.connect(@connect_opts)

      # First insert
      columns1 = [
        {"id", "UInt64", [1, 2]},
        {"name", "String", ["first", "second"]}
      ]

      assert {:ok, conn} = NativeIngester.insert(conn, "native_insert_reuse", columns1)

      # Second insert on the same connection
      columns2 = [
        {"id", "UInt64", [3, 4]},
        {"name", "String", ["third", "fourth"]}
      ]

      assert {:ok, conn} = NativeIngester.insert(conn, "native_insert_reuse", columns2)

      # Third insert
      columns3 = [
        {"id", "UInt64", [5]},
        {"name", "String", ["fifth"]}
      ]

      assert {:ok, conn} = NativeIngester.insert(conn, "native_insert_reuse", columns3)

      # Verify all 5 rows landed
      {:ok, rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT id, name FROM native_insert_reuse ORDER BY id"
        )

      assert length(rows) == 5
      assert Enum.map(rows, & &1["id"]) == [1, 2, 3, 4, 5]
      assert Enum.map(rows, & &1["name"]) == ["first", "second", "third", "fourth", "fifth"]

      assert :ok = Connection.close(conn)
    end
  end

  describe "OTEL logs table insert" do
    test "inserts all column types used in the logs DDL", %{backend: backend} do
      ddl = QueryTemplates.create_table_statement("native_otel_logs", :log, ttl_days: 0)
      {:ok, _} = ClickHouseAdaptor.execute_ch_query(backend, ddl)

      {:ok, conn} = Connection.connect(@connect_opts)

      {:ok, id_raw} = Ecto.UUID.dump(Ecto.UUID.generate())
      {:ok, mapping_id_raw} = Ecto.UUID.dump(Ecto.UUID.generate())
      ts_ns = 1_700_000_000_123_456_789

      columns = [
        {"id", "UUID", [id_raw]},
        {"source_uuid", "LowCardinality(String)", ["src-uuid-1"]},
        {"source_name", "LowCardinality(String)", ["my_source"]},
        {"project", "String", ["my_project"]},
        {"trace_id", "String", ["abc123"]},
        {"span_id", "String", ["span456"]},
        {"trace_flags", "UInt8", [1]},
        {"severity_text", "LowCardinality(String)", ["INFO"]},
        {"severity_number", "UInt8", [9]},
        {"service_name", "LowCardinality(String)", ["my_service"]},
        {"event_message", "String", ["test log msg"]},
        {"scope_name", "String", ["my_scope"]},
        {"scope_version", "LowCardinality(String)", ["1.0.0"]},
        {"scope_schema_url", "LowCardinality(String)", [""]},
        {"resource_schema_url", "LowCardinality(String)", [""]},
        {"resource_attributes", "JSON(max_dynamic_types=1, max_dynamic_paths=0)",
         [%{"host" => "localhost"}]},
        {"scope_attributes", "JSON(max_dynamic_types=1, max_dynamic_paths=0)", [%{}]},
        {"log_attributes", "JSON(max_dynamic_types=1, max_dynamic_paths=0)",
         [%{"level" => "info"}]},
        {"mapping_config_id", "UUID", [mapping_id_raw]},
        {"timestamp", "DateTime64(9)", [ts_ns]}
      ]

      assert {:ok, conn} = NativeIngester.insert(conn, "native_otel_logs", columns)

      {:ok, rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT project, event_message, severity_number, trace_id FROM native_otel_logs"
        )

      assert length(rows) == 1
      row = Enum.at(rows, 0)
      assert row["project"] == "my_project"
      assert row["event_message"] == "test log msg"
      assert row["severity_number"] == 9
      assert row["trace_id"] == "abc123"

      assert :ok = Connection.close(conn)
    end
  end

  describe "OTEL metrics table insert" do
    test "inserts all column types used in the metrics DDL", %{backend: backend} do
      ddl = QueryTemplates.create_table_statement("native_otel_metrics", :metric, ttl_days: 0)
      {:ok, _} = ClickHouseAdaptor.execute_ch_query(backend, ddl)

      {:ok, conn} = Connection.connect(@connect_opts)

      {:ok, id_raw} = Ecto.UUID.dump(Ecto.UUID.generate())
      {:ok, mapping_id_raw} = Ecto.UUID.dump(Ecto.UUID.generate())
      ts_ns = 1_700_000_000_123_456_789
      exemplar_ts = 1_700_000_000_500_000_000

      columns = [
        {"id", "UUID", [id_raw]},
        {"source_uuid", "LowCardinality(String)", ["src-uuid-1"]},
        {"source_name", "LowCardinality(String)", ["my_source"]},
        {"project", "String", ["my_project"]},
        {"time_unix", "Nullable(DateTime64(9))", [ts_ns]},
        {"start_time_unix", "Nullable(DateTime64(9))", [nil]},
        {"metric_name", "LowCardinality(String)", ["http.request.duration"]},
        {"metric_description", "String", ["Duration of HTTP requests"]},
        {"metric_unit", "LowCardinality(String)", ["ms"]},
        {"metric_type",
         "Enum8('gauge' = 1, 'sum' = 2, 'histogram' = 3, 'exponential_histogram' = 4, 'summary' = 5)",
         [3]},
        {"service_name", "LowCardinality(String)", ["my_service"]},
        {"event_message", "String", [""]},
        {"scope_name", "String", ["otel_scope"]},
        {"scope_version", "LowCardinality(String)", ["1.0.0"]},
        {"scope_schema_url", "LowCardinality(String)", [""]},
        {"resource_schema_url", "LowCardinality(String)", [""]},
        {"resource_attributes", "JSON(max_dynamic_types=1, max_dynamic_paths=0)",
         [%{"host" => "localhost"}]},
        {"scope_attributes", "JSON(max_dynamic_types=1, max_dynamic_paths=0)", [%{}]},
        {"attributes", "JSON(max_dynamic_types=1, max_dynamic_paths=0)",
         [%{"http.method" => "GET"}]},
        {"aggregation_temporality", "LowCardinality(String)", ["cumulative"]},
        {"is_monotonic", "Bool", [true]},
        {"flags", "UInt32", [0]},
        {"value", "Float64", [0.0]},
        {"count", "UInt64", [10]},
        {"sum", "Float64", [150.5]},
        {"bucket_counts", "Array(UInt64)", [[1, 3, 4, 2]]},
        {"explicit_bounds", "Array(Float64)", [[10.0, 50.0, 100.0]]},
        {"min", "Float64", [5.0]},
        {"max", "Float64", [95.0]},
        {"scale", "Int32", [0]},
        {"zero_count", "UInt64", [0]},
        {"positive_offset", "Int32", [0]},
        {"positive_bucket_counts", "Array(UInt64)", [[]]},
        {"negative_offset", "Int32", [0]},
        {"negative_bucket_counts", "Array(UInt64)", [[]]},
        {"quantile_values", "Array(Float64)", [[]]},
        {"quantiles", "Array(Float64)", [[]]},
        {"exemplars.filtered_attributes", "Array(JSON(max_dynamic_paths=0, max_dynamic_types=1))",
         [[%{"status" => "ok"}]]},
        {"exemplars.time_unix", "Array(DateTime64(9))", [[exemplar_ts]]},
        {"exemplars.value", "Array(Float64)", [[42.5]]},
        {"exemplars.span_id", "Array(String)", [["span123"]]},
        {"exemplars.trace_id", "Array(String)", [["trace456"]]},
        {"mapping_config_id", "UUID", [mapping_id_raw]},
        {"timestamp", "DateTime64(9)", [ts_ns]}
      ]

      assert {:ok, conn} = NativeIngester.insert(conn, "native_otel_metrics", columns)

      {:ok, rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT metric_name, count, sum, bucket_counts, explicit_bounds, is_monotonic FROM native_otel_metrics"
        )

      assert length(rows) == 1
      row = Enum.at(rows, 0)
      assert row["metric_name"] == "http.request.duration"
      assert row["count"] == 10
      assert_in_delta row["sum"], 150.5, 0.001
      assert row["bucket_counts"] == [1, 3, 4, 2]
      assert row["explicit_bounds"] == [10.0, 50.0, 100.0]
      assert row["is_monotonic"] == true

      # Verify Nullable DateTime64
      {:ok, ts_rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT toUnixTimestamp64Nano(time_unix) as t, start_time_unix FROM native_otel_metrics"
        )

      ts_row = Enum.at(ts_rows, 0)
      assert ts_row["t"] == ts_ns
      assert ts_row["start_time_unix"] == nil

      assert :ok = Connection.close(conn)
    end
  end

  describe "OTEL traces table insert" do
    test "inserts all column types used in the traces DDL", %{backend: backend} do
      ddl = QueryTemplates.create_table_statement("native_otel_traces", :trace, ttl_days: 0)
      {:ok, _} = ClickHouseAdaptor.execute_ch_query(backend, ddl)

      {:ok, conn} = Connection.connect(@connect_opts)

      {:ok, id_raw} = Ecto.UUID.dump(Ecto.UUID.generate())
      {:ok, mapping_id_raw} = Ecto.UUID.dump(Ecto.UUID.generate())
      ts_ns = 1_700_000_000_123_456_789
      event_ts1 = 1_700_000_000_200_000_000
      event_ts2 = 1_700_000_000_300_000_000

      columns = [
        {"id", "UUID", [id_raw]},
        {"source_uuid", "LowCardinality(String)", ["src-uuid-1"]},
        {"source_name", "LowCardinality(String)", ["my_source"]},
        {"project", "String", ["my_project"]},
        {"trace_id", "String", ["trace-abc"]},
        {"span_id", "String", ["span-def"]},
        {"parent_span_id", "String", ["span-parent"]},
        {"trace_state", "String", [""]},
        {"span_name", "LowCardinality(String)", ["GET /api/users"]},
        {"span_kind", "LowCardinality(String)", ["SERVER"]},
        {"service_name", "LowCardinality(String)", ["my_service"]},
        {"event_message", "String", [""]},
        {"duration", "UInt64", [1_500_000]},
        {"status_code", "LowCardinality(String)", ["OK"]},
        {"status_message", "String", [""]},
        {"scope_name", "String", ["otel_scope"]},
        {"scope_version", "String", ["1.0.0"]},
        {"resource_attributes", "JSON(max_dynamic_types=1, max_dynamic_paths=0)",
         [%{"host" => "localhost"}]},
        {"span_attributes", "JSON(max_dynamic_types=1, max_dynamic_paths=0)",
         [%{"http.method" => "GET", "http.url" => "/api/users"}]},
        {"events.timestamp", "Array(DateTime64(9))", [[event_ts1, event_ts2]]},
        {"events.name", "Array(LowCardinality(String))", [["exception", "retry"]]},
        {"events.attributes", "Array(JSON(max_dynamic_paths=0, max_dynamic_types=1))",
         [[%{"error" => "timeout"}, %{"attempt" => 2}]]},
        {"links.trace_id", "Array(String)", [["linked-trace-1"]]},
        {"links.span_id", "Array(String)", [["linked-span-1"]]},
        {"links.trace_state", "Array(String)", [[""]]},
        {"links.attributes", "Array(JSON(max_dynamic_paths=0, max_dynamic_types=1))",
         [[%{"link_key" => "link_value"}]]},
        {"mapping_config_id", "UUID", [mapping_id_raw]},
        {"timestamp", "DateTime64(9)", [ts_ns]}
      ]

      assert {:ok, conn} = NativeIngester.insert(conn, "native_otel_traces", columns)

      {:ok, rows} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          """
          SELECT span_name, duration, status_code, trace_id,
                 `events.name`, `links.trace_id`
          FROM native_otel_traces
          """
        )

      assert length(rows) == 1
      row = Enum.at(rows, 0)
      assert row["span_name"] == "GET /api/users"
      assert row["duration"] == 1_500_000
      assert row["status_code"] == "OK"
      assert row["trace_id"] == "trace-abc"
      assert row["events.name"] == ["exception", "retry"]
      assert row["links.trace_id"] == ["linked-trace-1"]

      assert :ok = Connection.close(conn)
    end
  end
end
