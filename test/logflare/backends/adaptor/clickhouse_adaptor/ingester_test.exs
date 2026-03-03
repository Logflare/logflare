defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.IngesterTest do
  use Logflare.DataCase, async: false

  import Logflare.ClickHouseMappedEvents
  import Mimic

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Ingester
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingConfigStore

  setup_all do
    case MappingConfigStore.start_link([]) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    :ok
  end

  setup :verify_on_exit!

  describe "encode_row/2 for logs" do
    test "encodes as iodata" do
      event = build_mapped_log_event(message: "test message")

      encoded = Ingester.encode_row(event, :log)
      assert is_list(encoded)
      assert IO.iodata_length(encoded) > 0
    end

    test "encodes source_uuid as string (not UUID binary)" do
      source = build(:source)
      event = build_mapped_log_event(source: source, message: "test")

      encoded = Ingester.encode_row(event, :log)
      binary = IO.iodata_to_binary(encoded)

      source_uuid_str = Atom.to_string(event.source_uuid)

      <<_id::binary-size(16), rest::binary>> = binary

      source_uuid_len = byte_size(source_uuid_str)
      <<^source_uuid_len, encoded_uuid::binary-size(source_uuid_len), _rest::binary>> = rest
      assert encoded_uuid == source_uuid_str
    end

    test "includes event_message in encoded output" do
      event = build_mapped_log_event(message: "hello world")

      encoded = Ingester.encode_row(event, :log)
      binary = IO.iodata_to_binary(encoded)

      assert binary =~ "hello world"
    end

    test "includes mapped scalar fields from realistic input" do
      event =
        build_mapped_log_event(
          message: "test msg",
          body: %{
            "trace_id" => "trace-123",
            "metadata" => %{"level" => "error"},
            "project" => "my-project"
          }
        )

      encoded = Ingester.encode_row(event, :log)
      binary = IO.iodata_to_binary(encoded)

      assert binary =~ "trace-123"
      assert binary =~ "ERROR"
      assert binary =~ "my-project"
    end

    test "encodes log_attributes from mapped input" do
      event =
        build_mapped_log_event(
          message: "test msg",
          body: %{"custom_field" => "custom_value"}
        )

      encoded = Ingester.encode_row(event, :log)
      binary = IO.iodata_to_binary(encoded)

      assert binary =~ "custom_field"
      assert binary =~ "custom_value"
    end
  end

  describe "encode_row/2 for metrics" do
    test "encodes as iodata" do
      event = build_mapped_metric_event()

      encoded = Ingester.encode_row(event, :metric)
      assert is_list(encoded)
      assert IO.iodata_length(encoded) > 0
    end

    test "includes metric fields in encoded output" do
      event =
        build_mapped_metric_event(body: %{"metric_name" => "http_requests"})

      encoded = Ingester.encode_row(event, :metric)
      binary = IO.iodata_to_binary(encoded)

      assert binary =~ "http_requests"
    end

    test "includes attributes as JSON" do
      event = build_mapped_metric_event(body: %{"host" => "web-1"})

      encoded = Ingester.encode_row(event, :metric)
      binary = IO.iodata_to_binary(encoded)

      assert binary =~ "host"
    end
  end

  describe "encode_row/2 for traces" do
    test "encodes as iodata" do
      event = build_mapped_trace_event()

      encoded = Ingester.encode_row(event, :trace)
      assert is_list(encoded)
      assert IO.iodata_length(encoded) > 0
    end

    test "includes trace fields in encoded output" do
      event =
        build_mapped_trace_event(
          body: %{
            "trace_id" => "t-abc",
            "span_id" => "s-def",
            "span_name" => "GET /users"
          }
        )

      encoded = Ingester.encode_row(event, :trace)
      binary = IO.iodata_to_binary(encoded)

      assert binary =~ "t-abc"
      assert binary =~ "s-def"
      assert binary =~ "GET /users"
    end

    test "includes span_attributes as JSON" do
      event = build_mapped_trace_event(body: %{"http.status_code" => "200"})

      encoded = Ingester.encode_row(event, :trace)
      binary = IO.iodata_to_binary(encoded)

      assert binary =~ "http.status_code"
    end
  end

  describe "encode_batch/2 for logs" do
    test "encodes multiple LogEvents" do
      events = [
        build_mapped_log_event(message: "first"),
        build_mapped_log_event(message: "second")
      ]

      batch = Ingester.encode_batch(events, :log)
      assert is_list(batch)

      encoded_row1 = Ingester.encode_row(Enum.at(events, 0), :log)
      encoded_row2 = Ingester.encode_row(Enum.at(events, 1), :log)

      assert IO.iodata_length(batch) ==
               IO.iodata_length(encoded_row1) + IO.iodata_length(encoded_row2)
    end

    test "handles single LogEvent" do
      events = [build_mapped_log_event(message: "only")]

      batch = Ingester.encode_batch(events, :log)
      single = Ingester.encode_row(Enum.at(events, 0), :log)
      assert batch == [single]
    end
  end

  describe "encode_batch/2 for metrics" do
    test "encodes multiple LogEvents" do
      events = [
        build_mapped_metric_event(message: "m1"),
        build_mapped_metric_event(message: "m2")
      ]

      batch = Ingester.encode_batch(events, :metric)
      assert is_list(batch)
      assert length(batch) == 2
    end
  end

  describe "encode_batch/2 for traces" do
    test "encodes single LogEvent" do
      events = [build_mapped_trace_event(message: "t1")]

      batch = Ingester.encode_batch(events, :trace)
      assert is_list(batch)
      assert length(batch) == 1
    end
  end

  describe "columns_for_type/1" do
    test "returns log columns" do
      columns = Ingester.columns_for_type(:log)
      assert "id" in columns
      assert "source_uuid" in columns
      assert "source_name" in columns
      assert "project" in columns
      assert "trace_id" in columns
      assert "span_id" in columns
      assert "trace_flags" in columns
      assert "severity_text" in columns
      assert "severity_number" in columns
      assert "service_name" in columns
      assert "event_message" in columns
      assert "scope_name" in columns
      assert "scope_version" in columns
      assert "scope_schema_url" in columns
      assert "resource_schema_url" in columns
      assert "resource_attributes" in columns
      assert "scope_attributes" in columns
      assert "log_attributes" in columns
      assert "timestamp" in columns
    end

    test "returns metric columns" do
      columns = Ingester.columns_for_type(:metric)
      assert "id" in columns
      assert "source_uuid" in columns
      assert "source_name" in columns
      assert "project" in columns
      assert "time_unix" in columns
      assert "start_time_unix" in columns
      assert "metric_name" in columns
      assert "metric_description" in columns
      assert "metric_unit" in columns
      assert "metric_type" in columns
      assert "service_name" in columns
      assert "event_message" in columns
      assert "scope_name" in columns
      assert "scope_version" in columns
      assert "resource_attributes" in columns
      assert "scope_attributes" in columns
      assert "attributes" in columns
      assert "aggregation_temporality" in columns
      assert "is_monotonic" in columns
      assert "flags" in columns
      assert "value" in columns
      assert "count" in columns
      assert "sum" in columns
      assert "min" in columns
      assert "max" in columns
      assert "scale" in columns
      assert "zero_count" in columns
      assert "positive_offset" in columns
      assert "negative_offset" in columns
      assert "timestamp" in columns
    end

    test "returns trace columns" do
      columns = Ingester.columns_for_type(:trace)
      assert "id" in columns
      assert "source_uuid" in columns
      assert "source_name" in columns
      assert "project" in columns
      assert "timestamp" in columns
      assert "trace_id" in columns
      assert "span_id" in columns
      assert "parent_span_id" in columns
      assert "trace_state" in columns
      assert "span_name" in columns
      assert "span_kind" in columns
      assert "service_name" in columns
      assert "event_message" in columns
      assert "duration" in columns
      assert "status_code" in columns
      assert "status_message" in columns
      assert "scope_name" in columns
      assert "scope_version" in columns
      assert "resource_attributes" in columns
      assert "span_attributes" in columns
    end
  end

  describe "encode_simple_row/2 for logs" do
    test "encodes as iodata" do
      event = build_mapped_log_event(message: "simple test", mapping_variant: :simple)

      encoded = Ingester.encode_simple_row(event, :log)
      assert is_list(encoded)
      assert IO.iodata_length(encoded) > 0
    end

    test "includes event_message in encoded output" do
      event = build_mapped_log_event(message: "hello simple", mapping_variant: :simple)

      encoded = Ingester.encode_simple_row(event, :log)
      binary = IO.iodata_to_binary(encoded)

      assert binary =~ "hello simple"
    end
  end

  describe "encode_simple_row/2 for metrics" do
    test "encodes as iodata" do
      event = build_mapped_metric_event(mapping_variant: :simple)

      encoded = Ingester.encode_simple_row(event, :metric)
      assert is_list(encoded)
      assert IO.iodata_length(encoded) > 0
    end

    test "includes metric fields in encoded output" do
      event =
        build_mapped_metric_event(
          body: %{"metric_name" => "simple_requests"},
          mapping_variant: :simple
        )

      encoded = Ingester.encode_simple_row(event, :metric)
      binary = IO.iodata_to_binary(encoded)

      assert binary =~ "simple_requests"
    end
  end

  describe "encode_simple_row/2 for traces" do
    test "encodes as iodata" do
      event = build_mapped_trace_event(mapping_variant: :simple)

      encoded = Ingester.encode_simple_row(event, :trace)
      assert is_list(encoded)
      assert IO.iodata_length(encoded) > 0
    end

    test "includes trace fields in encoded output" do
      event =
        build_mapped_trace_event(
          body: %{"span_name" => "GET /simple"},
          mapping_variant: :simple
        )

      encoded = Ingester.encode_simple_row(event, :trace)
      binary = IO.iodata_to_binary(encoded)

      assert binary =~ "GET /simple"
    end
  end

  describe "insert/4" do
    setup do
      insert(:plan, name: "Free")
      {source, backend, cleanup_fn} = setup_clickhouse_test()
      on_exit(cleanup_fn)

      {:ok, _supervisor_pid} = ClickHouseAdaptor.start_link(backend)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :log)

      Process.sleep(200)

      [source: source, backend: backend, table_name: table_name]
    end

    test "sends gzip-compressed body and content encoding in headers", %{
      backend: backend,
      table_name: table_name,
      source: source
    } do
      log_event = build_mapped_log_event(source: source, message: "Test compression default")

      Finch
      |> expect(:request, fn request, _pool, _opts ->
        headers = request.headers

        assert {"content-encoding", "gzip"} in headers,
               "Expected gzip content encoding in headers"

        <<first_byte, second_byte, _rest::binary>> = IO.iodata_to_binary(request.body)
        assert first_byte == 0x1F && second_byte == 0x8B, "Expected gzip compression in body"

        {:ok, %Finch.Response{status: 200, body: ""}}
      end)

      assert :ok = Ingester.insert(backend, table_name, [log_event], :log)
    end

    test "includes column list in INSERT query", %{
      backend: backend,
      table_name: table_name,
      source: source
    } do
      log_event = build_mapped_log_event(source: source, message: "Test")

      Finch
      |> expect(:request, fn request, _pool, _opts ->
        url =
          to_string(request.scheme) <>
            "://" <>
            request.host <> ":" <> to_string(request.port) <> request.path <> "?" <> request.query

        assert url =~ "id",
               "Expected URL to contain column list, got: #{url}"

        assert url =~ "source_uuid",
               "Expected URL to contain source_uuid column, got: #{url}"

        {:ok, %Finch.Response{status: 200, body: ""}}
      end)

      assert :ok = Ingester.insert(backend, table_name, [log_event], :log)
    end

    test "uses synchronous inserts (no async parameters in URL)", %{
      backend: backend,
      table_name: table_name,
      source: source
    } do
      log_event = build_mapped_log_event(source: source, message: "Test")

      Finch
      |> expect(:request, fn request, _pool, _opts ->
        url =
          to_string(request.scheme) <>
            "://" <>
            request.host <> ":" <> to_string(request.port) <> request.path <> "?" <> request.query

        assert url =~ "query=INSERT",
               "Expected URL to contain INSERT query, got: #{url}"

        refute url =~ "async_insert",
               "Expected URL to NOT contain async_insert parameter, got: #{url}"

        {:ok, %Finch.Response{status: 200, body: ""}}
      end)

      assert :ok = Ingester.insert(backend, table_name, [log_event], :log)
    end

    test "uses async inserts with wait flag when async_insert config is true", %{
      backend: backend,
      table_name: table_name,
      source: source
    } do
      backend_with_async = %{backend | config: Map.put(backend.config, :async_insert, true)}
      log_event = build_mapped_log_event(source: source, message: "Test async")

      Finch
      |> expect(:request, fn request, _pool, _opts ->
        url =
          to_string(request.scheme) <>
            "://" <>
            request.host <> ":" <> to_string(request.port) <> request.path <> "?" <> request.query

        assert url =~ "query=INSERT",
               "Expected URL to contain INSERT query, got: #{url}"

        assert url =~ "async_insert=1",
               "Expected URL to contain async_insert=1 parameter, got: #{url}"

        assert url =~ "wait_for_async_insert=1",
               "Expected URL to contain wait_for_async_insert=1 parameter, got: #{url}"

        {:ok, %Finch.Response{status: 200, body: ""}}
      end)

      assert :ok = Ingester.insert(backend_with_async, table_name, [log_event], :log)
    end
  end
end
