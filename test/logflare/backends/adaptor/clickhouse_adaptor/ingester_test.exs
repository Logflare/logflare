defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.IngesterTest do
  use Logflare.DataCase, async: false

  import Mimic

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Ingester

  setup :verify_on_exit!

  describe "encode_row/2 for logs" do
    test "encodes as iodata" do
      log_event = build(:log_event, message: "test message")

      encoded = Ingester.encode_row(log_event, :log)
      assert is_list(encoded)
      assert IO.iodata_length(encoded) > 0
    end

    test "encodes source_uuid as string (not UUID binary)" do
      source = build(:source)
      log_event = build(:log_event, source: source, message: "test")

      encoded = Ingester.encode_row(log_event, :log)
      binary = IO.iodata_to_binary(encoded)

      source_uuid_str = Atom.to_string(log_event.origin_source_uuid)

      # id is first 16 bytes (UUID binary), then source_uuid as varint-prefixed string
      <<_id::binary-size(16), rest::binary>> = binary

      source_uuid_len = byte_size(source_uuid_str)
      <<^source_uuid_len, encoded_uuid::binary-size(source_uuid_len), _rest::binary>> = rest
      assert encoded_uuid == source_uuid_str
    end

    test "includes event_message as body column" do
      log_event = build(:log_event, message: "hello world")

      encoded = Ingester.encode_row(log_event, :log)
      binary = IO.iodata_to_binary(encoded)

      assert binary =~ "hello world"
    end

    test "includes full body as log_attributes JSON" do
      log_event = build(:log_event, message: "test msg")

      encoded = Ingester.encode_row(log_event, :log)
      binary = IO.iodata_to_binary(encoded)

      assert binary =~ "event_message"
      assert binary =~ "test msg"
    end
  end

  describe "encode_row/2 for metrics" do
    test "encodes as iodata" do
      log_event = build(:log_event, message: "test metric") |> Map.put(:log_type, :metric)

      encoded = Ingester.encode_row(log_event, :metric)
      assert is_list(encoded)
      assert IO.iodata_length(encoded) > 0
    end

    test "includes full body as attributes JSON" do
      log_event = build(:log_event, message: "metric data") |> Map.put(:log_type, :metric)

      encoded = Ingester.encode_row(log_event, :metric)
      binary = IO.iodata_to_binary(encoded)

      assert binary =~ "event_message"
    end
  end

  describe "encode_row/2 for traces" do
    test "encodes as iodata" do
      log_event = build(:log_event, message: "test trace") |> Map.put(:log_type, :trace)

      encoded = Ingester.encode_row(log_event, :trace)
      assert is_list(encoded)
      assert IO.iodata_length(encoded) > 0
    end

    test "includes full body as span_attributes JSON" do
      log_event = build(:log_event, message: "trace data") |> Map.put(:log_type, :trace)

      encoded = Ingester.encode_row(log_event, :trace)
      binary = IO.iodata_to_binary(encoded)

      assert binary =~ "event_message"
    end
  end

  describe "encode_batch/2 for logs" do
    test "encodes multiple LogEvents" do
      log_events = [
        build(:log_event, message: "first"),
        build(:log_event, message: "second")
      ]

      batch = Ingester.encode_batch(log_events, :log)
      assert is_list(batch)

      encoded_row1 = Ingester.encode_row(Enum.at(log_events, 0), :log)
      encoded_row2 = Ingester.encode_row(Enum.at(log_events, 1), :log)

      assert IO.iodata_length(batch) ==
               IO.iodata_length(encoded_row1) + IO.iodata_length(encoded_row2)
    end

    test "handles single LogEvent" do
      log_events = [build(:log_event, message: "only")]

      batch = Ingester.encode_batch(log_events, :log)
      single = Ingester.encode_row(Enum.at(log_events, 0), :log)
      assert batch == [single]
    end
  end

  describe "encode_batch/2 for metrics" do
    test "encodes multiple LogEvents" do
      log_events = [
        build(:log_event, message: "m1") |> Map.put(:log_type, :metric),
        build(:log_event, message: "m2") |> Map.put(:log_type, :metric)
      ]

      batch = Ingester.encode_batch(log_events, :metric)
      assert is_list(batch)
      assert length(batch) == 2
    end
  end

  describe "encode_batch/2 for traces" do
    test "encodes single LogEvent" do
      log_events = [
        build(:log_event, message: "t1") |> Map.put(:log_type, :trace)
      ]

      batch = Ingester.encode_batch(log_events, :trace)
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
      assert "event_message" in columns
      assert "log_attributes" in columns
      assert "timestamp" in columns
    end

    test "returns metric columns" do
      columns = Ingester.columns_for_type(:metric)
      assert "id" in columns
      assert "source_uuid" in columns
      assert "source_name" in columns
      assert "event_message" in columns
      assert "time_unix" in columns
      assert "start_time_unix" in columns
      assert "metric_type" in columns
      assert "attributes" in columns
      assert "timestamp" in columns
    end

    test "returns trace columns" do
      columns = Ingester.columns_for_type(:trace)
      assert "id" in columns
      assert "source_uuid" in columns
      assert "source_name" in columns
      assert "event_message" in columns
      assert "span_attributes" in columns
      assert "timestamp" in columns
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
      log_event = build(:log_event, source: source, message: "Test compression default")

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
      log_event = build(:log_event, source: source, message: "Test")

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
      log_event = build(:log_event, source: source, message: "Test")

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
      log_event = build(:log_event, source: source, message: "Test async")

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
