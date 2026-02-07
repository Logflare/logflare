defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.IngesterTest do
  use Logflare.DataCase, async: false

  import Mimic

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Ingester
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.RowBinaryEncoder

  setup :verify_on_exit!

  describe "encode_row/1" do
    test "encodes a LogEvent as iodata" do
      log_event = build(:log_event, message: "test message")

      encoded = Ingester.encode_row(log_event)
      assert is_list(encoded)

      # id (16) + source_uuid (16) + source_name (varint+str) + body (varint+json) + ingested_at (8) + timestamp (8)
      assert IO.iodata_length(encoded) >= 16 + 16 + 1 + 1 + 10 + 8 + 8
    end

    test "encodes origin_source_uuid as source_uuid" do
      source = build(:source)
      log_event = build(:log_event, source: source, message: "test")

      encoded = Ingester.encode_row(log_event)
      binary = IO.iodata_to_binary(encoded)

      source_uuid_str = Atom.to_string(log_event.origin_source_uuid)
      expected_source_uuid_bytes = RowBinaryEncoder.uuid(source_uuid_str)

      # source_uuid is the second 16 bytes (after id)
      <<_id::binary-size(16), source_uuid_bytes::binary-size(16), _rest::binary>> = binary
      assert source_uuid_bytes == expected_source_uuid_bytes
    end

    test "encodes origin_source_name after source_uuid" do
      source = build(:source)
      log_event = build(:log_event, source: source, message: "test")

      encoded = Ingester.encode_row(log_event)
      binary = IO.iodata_to_binary(encoded)

      # skip id (16) + source_uuid (16)
      <<_::binary-size(32), rest::binary>> = binary

      source_name = log_event.origin_source_name
      source_name_len = byte_size(source_name)

      <<^source_name_len, encoded_name::binary-size(source_name_len), _rest::binary>> = rest
      assert encoded_name == source_name
    end
  end

  describe "encode_batch/1" do
    test "encodes multiple LogEvents as iodata" do
      log_events = [
        build(:log_event, message: "first"),
        build(:log_event, message: "second")
      ]

      batch = Ingester.encode_batch(log_events)
      assert is_list(batch)

      encoded_row1 = Ingester.encode_row(Enum.at(log_events, 0))
      encoded_row2 = Ingester.encode_row(Enum.at(log_events, 1))

      assert IO.iodata_length(batch) ==
               IO.iodata_length(encoded_row1) + IO.iodata_length(encoded_row2)
    end

    test "handles single LogEvent as iodata" do
      log_events = [build(:log_event, message: "only")]

      batch = Ingester.encode_batch(log_events)
      single = Ingester.encode_row(Enum.at(log_events, 0))
      assert batch == [single]
    end
  end

  describe "insert/4 with LogEvent structs and Backend" do
    setup do
      insert(:plan, name: "Free")
      {source, backend, cleanup_fn} = setup_clickhouse_test()
      on_exit(cleanup_fn)

      {:ok, _supervisor_pid} = ClickHouseAdaptor.start_link(backend)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend)

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

      assert :ok = Ingester.insert(backend, table_name, [log_event])
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

      assert :ok = Ingester.insert(backend, table_name, [log_event])
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

      assert :ok = Ingester.insert(backend_with_async, table_name, [log_event])
    end
  end
end
