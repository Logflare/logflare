defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.IngesterTest do
  use Logflare.DataCase, async: false

  import Bitwise
  import Mimic

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Ingester

  setup :verify_on_exit!

  describe "encode_as_varint/1" do
    test "encodes zero" do
      assert Ingester.encode_as_varint(0) == <<0>>
    end

    test "encodes values less than 128" do
      assert Ingester.encode_as_varint(1) == <<1>>
      assert Ingester.encode_as_varint(127) == <<127>>
    end

    test "encodes 128 (requires 2 bytes)" do
      assert Ingester.encode_as_varint(128) == <<0x80, 0x01>>
    end

    test "encodes 300" do
      assert Ingester.encode_as_varint(300) == <<0xAC, 0x02>>
    end

    test "encodes larger numbers" do
      assert Ingester.encode_as_varint(16_384) == <<0x80, 0x80, 0x01>>
    end
  end

  describe "encode_as_uuid/1" do
    test "encodes UUID string to 16 bytes" do
      uuid = "550e8400-e29b-41d4-a716-446655440000"
      encoded = Ingester.encode_as_uuid(uuid)

      assert byte_size(encoded) == 16

      assert encoded ==
               <<0xD4, 0x41, 0x9B, 0xE2, 0x00, 0x84, 0x0E, 0x55, 0x00, 0x00, 0x44, 0x55, 0x66,
                 0x44, 0x16, 0xA7>>
    end

    test "raises an exception for invalid UUIDs" do
      assert_raise RuntimeError,
                   "invalid uuid when trying to encode for ClickHouse: \"6E6F6F626172\"",
                   fn ->
                     Ingester.encode_as_uuid("6E6F6F626172")
                   end
    end

    test "handles uppercase UUIDs" do
      uuid = "550E8400-E29B-41D4-A716-446655440000"
      encoded = Ingester.encode_as_uuid(uuid)

      assert byte_size(encoded) == 16
    end

    test "handles UUIDs without dashes" do
      uuid = "550e8400e29b41d4a716446655440000"
      encoded = Ingester.encode_as_uuid(uuid)

      assert byte_size(encoded) == 16
    end
  end

  describe "encode_as_string/1" do
    test "encodes binary string with varint length prefix" do
      encoded = Ingester.encode_as_string("hello")

      assert is_list(encoded)
      assert IO.iodata_to_binary(encoded) == <<5, "hello">>
    end

    test "encodes simple iodata with varint length prefix" do
      encoded = Ingester.encode_as_string(["hello"])

      assert is_list(encoded)
      assert IO.iodata_to_binary(encoded) == <<5, "hello">>
    end

    test "encodes longer iodata" do
      long_string = String.duplicate("a", 200)
      encoded = Ingester.encode_as_string([long_string])

      assert is_list(encoded)
      binary = IO.iodata_to_binary(encoded)
      <<length_bytes::binary-size(2), content::binary>> = binary
      assert length_bytes == <<0xC8, 0x01>>
      assert content == long_string
    end

    test "encodes iodata containing UTF-8 characters correctly" do
      # "café" is 5 bytes in UTF-8 (é is 2 bytes)
      encoded = Ingester.encode_as_string(["café"])

      assert is_list(encoded)
      assert IO.iodata_to_binary(encoded) == <<5, "café">>
    end

    test "encodes iodata containing emoji without issues" do
      encoded = Ingester.encode_as_string(["🚀"])

      assert is_list(encoded)
      assert IO.iodata_to_binary(encoded) == <<4, "🚀">>
    end

    test "encodes complex iodata without intermediate binary allocation" do
      iodata = ["hello", " ", "world"]
      encoded = Ingester.encode_as_string(iodata)

      assert is_list(encoded)
      assert IO.iodata_length(encoded) == 1 + 11
      # When converted to binary, should be: varint(11) + "hello world"
      assert IO.iodata_to_binary(encoded) == <<11, "hello world">>
    end
  end

  describe "encode_as_datetime64/1" do
    test "encodes DateTime to microseconds since epoch" do
      datetime = ~U[2024-01-01 12:30:45.123456Z]
      encoded = Ingester.encode_as_datetime64(datetime)

      assert byte_size(encoded) == 8

      <<timestamp_int::little-signed-64>> = encoded
      expected = DateTime.to_unix(datetime, :microsecond)
      assert timestamp_int == expected
    end

    test "encodes epoch correctly" do
      epoch = ~U[1970-01-01 00:00:00.000000Z]
      encoded = Ingester.encode_as_datetime64(epoch)

      <<timestamp_int::little-signed-64>> = encoded
      assert timestamp_int == 0
    end

    test "handles microsecond precision" do
      datetime = ~U[2024-01-01 00:00:00.123456Z]
      encoded = Ingester.encode_as_datetime64(datetime)

      <<timestamp_int::little-signed-64>> = encoded
      expected_seconds = DateTime.to_unix(datetime, :second)
      expected = expected_seconds * 1_000_000 + 123_456
      assert timestamp_int == expected
    end
  end

  describe "insert/3 with LogEvent structs and Backend" do
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

    test "encodes row with correct field order",
         %{
           backend: backend,
           table_name: table_name,
           source: source
         } do
      log_event =
        build(:log_event,
          id: "550e8400-e29b-41d4-a716-446655440000",
          source: source,
          message: "Test encoding"
        )

      Finch
      |> expect(:request, fn request, _pool, _opts ->
        body = request.body |> IO.iodata_to_binary() |> :zlib.gunzip()

        # id (16), source_uuid (16), source_name (varint+string), body (varint+string), ingested_at (8), timestamp (8)
        <<id_bytes::binary-size(16), source_uuid_bytes::binary-size(16), rest::binary>> = body

        assert id_bytes == Ingester.encode_as_uuid("550e8400-e29b-41d4-a716-446655440000")

        expected_source_uuid = Atom.to_string(source.token)
        assert source_uuid_bytes == Ingester.encode_as_uuid(expected_source_uuid)

        {source_name, rest} = parse_varint_string(rest)
        assert source_name == source.name

        {body_json, rest} = parse_varint_string(rest)
        body_decoded = Jason.decode!(body_json)
        assert body_decoded["event_message"] == "Test encoding"
        assert body_decoded["id"] == "550e8400-e29b-41d4-a716-446655440000"

        # ingested_at (8), timestamp (8)
        <<_ingested_at::binary-size(8), _timestamp::binary-size(8)>> = rest

        {:ok, %Finch.Response{status: 200, body: ""}}
      end)

      assert :ok = Ingester.insert(backend, table_name, [log_event])
    end

    test "encodes multiple rows correctly with `source_name` from cache", %{
      backend: backend,
      table_name: table_name,
      source: source
    } do
      log_events = [
        build(:log_event,
          id: "550e8400-e29b-41d4-a716-446655440001",
          source: source,
          message: "First message"
        ),
        build(:log_event,
          id: "550e8400-e29b-41d4-a716-446655440002",
          source: source,
          message: "Second message"
        )
      ]

      Finch
      |> expect(:request, fn request, _pool, _opts ->
        body = request.body |> IO.iodata_to_binary() |> :zlib.gunzip()

        {row1, rest} = parse_row(body)
        assert row1.source_name == source.name
        assert row1.body["event_message"] == "First message"

        {row2, _rest} = parse_row(rest)
        assert row2.source_name == source.name
        assert row2.body["event_message"] == "Second message"

        {:ok, %Finch.Response{status: 200, body: ""}}
      end)

      assert :ok = Ingester.insert(backend, table_name, log_events)
    end

    test "encodes empty `source_name` when source not in cache", %{
      backend: backend,
      table_name: table_name
    } do
      uncached_source = build(:source)

      log_event =
        build(:log_event,
          id: "550e8400-e29b-41d4-a716-446655440003",
          source: uncached_source,
          message: "Uncached source test"
        )

      Finch
      |> expect(:request, fn request, _pool, _opts ->
        body = request.body |> IO.iodata_to_binary() |> :zlib.gunzip()

        # skip id (16) + source_uuid (16)
        <<_::binary-size(32), rest::binary>> = body

        {source_name, _rest} = parse_varint_string(rest)
        assert source_name == ""

        {:ok, %Finch.Response{status: 200, body: ""}}
      end)

      assert :ok = Ingester.insert(backend, table_name, [log_event])
    end
  end

  defp parse_varint_string(<<byte, rest::binary>>) when byte < 128 do
    <<string::binary-size(byte), rest::binary>> = rest
    {string, rest}
  end

  defp parse_varint_string(<<byte1, byte2, rest::binary>>) do
    length = (byte1 &&& 0x7F) ||| byte2 <<< 7
    <<string::binary-size(length), rest::binary>> = rest
    {string, rest}
  end

  defp parse_row(binary) do
    # id (16), source_uuid (16), source_name (varint), body (varint), ingested_at (8), timestamp (8)
    <<_id::binary-size(16), _source_uuid::binary-size(16), rest::binary>> = binary

    {source_name, rest} = parse_varint_string(rest)
    {body_json, rest} = parse_varint_string(rest)
    <<_ingested_at::binary-size(8), _timestamp::binary-size(8), rest::binary>> = rest

    row = %{
      source_name: source_name,
      body: Jason.decode!(body_json)
    }

    {row, rest}
  end
end
