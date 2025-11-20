defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.IngesterTest do
  use Logflare.DataCase, async: false

  import Mimic

  alias Logflare.Backends.Adaptor.ClickhouseAdaptor
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor.Ingester

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
               <<0x55, 0x0E, 0x84, 0x00, 0xE2, 0x9B, 0x41, 0xD4, 0xA7, 0x16, 0x44, 0x66, 0x55,
                 0x44, 0x00, 0x00>>
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

  describe "encode_row/1" do
    test "encodes a LogEvent as iodata" do
      log_event = build(:log_event, message: "test message")

      encoded = Ingester.encode_row(log_event)
      assert is_list(encoded)
      # UUID (16) + varint length (1+) + body (JSON) + timestamp (8)
      assert IO.iodata_length(encoded) >= 16 + 1 + 10 + 8
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

      {:ok, _supervisor_pid} = ClickhouseAdaptor.start_link({source, backend})

      table_name = ClickhouseAdaptor.clickhouse_ingest_table_name(source)

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
      |> expect(:request, fn request, _pool ->
        headers = request.headers

        assert {"content-encoding", "gzip"} in headers,
               "Expected gzip content encoding in headers"

        <<first_byte, second_byte, _rest::binary>> = IO.iodata_to_binary(request.body)
        assert first_byte == 0x1F && second_byte == 0x8B, "Expected gzip compression in body"

        {:ok, %Finch.Response{status: 200, body: ""}}
      end)

      assert :ok = Ingester.insert(backend, table_name, [log_event])
    end

    test "sends `async_insert=1` and `wait_for_async_insert=1` in URL", %{
      backend: backend,
      table_name: table_name,
      source: source
    } do
      log_event = build(:log_event, source: source, message: "Test")

      Finch
      |> expect(:request, fn request, _pool ->
        # Extract URL from the request
        url =
          to_string(request.scheme) <>
            "://" <>
            request.host <> ":" <> to_string(request.port) <> request.path <> "?" <> request.query

        assert url =~ ~r/[?&]async_insert=1/,
               "Expected URL to contain `async_insert=1`, got: #{url}"

        assert url =~ "wait_for_async_insert=1",
               "Expected URL to contain `wait_for_async_insert=1`, got: #{url}"

        {:ok, %Finch.Response{status: 200, body: ""}}
      end)

      assert :ok = Ingester.insert(backend, table_name, [log_event])
    end
  end
end
