defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeEncoderTest do
  use Logflare.DataCase, async: true
  use ExUnitProperties

  import Logflare.Factory

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Ingester
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingDefaults
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeEncoder
  alias Logflare.LogEvent
  alias Logflare.Mapper
  alias Logflare.Mapper.MappingConfig
  alias Logflare.Mapper.MappingConfig.FieldConfig, as: Field
  alias Logflare.Mapper.Native

  test "fused log output is byte-identical and applies explicit severity numbers" do
    event =
      raw_event(:log, %{
        "event_message" => "request failed",
        "project" => "project-ref",
        "trace_id" => "trace-id",
        "span_id" => "span-id",
        "severity_text" => "info",
        "severity_number" => 17,
        "resource" => %{
          "service" => %{"name" => "api"},
          "host" => %{"name" => "node-1"}
        },
        "scope" => %{"name" => "plug", "attributes" => %{"version" => "1"}},
        "metadata" => %{"region" => "us-east-1", "request_id" => "req-1"},
        "timestamp" => 1_700_000_000_000_001
      })

    assert_fused_matches_separate(event, :log)
  end

  test "fused metric output is byte-identical for arrays, maps, and nullable times" do
    event =
      raw_event(:metric, %{
        "project" => "project-ref",
        "metric_name" => "http.server.duration",
        "metric_description" => "request duration",
        "metric_unit" => "ms",
        "metric_type" => "histogram",
        "resource" => %{"service" => %{"name" => "api"}},
        "scope" => %{"name" => "otel"},
        "count" => 3,
        "sum" => 42.5,
        "min" => 1.5,
        "max" => 30.0,
        "bucket_counts" => [1, 2],
        "explicit_bounds" => [10.0, 50.0],
        "exemplars" => [
          %{
            "filtered_attributes" => %{"region" => "iad"},
            "time_unix_nano" => 1_700_000_000_000_000_001,
            "value" => 12.5,
            "span_id" => "span-id",
            "trace_id" => "trace-id"
          }
        ],
        "timestamp" => 1_700_000_000_000_002
      })

    assert_fused_matches_separate(event, :metric)
  end

  test "fused trace output is byte-identical and computes duration from timestamps" do
    event =
      raw_event(:trace, %{
        "project" => "project-ref",
        "trace_id" => "trace-id",
        "span_id" => "span-id",
        "parent_span_id" => "parent-id",
        "span_name" => "GET /items",
        "span_kind" => "server",
        "start_time" => 1_700_000_000_000_000_000,
        "end_time" => 1_700_000_000_000_000_250,
        "duration" => 0,
        "status" => %{"code" => "OK", "message" => "done"},
        "events" => [
          %{
            "time_unix_nano" => 1_700_000_000_000_000_100,
            "name" => "cache.hit",
            "attributes" => %{"cache" => "users"}
          }
        ],
        "links" => [
          %{
            "trace_id" => "linked-trace",
            "span_id" => "linked-span",
            "trace_state" => "vendor=value",
            "attributes" => %{"kind" => "parent"}
          }
        ],
        "timestamp" => 1_700_000_000_000_003
      })

    assert_fused_matches_separate(event, :trace)
  end

  test "fused output preserves nullable ingested_at and the generic mapper interface" do
    event =
      :log
      |> raw_event(%{"event_message" => "message", "timestamp" => 1_700_000_000_000_004})
      |> Map.put(:ingested_at, nil)

    compiled = Mapper.compile!(MappingDefaults.for_log())
    assert is_map(Mapper.map(event.body, compiled))
    assert_fused_matches_separate(event, :log, compiled)
  end

  test "independently fused rows concatenate to byte-identical output for every event type" do
    for event_type <- [:log, :metric, :trace] do
      events =
        Enum.map(1..9, fn index ->
          raw_event(event_type, %{
            "event_message" => "event-#{index}",
            "timestamp" => 1_700_000_000_000_000 + index
          })
        end)

      compiled = Mapper.compile!(MappingDefaults.for_type(event_type))
      config_id = encoded_config_id(event_type)

      expected =
        events
        |> Enum.map(&separate_row(&1, event_type, compiled, config_id))
        |> IO.iodata_to_binary()

      actual =
        events
        |> Enum.map(&NativeEncoder.map_and_encode_row(&1, compiled, event_type, config_id))
        |> IO.iodata_to_binary()

      assert actual == expected
    end
  end

  property "fused log rows match the separate encoder for varied scalar and map values" do
    compiled = Mapper.compile!(MappingDefaults.for_log())
    config_id = encoded_config_id(:log)

    check all(
            message <- string(:printable, max_length: 160),
            trace_flags <- integer(0..255),
            severity_number <- integer(0..255),
            severity_text <- member_of(~w(trace debug info notice warn error fatal)),
            metadata <-
              map_of(
                string(:alphanumeric, min_length: 1, max_length: 12),
                string(:printable, max_length: 24),
                max_length: 12
              ),
            timestamp <- integer(1_600_000_000_000_000..1_800_000_000_000_000),
            max_runs: 75
          ) do
      event =
        raw_event(:log, %{
          "event_message" => message,
          "trace_flags" => trace_flags,
          "severity_text" => severity_text,
          "severity_number" => severity_number,
          "metadata" => metadata,
          "timestamp" => timestamp
        })

      assert_fused_matches_separate(event, :log, compiled, config_id)
    end
  end

  test "full-row parity covers UUID, varuint, numeric, nullable, and array boundaries" do
    long_attributes = Map.new(1..130, &{"attribute-#{&1}", "value-#{&1}"})

    log_event =
      raw_event(:log, %{
        "event_message" => String.duplicate("é", 130),
        "trace_flags" => 255,
        "severity_number" => 255,
        "metadata" => long_attributes,
        "timestamp" => 1_700_000_000_000_201
      })
      |> struct(
        id: "00112233445566778899AABBCCDDEEFF",
        source_name: "source-世界",
        ingested_at: DateTime.from_unix!(-500_000, :microsecond)
      )

    metric_event =
      raw_event(:metric, %{
        "project" => "metric-boundaries",
        "time_unix_nano" => -1_700_000_000_000_000_000,
        "start_time_unix_nano" => 1_700_000_000_000_000_000,
        "metric_type" => "summary",
        "flags" => 4_294_967_295,
        "value" => -123.5,
        "count" => "18446744073709551615",
        "sum" => -42.25,
        "min" => -99.5,
        "max" => 99.5,
        "scale" => -2_147_483_648,
        "zero_count" => "18446744073709551615",
        "positive_offset" => 2_147_483_647,
        "negative_offset" => -2_147_483_648,
        "bucket_counts" => [0, "18446744073709551615"],
        "explicit_bounds" => Enum.map(1..130, &(&1 / 10)),
        "positive_bucket_counts" => [1, 2, 3],
        "negative_bucket_counts" => [4, 5, 6],
        "quantile_values" => [-1.5, 2.5],
        "quantiles" => [0.25, 0.75],
        "timestamp" => 1_700_000_000_000_202
      })
      |> struct(id: "00112233-4455-6677-8899-AABBCCDDEEFF", ingested_at: nil)

    trace_event =
      raw_event(:trace, %{
        "trace_id" => "trace-boundary",
        "span_id" => "span-boundary",
        "duration" => "18446744073709551615",
        "start_time" => -1_700_000_000_000_000_000,
        "end_time" => 1_700_000_000_000_000_000,
        "events" => [],
        "links" => [],
        "timestamp" => 1_700_000_000_000_203
      })
      |> struct(id: "ffeeddccbbaa99887766554433221100")

    for {event, event_type} <- [
          {log_event, :log},
          {metric_event, :metric},
          {trace_event, :trace}
        ] do
      assert_fused_matches_separate(event, event_type)
    end
  end

  test "native input and layout errors are returned without crashing the VM" do
    compiled = Mapper.compile!(MappingDefaults.for_log())
    config_id = encoded_config_id(:log)
    event = raw_event(:log, %{"event_message" => "valid", "timestamp" => 1_700_000_000_000_301})
    document = native_document(event)

    assert {:error, "unsupported ClickHouse event type"} =
             Native.map_and_encode_clickhouse(document, compiled, :unsupported, config_id)

    assert {:error, "document must contain a body and row envelope"} =
             Native.map_and_encode_clickhouse(event.body, compiled, :log, config_id)

    assert {:error, "row envelope must contain ID, source UUID, source name, and ingested_at"} =
             Native.map_and_encode_clickhouse(
               {event.body, :invalid_envelope},
               compiled,
               :log,
               config_id
             )

    incomplete = Mapper.compile!(MappingConfig.new([Field.string("project", path: "$.project")]))

    assert_raise ArgumentError,
                 "failed to encode ClickHouse row: compiled mapping is missing required ClickHouse field 'trace_id'",
                 fn ->
                   NativeEncoder.map_and_encode_row(event, incomplete, :log, config_id)
                 end
  end

  test "row errors return an actionable reason" do
    compiled = Mapper.compile!(MappingDefaults.for_log())
    config_id = encoded_config_id(:log)
    valid = raw_event(:log, %{"event_message" => "valid", "timestamp" => 1_700_000_000_000_401})

    assert_raise ArgumentError, ~r/mapping config ID must be a 16-byte encoded UUID/, fn ->
      NativeEncoder.map_and_encode_row(valid, compiled, :log, <<0::120>>)
    end

    invalid_uuid = %{valid | id: "not-a-uuid"}

    assert_raise ArgumentError, ~r/invalid event UUID/, fn ->
      NativeEncoder.map_and_encode_row(invalid_uuid, compiled, :log, config_id)
    end

    malformed_canonical_uuid = %{valid | id: "0011223--4455-6677-8899-aabbccddeeff"}

    assert_raise ArgumentError, ~r/invalid event UUID/, fn ->
      NativeEncoder.map_and_encode_row(malformed_canonical_uuid, compiled, :log, config_id)
    end

    missing_timestamp = %{valid | body: %{"event_message" => "missing timestamp"}}

    assert_raise ArgumentError, ~r/mapped signed field is not an integer/, fn ->
      NativeEncoder.map_and_encode_row(missing_timestamp, compiled, :log, config_id)
    end
  end

  test "mapping config IDs are contiguous 16-byte binaries" do
    for event_type <- [:log, :metric, :trace] do
      config_id = encoded_config_id(event_type)
      assert is_binary(config_id)
      assert byte_size(config_id) == 16
    end
  end

  defp raw_event(event_type, body) do
    %LogEvent{} = event = build(:log_event)
    %LogEvent{event | body: body, event_type: event_type}
  end

  defp assert_fused_matches_separate(
         event,
         event_type,
         compiled \\ nil,
         config_id \\ nil
       ) do
    compiled = compiled || Mapper.compile!(MappingDefaults.for_type(event_type))
    config_id = config_id || encoded_config_id(event_type)
    expected = separate_row(event, event_type, compiled, config_id) |> IO.iodata_to_binary()

    assert NativeEncoder.map_and_encode_row(event, compiled, event_type, config_id) == expected
  end

  defp native_document(event) do
    ingested_at = if event.ingested_at, do: DateTime.to_unix(event.ingested_at, :microsecond)

    {event.body,
     {event.id, Atom.to_string(event.source_uuid), event.source_name || "", ingested_at}}
  end

  defp separate_row(event, event_type, compiled, config_id) do
    mapped_body =
      event.body
      |> Mapper.map(compiled)
      |> maybe_compute_duration(event_type)
      |> resolve_severity_number(event_type)

    Ingester.encode_row(%{event | body: mapped_body}, event_type, config_id)
  end

  defp encoded_config_id(event_type) do
    event_type |> MappingDefaults.config_id() |> Ingester.encode_mapping_config_id()
  end

  defp maybe_compute_duration(
         %{"start_time" => start_time, "end_time" => end_time, "duration" => 0} = body,
         :trace
       )
       when is_integer(start_time) and is_integer(end_time) and end_time > start_time do
    %{body | "duration" => end_time - start_time}
  end

  defp maybe_compute_duration(body, _event_type), do: body

  defp resolve_severity_number(%{"severity_number_alt" => alt} = body, :log)
       when is_integer(alt) and alt > 0 do
    %{body | "severity_number" => alt}
  end

  defp resolve_severity_number(body, _event_type), do: body
end
