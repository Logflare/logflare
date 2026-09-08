defmodule Logflare.Mapper.NdjsonOutputTest do
  use ExUnit.Case, async: true

  import Logflare.Factory

  alias Logflare.ClickHouseMappedEvents
  alias Logflare.LogEvent
  alias Logflare.Mapper
  alias Logflare.Mapper.MappingConfig
  alias Logflare.Mapper.MappingConfig.FieldConfig, as: Field
  alias Logflare.Mapper.MappingConfig.OutputFormat
  alias Logflare.Mapper.OtelDefaults
  alias Logflare.Mapper.OutputContext

  @envelope_keys ~w(id source_uuid source_name mapping_config_id ingested_at)
  @fixtures %{
    log: %{
      "event_message" => "Something happened",
      "project" => "abcdefghijklmnopqrst",
      "trace_id" => "abc123",
      "span_id" => "span-1",
      "severity_number" => 17,
      "metadata" => %{"level" => "info", "region" => "us-east-1", "nested" => %{"k" => "v"}},
      "resource" => %{"service" => %{"name" => "svc"}},
      "timestamp" => 1_700_000_000_000_001
    },
    metric: %{
      "event_message" => "cpu",
      "project" => "abcdefghijklmnopqrst",
      "metric_type" => "gauge",
      "unit" => "1",
      "value" => 0.75,
      "attributes" => %{"host" => "a"},
      "resource" => %{"service" => %{"name" => "svc"}},
      "timestamp" => 1_700_000_000_000_002
    },
    trace: %{
      "event_message" => "GET /",
      "project" => "abcdefghijklmnopqrst",
      "trace_id" => "abc123",
      "span_id" => "span-1",
      "start_time" => 1_700_000_000_000_000_000,
      "end_time" => 1_700_000_000_000_001_500,
      "resource" => %{"service" => %{"name" => "svc"}},
      "timestamp" => 1_700_000_000_000_003
    }
  }

  setup_all do
    # the :map comparison configs take the NDJSON timestamp precision
    # (microseconds) so both sides of the comparison share the same unit
    {:ok,
     ndjson:
       Map.new(
         [:log, :metric, :trace],
         &{&1, Mapper.compile!(OtelDefaults.for_type(&1, :ndjson))}
       ),
     map:
       Map.new(
         [:log, :metric, :trace],
         fn event_type ->
           config = %{
             MappingConfig.apply_timestamp_precision(OtelDefaults.for_type(event_type, :ndjson))
             | output: nil
           }

           {event_type, Mapper.compile!(config)}
         end
       )}
  end

  describe "default OTEL configs" do
    test "every event type", %{ndjson: ndjson, map: map} do
      for {event_type, body} <- @fixtures do
        row = encode(event_type, body, ndjson)

        assert String.ends_with?(row, "\n")
        assert [_, ""] = String.split(row, "\n")
        assert Map.drop(Jason.decode!(row), @envelope_keys) == expected(event_type, body, map)
      end
    end

    test "event with ingested_at", %{ndjson: ndjson} do
      event = raw_event(:log, @fixtures.log, ingested_at: ~U[2024-01-02 03:04:05.123456Z])
      %LogEvent{id: id, source_name: source_name} = event
      source_uuid = Atom.to_string(event.source_uuid)
      config_id = OtelDefaults.config_id(:log)

      assert %{
               "id" => ^id,
               "source_uuid" => ^source_uuid,
               "source_name" => ^source_name,
               "mapping_config_id" => ^config_id,
               "ingested_at" => 1_704_164_645_123_456
             } = decode(event, ndjson.log, config_id)
    end

    test "event without ingested_at", %{ndjson: ndjson} do
      event = raw_event(:log, @fixtures.log, ingested_at: nil)
      decoded = decode(event, ndjson.log)

      assert %{"ingested_at" => nil} = decoded
      refute Map.has_key?(decoded, "severity_number_alt")
    end

    test "log with and without explicit severity_number", %{ndjson: ndjson} do
      body = Map.put(@fixtures.log, "metadata", %{"level" => "info"})

      assert %{"severity_number" => 17} = decode(:log, body, ndjson)
      assert %{"severity_number" => 9} = decode(:log, Map.delete(body, "severity_number"), ndjson)
    end

    test "trace with explicit, derived, and non-positive duration", %{ndjson: ndjson} do
      for {overrides, expected} <- [
            {%{}, 1},
            {%{"duration" => 42}, 42},
            {%{"end_time" => 1}, 0}
          ] do
        assert %{"duration" => ^expected} =
                 decode(:trace, Map.merge(@fixtures.trace, overrides), ndjson)
      end
    end

    test "metric_type labels", %{ndjson: ndjson} do
      for label <- ~w(gauge histogram summary) do
        body = Map.put(@fixtures.metric, "metric_type", label)
        assert %{"metric_type" => ^label} = decode(:metric, body, ndjson)
      end
    end

    test "uint64 above 2^53", %{ndjson: ndjson} do
      big = Integer.pow(2, 53) + 1
      row = encode(:trace, Map.put(@fixtures.trace, "duration", big), ndjson)

      assert String.contains?(row, ~s("duration":#{big},))
      assert %{"duration" => ^big} = Jason.decode!(row)
    end

    test "non-UTF-8 string field", %{ndjson: ndjson} do
      body = Map.put(@fixtures.log, "event_message", <<0xFF, 0xFE>>)

      assert %{"event_message" => nil} = decode(:log, body, ndjson)
    end
  end

  test "mismatched output_context", %{ndjson: ndjson} do
    rowbinary = Mapper.compile!(OtelDefaults.for_log())
    event = raw_event(:log, @fixtures.log)
    ndjson_context = OutputContext.ndjson(event, "cfg")
    rowbinary_context = OutputContext.ch_row_binary(event, <<0::128>>)

    assert {:error, "NDJSON output requires an ndjson output_context"} =
             Mapper.map_result(event.body, ndjson.log, output_context: rowbinary_context)

    assert {:error, _} = Mapper.map_result(event.body, ndjson.log)

    assert {:error, "ClickHouse RowBinary output requires a ch_row_binary output_context"} =
             Mapper.map_result(event.body, rowbinary, output_context: ndjson_context)
  end

  describe "custom configs" do
    test "config missing derived-rule inputs" do
      config =
        MappingConfig.new([Field.string("event_message", path: "$.event_message")],
          output: OutputFormat.ndjson(:log)
        )

      assert {:error, reason} = Mapper.compile(config)
      assert reason =~ "severity_number_alt"
    end

    test "metric config with arbitrary fields" do
      config =
        MappingConfig.new(
          [
            Field.string("b", path: "$.b"),
            Field.uint64("a", path: "$.a"),
            Field.enum8("kind", paths: ["$.kind"], values: %{"One" => 1})
          ],
          output: OutputFormat.ndjson(:metric)
        )

      event = raw_event(:metric, %{"b" => "x", "a" => 1, "kind" => "one"}, ingested_at: nil)
      decoded = decode(event, Mapper.compile!(config))

      assert Map.drop(decoded, @envelope_keys) == %{"b" => "x", "a" => 1, "kind" => "one"}
    end
  end

  defp encode(event_type, body, ndjson) do
    event = raw_event(event_type, body)
    context = OutputContext.ndjson(event, OtelDefaults.config_id(event_type))
    Mapper.map(event.body, ndjson[event_type], output_context: context)
  end

  defp decode(event_type, body, ndjson) when is_atom(event_type) do
    event_type |> encode(body, ndjson) |> Jason.decode!()
  end

  defp decode(%LogEvent{} = event, compiled, config_id \\ "cfg") do
    context = OutputContext.ndjson(event, config_id)
    event.body |> Mapper.map(compiled, output_context: context) |> Jason.decode!()
  end

  defp expected(event_type, body, map) do
    body
    |> Mapper.map(map[event_type])
    |> ClickHouseMappedEvents.apply_derived_fields(event_type)
    |> Map.drop(["severity_number_alt"])
    |> Jason.encode!()
    |> Jason.decode!()
    |> label_enums(event_type)
  end

  defp raw_event(event_type, body, overrides \\ []) do
    %LogEvent{} = event = build(:log_event)
    struct!(%LogEvent{event | body: body, event_type: event_type}, overrides)
  end

  defp label_enums(map, :metric) do
    field = Enum.find(OtelDefaults.for_metric().fields, &(&1.name == "metric_type"))
    labels = Map.new(field.enum_values, fn {label, value} -> {value, label} end)
    Map.update!(map, "metric_type", &Map.fetch!(labels, &1))
  end

  defp label_enums(map, _event_type), do: map
end
