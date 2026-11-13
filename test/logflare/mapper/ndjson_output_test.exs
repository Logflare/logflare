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
  @array_flat_map_fields ~w(events.attributes links.attributes exemplars.filtered_attributes)
  @bigquery_opts [column_names: :underscore, nested_arrays: :json_strings]
  # enum8 fields: the :map output emits the integer, NDJSON the label
  @label_only_keys ~w(metric_type)
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
    {:ok,
     ndjson:
       Map.new(
         [:log, :metric, :trace],
         &{&1, Mapper.compile!(OtelDefaults.for_type(&1, :ndjson))}
       ),
     bigquery:
       Map.new(
         [:log, :metric, :trace],
         &{&1, Mapper.compile!(OtelDefaults.for_type(&1, {:ndjson, @bigquery_opts}))}
       ),
     map:
       Map.new(
         [:log, :metric, :trace],
         &{&1, Mapper.compile!(%{OtelDefaults.for_type(&1, :ndjson) | output: nil})}
       )}
  end

  describe "default OTEL configs" do
    test "every event type", %{ndjson: ndjson, map: map} do
      for {event_type, body} <- @fixtures do
        row = encode(event_type, body, ndjson)

        assert String.ends_with?(row, "\n")
        assert [_, ""] = String.split(row, "\n")

        assert Map.drop(Jason.decode!(row), @envelope_keys ++ @label_only_keys) ==
                 expected(event_type, body, map)
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
             } = decoded = map_and_decode(event, ndjson.log, config_id)

      refute Map.has_key?(decoded, "severity_number_alt")
    end

    test "log with and without explicit severity_number", %{ndjson: ndjson} do
      body = Map.put(@fixtures.log, "metadata", %{"level" => "info"})

      assert %{"severity_number" => 17} = map_and_decode(:log, body, ndjson)

      assert %{"severity_number" => 9} =
               map_and_decode(:log, Map.delete(body, "severity_number"), ndjson)
    end

    test "trace with explicit, derived, sub-microsecond, and non-positive duration",
         %{ndjson: ndjson} do
      for {overrides, expected} <- [
            {%{}, 1},
            {%{"duration" => 42_000}, 42},
            {%{"duration" => 999}, 1},
            {%{"end_time" => 1}, 0}
          ] do
        assert %{"duration" => ^expected} =
                 map_and_decode(:trace, Map.merge(@fixtures.trace, overrides), ndjson)
      end
    end

    test "metric_type labels", %{ndjson: ndjson} do
      for {input, label} <- [
            {"gauge", "gauge"},
            {"histogram", "histogram"},
            {"summary", "summary"},
            {3, "histogram"},
            {7, "gauge"}
          ] do
        body = Map.put(@fixtures.metric, "metric_type", input)
        assert %{"metric_type" => ^label} = map_and_decode(:metric, body, ndjson)
      end
    end

    test "uint64 duration above 2^53 and above i64 max", %{ndjson: ndjson} do
      for nanos <- [Integer.pow(2, 53) * 1000 + 1000, Integer.pow(2, 64) - 1] do
        expected = div(nanos, 1000)
        row = encode(:trace, Map.put(@fixtures.trace, "duration", nanos), ndjson)

        assert %{"duration" => ^expected} = Jason.decode!(row)
      end
    end

    test "non-UTF-8 string field", %{ndjson: ndjson} do
      body = Map.put(@fixtures.log, "event_message", <<0xFF, 0xFE>>)

      assert %{"event_message" => "\uFFFD\uFFFD"} = map_and_decode(:log, body, ndjson)
    end
  end

  test "mismatched output_context", %{ndjson: ndjson} do
    rowbinary = Mapper.compile!(OtelDefaults.for_log())
    event = raw_event(:log, @fixtures.log)
    ndjson_context = OutputContext.ndjson(event, OtelDefaults.config_id(:log))
    rowbinary_context = OutputContext.ch_row_binary(event, <<0::128>>)

    assert {:error, "Invalid ndjson output context"} =
             Mapper.map_result(event.body, ndjson.log, output_context: rowbinary_context)

    assert {:error, _} = Mapper.map_result(event.body, ndjson.log)

    assert {:error, "Invalid ch_row_binary output context"} =
             Mapper.map_result(event.body, rowbinary, output_context: ndjson_context)
  end

  describe "custom configs" do
    test "json column with keys that collapse to the same lossy string" do
      config =
        MappingConfig.new([Field.json("attrs", path: "$.attrs")],
          output: OutputFormat.ndjson(:metric)
        )

      body = %{"attrs" => %{<<0xFF>> => "a", <<0xFE>> => "b", "ok" => "c"}}
      event = raw_event(:metric, body)
      context = OutputContext.ndjson(event, OtelDefaults.config_id(:metric))
      row = Mapper.map(event.body, Mapper.compile!(config), output_context: context)

      assert length(String.split(row, ~s("\uFFFD":))) == 2
      assert %{"attrs" => %{"\uFFFD" => _, "ok" => "c"} = attrs} = Jason.decode!(row)
      assert map_size(attrs) == 2
    end

    test "config with an envelope field name" do
      config =
        MappingConfig.new([Field.string("source_name", path: "$.source")],
          output: OutputFormat.ndjson(:metric)
        )

      assert {:error, "field name 'source_name' is reserved for the NDJSON envelope"} =
               Mapper.compile(config)
    end

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
            Field.enum8("kind", paths: ["$.kind"], values: %{"one" => 1})
          ],
          output: OutputFormat.ndjson(:metric)
        )

      compiled = Mapper.compile!(config)

      for {kind, expected} <- [{"one", "one"}, {1, "one"}, {9, nil}] do
        event = raw_event(:metric, %{"b" => "x", "a" => 1, "kind" => kind})
        decoded = map_and_decode(event, compiled, OtelDefaults.config_id(:metric))

        assert Map.drop(decoded, @envelope_keys) == %{"b" => "x", "a" => 1, "kind" => expected}
      end
    end
  end

  describe "column_names and nested_arrays options" do
    @nested_bodies %{
      metric:
        Map.put(@fixtures.metric, "exemplars", [
          %{"filtered_attributes" => %{"k" => "v", "nested" => %{"a" => 1}}, "value" => 4.2}
        ]),
      trace:
        Map.merge(@fixtures.trace, %{
          "events" => [%{"name" => "exception", "attributes" => %{"exception.message" => "boom"}}],
          "links" => [%{"trace_id" => "lt", "attributes" => %{"link.type" => "parent"}}]
        })
    }

    test "default OTEL configs write the plain values under BigQuery column names",
         %{ndjson: ndjson, bigquery: bigquery} do
      for event_type <- [:log, :metric, :trace] do
        event = raw_event(event_type, Map.get(@nested_bodies, event_type, @fixtures[event_type]))
        config_id = OtelDefaults.config_id(event_type)
        plain = map_and_decode(event, ndjson[event_type], config_id)
        row = map_and_decode(event, bigquery[event_type], config_id)

        refute Enum.any?(Map.keys(row), &String.contains?(&1, "."))

        assert Map.keys(row) |> Enum.sort() ==
                 Map.keys(plain) |> Enum.map(&underscore/1) |> Enum.sort()

        for {key, value} <- plain do
          bigquery_value = row[underscore(key)]

          if key in @array_flat_map_fields do
            assert Enum.all?(bigquery_value, &is_binary/1)
            assert Enum.map(bigquery_value, &Jason.decode!/1) == value
          else
            assert bigquery_value == value
          end
        end
      end

      assert %{
               "events_name" => ["exception"],
               "events_attributes" => [~s({"exception.message":"boom"})],
               "links_attributes" => [~s({"link.type":"parent"})]
             } = map_and_decode(:trace, @nested_bodies.trace, bigquery)

      assert %{"exemplars_filtered_attributes" => [~s({"k":"v","nested.a":"1"})]} =
               map_and_decode(:metric, @nested_bodies.metric, bigquery)
    end

    test "nested_arrays: :json_strings only changes array_flat_map fields" do
      config =
        MappingConfig.new(
          [
            Field.array_flat_map("items.attrs", path: "$.items[*].attrs"),
            Field.array_string("items.names", path: "$.items[*].name"),
            Field.json("meta", path: "$.meta")
          ],
          output: OutputFormat.ndjson(:metric, nested_arrays: :json_strings)
        )

      body = %{"items" => [%{"name" => "a", "attrs" => %{"k" => "v"}}], "meta" => %{"x" => 1}}
      event = raw_event(:metric, body)
      decoded = map_and_decode(event, Mapper.compile!(config), OtelDefaults.config_id(:metric))

      assert %{
               "items.attrs" => [~s({"k":"v"})],
               "items.names" => ["a"],
               "meta" => %{"x" => 1}
             } = decoded
    end

    test "renaming collisions and unknown option values are compile errors" do
      underscore = OutputFormat.ndjson(:metric, column_names: :underscore)
      colliding = [Field.string("a.b", path: "$.x"), Field.string("a_b", path: "$.y")]
      plain_field = [Field.string("a", path: "$.a")]

      assert {:ok, _compiled} =
               Mapper.compile(MappingConfig.new(colliding, output: OutputFormat.ndjson(:metric)))

      for {fields, output, error} <- [
            {colliding, underscore,
             "NDJSON column name 'a_b' is produced by more than one field"},
            {[Field.string("source.name", path: "$.source")], underscore,
             "field name 'source_name' is reserved for the NDJSON envelope"},
            {plain_field, struct!(OutputFormat.ndjson(:metric), column_names: :camel),
             "unsupported NDJSON column_names 'camel'"},
            {plain_field, struct!(OutputFormat.ndjson(:metric), nested_arrays: :flat),
             "unsupported NDJSON nested_arrays 'flat'"}
          ] do
        assert {:error, ^error} = Mapper.compile(MappingConfig.new(fields, output: output))
      end
    end
  end

  defp underscore(key), do: String.replace(key, ".", "_")

  defp encode(event_type, body, ndjson) do
    event = raw_event(event_type, body)
    context = OutputContext.ndjson(event, OtelDefaults.config_id(event_type))
    Mapper.map(event.body, ndjson[event_type], output_context: context)
  end

  defp map_and_decode(event_type, body, ndjson) when is_atom(event_type) do
    event_type |> encode(body, ndjson) |> Jason.decode!()
  end

  defp map_and_decode(%LogEvent{} = event, compiled, config_id) do
    context = OutputContext.ndjson(event, config_id)
    event.body |> Mapper.map(compiled, output_context: context) |> Jason.decode!()
  end

  defp expected(event_type, body, map) do
    body
    |> Mapper.map(map[event_type])
    |> ClickHouseMappedEvents.apply_derived_fields(event_type)
    |> Map.drop(["severity_number_alt" | @label_only_keys])
    |> Jason.encode!()
    |> Jason.decode!()
  end

  defp raw_event(event_type, body, overrides \\ []) do
    %LogEvent{} = event = build(:log_event)
    struct!(%LogEvent{event | body: body, event_type: event_type}, overrides)
  end
end
