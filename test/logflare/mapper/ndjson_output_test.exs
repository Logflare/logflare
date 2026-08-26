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
    compiled =
      Map.new([:log, :metric, :trace], fn event_type ->
        {event_type,
         {Mapper.compile!(OtelDefaults.for_type(event_type, :ndjson)),
          Mapper.compile!(OtelDefaults.for_type(event_type, :map))}}
      end)

    {:ok, compiled: compiled}
  end

  describe "row shape" do
    test "emits one newline-terminated JSON object per document", %{compiled: compiled} do
      for {event_type, body} <- @fixtures do
        {ndjson, _map} = compiled[event_type]
        row = map_ndjson(event_type, body, ndjson)

        assert String.ends_with?(row, "\n")
        assert {:ok, %{}} = Jason.decode(row)
        assert String.split(row, "\n") |> length() == 2
      end
    end

    test "agrees with map output on every mapped field", %{compiled: compiled} do
      for {event_type, body} <- @fixtures do
        {ndjson, map} = compiled[event_type]
        decoded = event_type |> map_ndjson(body, ndjson) |> Jason.decode!()

        expected =
          body
          |> Mapper.map(map)
          |> ClickHouseMappedEvents.apply_derived_fields(event_type)
          |> Map.drop(["severity_number_alt"])
          |> jsonify()
          |> label_enums(event_type)

        assert Map.drop(decoded, @envelope_keys) == expected
      end
    end

    test "envelope fields carry the documented shapes", %{compiled: compiled} do
      {ndjson, _} = compiled.log
      ingested_at = ~U[2024-01-02 03:04:05.123456Z]
      event = raw_event(:log, @fixtures.log, ingested_at: ingested_at)
      config_id = OtelDefaults.config_id(:log)

      decoded =
        event.body
        |> Mapper.map(ndjson, output_context: OutputContext.ndjson(event, config_id))
        |> Jason.decode!()

      assert %{
               "id" => id,
               "source_uuid" => source_uuid,
               "source_name" => source_name,
               "mapping_config_id" => ^config_id,
               "ingested_at" => 1_704_164_645_123_456_000
             } = decoded

      assert id == event.id
      assert source_uuid == Atom.to_string(event.source_uuid)
      assert source_name == event.source_name
    end

    test "keeps envelope first, then fields in config order", %{compiled: compiled} do
      {ndjson, _} = compiled.log
      row = map_ndjson(:log, @fixtures.log, ndjson)
      %Jason.OrderedObject{values: pairs} = Jason.decode!(row, objects: :ordered_objects)
      field_names = Enum.map(OtelDefaults.for_log().fields, & &1.name) -- ["severity_number_alt"]

      assert Enum.map(pairs, &elem(&1, 0)) == @envelope_keys ++ field_names
    end

    test "nil ingested_at is null and severity_number_alt is absent", %{compiled: compiled} do
      {ndjson, _} = compiled.log
      event = raw_event(:log, @fixtures.log, ingested_at: nil)

      decoded =
        event.body
        |> Mapper.map(ndjson, output_context: OutputContext.ndjson(event, "cfg"))
        |> Jason.decode!()

      assert %{"ingested_at" => nil} = decoded
      refute Map.has_key?(decoded, "severity_number_alt")
    end
  end

  describe "derived fields" do
    test "severity_number falls back to the alternate source", %{compiled: compiled} do
      {ndjson, _} = compiled.log
      body = Map.put(@fixtures.log, "metadata", %{"level" => "info"})

      assert %{"severity_number" => 17} = :log |> map_ndjson(body, ndjson) |> Jason.decode!()

      assert %{"severity_number" => 9} =
               :log |> map_ndjson(Map.delete(body, "severity_number"), ndjson) |> Jason.decode!()
    end

    test "duration is computed from the span when not explicit", %{compiled: compiled} do
      {ndjson, _} = compiled.trace

      assert %{"duration" => 1500} =
               :trace |> map_ndjson(@fixtures.trace, ndjson) |> Jason.decode!()

      assert %{"duration" => 42} =
               :trace
               |> map_ndjson(Map.put(@fixtures.trace, "duration", 42), ndjson)
               |> Jason.decode!()

      assert %{"duration" => 0} =
               :trace
               |> map_ndjson(Map.put(@fixtures.trace, "end_time", 1), ndjson)
               |> Jason.decode!()
    end
  end

  describe "value encoding" do
    test "enum8 fields are emitted as labels", %{compiled: compiled} do
      {ndjson, _} = compiled.metric

      for {label, _} <- [{"gauge", 1}, {"histogram", 3}, {"summary", 5}] do
        body = Map.put(@fixtures.metric, "metric_type", label)
        assert %{"metric_type" => ^label} = :metric |> map_ndjson(body, ndjson) |> Jason.decode!()
      end
    end

    test "integers above 2^53 are exact", %{compiled: compiled} do
      {ndjson, _} = compiled.trace
      big = Integer.pow(2, 53) + 1
      row = map_ndjson(:trace, Map.put(@fixtures.trace, "duration", big), ndjson)

      assert String.contains?(row, ~s("duration":#{big},))
      assert %{"duration" => ^big} = Jason.decode!(row)
    end

    test "non-UTF-8 strings become null", %{compiled: compiled} do
      {ndjson, _} = compiled.log
      body = Map.put(@fixtures.log, "event_message", <<0xFF, 0xFE>>)

      assert %{"event_message" => nil} = :log |> map_ndjson(body, ndjson) |> Jason.decode!()
    end

    test "map fields are emitted with sorted keys", %{compiled: compiled} do
      {ndjson, _} = compiled.log
      body = Map.put(@fixtures.log, "metadata", %{"z" => "1", "a" => "2", "m" => "3"})
      row = map_ndjson(:log, body, ndjson)

      %Jason.OrderedObject{values: pairs} = Jason.decode!(row, objects: :ordered_objects)
      %Jason.OrderedObject{values: attrs} = :proplists.get_value("log_attributes", pairs)
      keys = Enum.map(attrs, &elem(&1, 0))

      assert keys == Enum.sort(keys)
      assert Enum.filter(keys, &(&1 in ~w(a m z))) == ~w(a m z)
    end
  end

  describe "output context" do
    test "requires an ndjson context", %{compiled: compiled} do
      {ndjson, _} = compiled.log
      event = raw_event(:log, @fixtures.log)

      assert {:error, reason} =
               Mapper.map_result(event.body, ndjson,
                 output_context: OutputContext.ch_row_binary(event, <<0::128>>)
               )

      assert reason =~ "requires an ndjson output_context"
      assert {:error, _} = Mapper.map_result(event.body, ndjson)
    end

    test "RowBinary output rejects an ndjson context" do
      compiled = Mapper.compile!(OtelDefaults.for_log())
      event = raw_event(:log, @fixtures.log)

      assert {:error, reason} =
               Mapper.map_result(event.body, compiled,
                 output_context: OutputContext.ndjson(event, "cfg")
               )

      assert reason =~ "requires a ch_row_binary output_context"
    end
  end

  describe "compile" do
    test "requires the fields consumed by the derived rules" do
      config =
        MappingConfig.new([Field.string("event_message", path: "$.event_message")],
          output: OutputFormat.ndjson(:log)
        )

      assert {:error, reason} = Mapper.compile(config)
      assert reason =~ "severity_number_alt"
    end

    test "passes through arbitrary metric fields in config order" do
      config =
        MappingConfig.new(
          [
            Field.string("b", path: "$.b"),
            Field.uint64("a", path: "$.a"),
            Field.enum8("kind", paths: ["$.kind"], values: %{"One" => 1})
          ],
          output: OutputFormat.ndjson(:metric)
        )

      compiled = Mapper.compile!(config)
      event = raw_event(:metric, %{"b" => "x", "a" => 1, "kind" => "one"}, ingested_at: nil)

      row =
        Mapper.map(event.body, compiled, output_context: OutputContext.ndjson(event, "cfg"))

      assert String.ends_with?(row, ~s("b":"x","a":1,"kind":"one"}\n))
    end
  end

  defp map_ndjson(event_type, body, compiled) do
    event = raw_event(event_type, body)
    context = OutputContext.ndjson(event, OtelDefaults.config_id(event_type))
    Mapper.map(event.body, compiled, output_context: context)
  end

  defp raw_event(event_type, body, overrides \\ []) do
    %LogEvent{} = event = build(:log_event)
    struct!(%LogEvent{event | body: body, event_type: event_type}, overrides)
  end

  defp jsonify(map), do: map |> Jason.encode!() |> Jason.decode!()

  defp label_enums(map, :metric) do
    %{metric_type: labels} =
      OtelDefaults.for_metric().fields
      |> Enum.find(&(&1.name == "metric_type"))
      |> then(&%{metric_type: Map.new(&1.enum_values, fn {label, value} -> {value, label} end)})

    Map.update!(map, "metric_type", &Map.fetch!(labels, &1))
  end

  defp label_enums(map, _event_type), do: map
end
