defmodule Logflare.ClickHouseMappedEvents do
  @moduledoc """
  Builds LogEvents with post-mapping bodies for use in ingester tests.

  Each builder takes raw input-side fields, runs them through the Mapper NIF,
  and returns a LogEvent whose body matches the shape the ingester expects
  for RowBinary encoding.
  """

  import Logflare.Factory
  import Logflare.Utils.Guards, only: [is_empty_map: 1]

  alias Logflare.LogEvent.TypeDetection
  alias Logflare.Mapper
  alias Logflare.Mapper.OtelDefaults

  @compiled_mapping_key {__MODULE__, :compiled_mapping}

  @doc """
  Builds a log event with a semi-realistic OTEL-style input body, then maps it
  through the compiled logs mapping config.

  Accepts keyword opts to override input fields. Any key passed here is
  merged into the raw body *before* mapping, so use input-side field names
  (e.g. `metadata: %{"level" => "error"}`, not `severity_text: "ERROR"`).
  """
  @spec build_mapped_log_event(keyword()) :: Logflare.LogEvent.t()
  def build_mapped_log_event(opts \\ []) do
    source = opts[:source] || build(:source)
    message = opts[:message] || "test message"

    event = build(:log_event, source: source, message: message)

    input_body =
      %{
        "event_message" => message,
        "project" => "test-project",
        "trace_id" => "abc123def456",
        "span_id" => "span-789",
        "metadata" => %{
          "level" => "info",
          "region" => "us-east-1",
          "request_id" => "req-001"
        },
        "resource" => %{
          "service" => %{"name" => "test-svc"}
        },
        "timestamp" => DateTime.to_unix(DateTime.utc_now(), :microsecond)
      }
      |> deep_merge_opts(opts[:body] || %{})

    {mapped_body, config_id} = map_body(input_body, :log)

    mapped_body =
      mapped_body
      |> Map.put("mapping_config_id", config_id)
      |> apply_derived_fields(:log)

    %{event | body: mapped_body}
  end

  @doc """
  Builds a metric event with a realistic input body, then maps it
  through the compiled metrics mapping config.
  """
  @spec build_mapped_metric_event(keyword()) :: Logflare.LogEvent.t()
  def build_mapped_metric_event(opts \\ []) do
    source = opts[:source] || build(:source)
    message = opts[:message] || "metric event"

    event =
      build(:log_event, source: source, message: message)
      |> Map.put(:event_type, :metric)

    input_body =
      %{
        "event_message" => message,
        "project" => "test-project",
        "metric_name" => "http_requests_total",
        "metric_description" => "Total HTTP requests",
        "metric_unit" => "1",
        "gauge" => %{"value" => 42.5},
        "value" => 42.5,
        "resource" => %{
          "service" => %{"name" => "metrics-svc"}
        },
        "timestamp" => DateTime.to_unix(DateTime.utc_now(), :microsecond)
      }
      |> deep_merge_opts(opts[:body] || %{})

    {mapped_body, config_id} = map_body(input_body, :metric)
    mapped_body = Map.put(mapped_body, "mapping_config_id", config_id)

    %{event | body: mapped_body}
  end

  @doc """
  Builds a trace event with a realistic input body, then maps it
  through the compiled traces mapping config.
  """
  @spec build_mapped_trace_event(keyword()) :: Logflare.LogEvent.t()
  def build_mapped_trace_event(opts \\ []) do
    source = opts[:source] || build(:source)
    message = opts[:message] || "trace event"

    event =
      build(:log_event, source: source, message: message)
      |> Map.put(:event_type, :trace)

    input_body =
      %{
        "event_message" => message,
        "project" => "test-project",
        "trace_id" => "trace-abc-123",
        "span_id" => "span-def-456",
        "parent_span_id" => "span-parent-789",
        "span_name" => "GET /api/users",
        "span_kind" => "server",
        "duration" => 1500,
        "status" => %{"code" => "OK", "message" => "success"},
        "resource" => %{
          "service" => %{"name" => "trace-svc"}
        },
        "timestamp" => DateTime.to_unix(DateTime.utc_now(), :microsecond)
      }
      |> deep_merge_opts(opts[:body] || %{})

    {mapped_body, config_id} = map_body(input_body, :trace)

    mapped_body =
      mapped_body
      |> Map.put("mapping_config_id", config_id)
      |> apply_derived_fields(:trace)

    %{event | body: mapped_body}
  end

  @doc """
  Reference implementation of the derived-field rules the mapper output
  writers apply in Rust (`native/mapper_ex/src/derive.rs`): log
  `severity_number` falls back to `severity_number_alt`, and a zero trace
  `duration` is computed from `end_time - start_time`.
  """
  @spec apply_derived_fields(map(), TypeDetection.event_type()) :: map()
  def apply_derived_fields(%{"severity_number_alt" => alt} = body, :log)
      when is_integer(alt) and alt > 0 do
    %{body | "severity_number" => alt}
  end

  def apply_derived_fields(
        %{"start_time" => start_time, "end_time" => end_time, "duration" => 0} = body,
        :trace
      )
      when is_integer(start_time) and is_integer(end_time) and end_time > start_time do
    %{body | "duration" => end_time - start_time}
  end

  def apply_derived_fields(body, _event_type), do: body

  @spec map_body(map(), TypeDetection.event_type()) :: {map(), String.t()}
  defp map_body(input_body, event_type) do
    {compiled, config_id} = compiled_mapping(event_type)
    {Mapper.map(input_body, compiled), config_id}
  end

  @spec compiled_mapping(TypeDetection.event_type()) :: {reference(), String.t()}
  defp compiled_mapping(event_type) do
    key = {@compiled_mapping_key, event_type}

    case :persistent_term.get(key, nil) do
      nil ->
        cached =
          {Mapper.compile!(OtelDefaults.for_type(event_type, :map)),
           OtelDefaults.config_id(event_type)}

        :persistent_term.put(key, cached)
        cached

      cached ->
        cached
    end
  end

  @spec deep_merge_opts(map(), map()) :: map()
  defp deep_merge_opts(base, overrides) when is_empty_map(overrides), do: base

  defp deep_merge_opts(base, overrides) do
    Map.merge(base, overrides, fn
      _key, base_val, override_val when is_map(base_val) and is_map(override_val) ->
        deep_merge_opts(base_val, override_val)

      _key, _base_val, override_val ->
        override_val
    end)
  end
end
