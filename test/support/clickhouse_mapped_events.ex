defmodule Logflare.ClickHouseMappedEvents do
  @moduledoc """
  Builds LogEvents with post-mapping bodies for use in ingester tests.

  Each builder takes raw input-side fields, runs them through the Mapper NIF,
  and returns a LogEvent whose body matches the shape the ingester expects
  for RowBinary encoding.
  """

  import Logflare.Factory

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingConfigStore
  alias Logflare.Mapper

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

    {:ok, compiled, config_id} = MappingConfigStore.get_compiled(:log, opts[:mapping_variant])

    mapped_body =
      input_body
      |> Mapper.map(compiled)
      |> Map.put("mapping_config_id", config_id)
      |> resolve_severity_number()

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

    {:ok, compiled, config_id} = MappingConfigStore.get_compiled(:metric, opts[:mapping_variant])

    mapped_body =
      input_body
      |> Mapper.map(compiled)
      |> Map.put("mapping_config_id", config_id)

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

    {:ok, compiled, config_id} = MappingConfigStore.get_compiled(:trace, opts[:mapping_variant])

    mapped_body =
      input_body
      |> Mapper.map(compiled)
      |> Map.put("mapping_config_id", config_id)

    %{event | body: mapped_body}
  end

  @spec deep_merge_opts(map(), map()) :: map()
  @spec resolve_severity_number(map()) :: map()
  defp resolve_severity_number(%{"severity_number_alt" => alt} = body)
       when is_integer(alt) and alt > 0 do
    %{body | "severity_number" => alt}
  end

  defp resolve_severity_number(body), do: body

  defp deep_merge_opts(base, overrides) when map_size(overrides) == 0, do: base

  defp deep_merge_opts(base, overrides) do
    Map.merge(base, overrides, fn
      _key, base_val, override_val when is_map(base_val) and is_map(override_val) ->
        deep_merge_opts(base_val, override_val)

      _key, _base_val, override_val ->
        override_val
    end)
  end
end
