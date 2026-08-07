defmodule Logflare.Mapper.PostProcess do
  @moduledoc """
  Event-type-specific fixups applied to mapper output before it is handed to a
  backend, shared by the adaptors ingesting the OTEL column format
  (ClickHouse, S3 Tables).
  """

  alias Logflare.LogEvent.TypeDetection

  @doc """
  Applies all post-processing steps for the given event type: computes a
  missing trace duration from `start_time`/`end_time` and resolves the log
  severity number from `severity_number_alt`.
  """
  @spec apply(map(), TypeDetection.event_type()) :: map()
  def apply(body, event_type) do
    body
    |> maybe_compute_duration(event_type)
    |> resolve_severity_number(event_type)
  end

  @spec maybe_compute_duration(map(), TypeDetection.event_type()) :: map()
  defp maybe_compute_duration(
         %{"start_time" => start_time, "end_time" => end_time, "duration" => 0} = body,
         :trace
       )
       when is_integer(start_time) and is_integer(end_time) and end_time > start_time do
    %{body | "duration" => end_time - start_time}
  end

  defp maybe_compute_duration(body, _event_type), do: body

  @spec resolve_severity_number(map(), TypeDetection.event_type()) :: map()
  defp resolve_severity_number(
         %{"severity_number_alt" => alt} = body,
         :log
       )
       when is_integer(alt) and alt > 0 do
    %{body | "severity_number" => alt}
  end

  defp resolve_severity_number(body, _event_type), do: body
end
