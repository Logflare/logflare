defmodule Logflare.Backends.UserMonitoring.SpanProcessor do
  @moduledoc """
  OpenTelemetry span processor that ingests finished `logflare.endpoints.query` spans into the
  `system.traces` source of users with system monitoring.

  `LogflareWeb.OpenTelemetrySampler` samples these spans for such users. The ingest runs in the
  process that ends the span. A failed ingest is logged and never fails the query.
  """

  @behaviour :otel_span_processor

  require Logger
  require Record

  alias Logflare.Backends.UserMonitoring
  alias Logflare.Logs
  alias Logflare.Logs.Processor
  alias Logflare.Sources
  alias Logflare.Users

  Record.defrecordp(
    :span,
    Record.extract(:span, from_lib: "opentelemetry/include/otel_span.hrl")
  )

  @span_name "logflare.endpoints.query"

  @impl :otel_span_processor
  def on_start(_ctx, span, _config), do: span

  @impl :otel_span_processor
  def on_end(span(name: @span_name) = span, _config) do
    ingest(span)
    true
  end

  def on_end(_span, _config), do: true

  @impl :otel_span_processor
  def force_flush(_config), do: :ok

  @spec ingest(tuple()) :: term()
  defp ingest(span(attributes: attributes) = span) do
    attributes = :otel_attributes.map(attributes)

    with user_id when is_integer(user_id) <- attributes["user_id"],
         %{system_monitoring: true} <- Users.Cache.get(user_id),
         %Sources.Source{} = source <- UserMonitoring.get_system_source(user_id, :traces) do
      Processor.ingest([to_event(span, attributes)], Logs.Raw, source)
    end
  catch
    kind, reason ->
      Logger.warning("Failed to ingest endpoint query span",
        error_string: Exception.format(kind, reason)
      )
  end

  @spec to_event(tuple(), map()) :: map()
  defp to_event(span, attributes) do
    start_time = :opentelemetry.timestamp_to_nano(span(span, :start_time))

    %{
      "event_message" => @span_name,
      "metadata" => %{"type" => "span"},
      "trace_id" => hex_id(span(span, :trace_id), 32),
      "span_id" => hex_id(span(span, :span_id), 16),
      "parent_span_id" => hex_id(span(span, :parent_span_id), 16),
      "start_time" => start_time,
      "end_time" => :opentelemetry.timestamp_to_nano(span(span, :end_time)),
      "timestamp" => start_time,
      "status" => status(span(span, :status)),
      "attributes" => attributes
    }
  end

  @spec hex_id(non_neg_integer() | :undefined, pos_integer()) :: String.t() | nil
  defp hex_id(id, length) when is_integer(id),
    do: id |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(length, "0")

  defp hex_id(_id, _length), do: nil

  @spec status(tuple() | :undefined) :: map() | nil
  defp status({:status, code, message}), do: %{"code" => to_string(code), "message" => message}
  defp status(_status), do: nil
end
