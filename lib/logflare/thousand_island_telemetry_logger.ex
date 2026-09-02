defmodule Logflare.ThousandIslandTelemetryLogger do
  @moduledoc """
  Logs abnormal Thousand Island connection terminations with extra metadata.
  """

  require Logger

  @doc """
  Attaches the telemetry handler that logs Thousand Island connection failures.
  """
  def attach do
    # Thousand Island 1.4 reports connection exceptions through `:stop` metadata.
    :telemetry.attach(
      __MODULE__,
      [:thousand_island, :connection, :stop],
      &__MODULE__.handle_event/4,
      _no_config = []
    )
  end

  @doc """
  Detaches the telemetry handler.
  """
  def detach do
    :telemetry.detach(__MODULE__)
  end

  @doc false
  def handle_event(event, measurements, metadata, _no_config) do
    case {event, metadata} do
      # Normal closes and idle timeouts are expected connection lifecycle events.
      {[:thousand_island, :connection, :stop],
       %{handler: Bandit.DelegatingHandler, error: error} = metadata}
      when error not in [nil, :normal, :timeout] ->
        Logger.warning(fn ->
          "Bandit connection #{format_connection(measurements, metadata)} stopped: " <>
            format_error(error)
        end)

      _event ->
        :ignore
    end
  end

  defp format_connection(measurements, metadata) do
    "connection_id=#{format_id(metadata[:telemetry_span_context])} " <>
      "peer_ip=#{format_ip(metadata[:remote_address])} " <>
      "peer_port=#{format_integer(metadata[:remote_port])} " <>
      "duration_ms=#{format_duration(measurements)} " <>
      "received_bytes=#{format_integer(measurements[:recv_oct])} " <>
      "sent_bytes=#{format_integer(measurements[:send_oct])}"
  end

  defp format_error(error) when is_tuple(error) and tuple_size(error) > 0,
    do: error |> elem(0) |> format_error()

  defp format_error(%{__struct__: module}) when is_atom(module), do: inspect(module)
  defp format_error(error) when is_atom(error), do: Atom.to_string(error)
  defp format_error(_error), do: "unknown_error"

  defp format_ip(address) when is_tuple(address) do
    case :inet.ntoa(address) do
      formatted when is_list(formatted) -> to_string(formatted)
      _error -> "unknown"
    end
  end

  defp format_ip(_address), do: "unknown"

  defp format_duration(%{duration: duration}) when is_integer(duration) and duration >= 0 do
    System.convert_time_unit(duration, :native, :millisecond)
  end

  defp format_duration(_measurements), do: "unknown"

  defp format_id(id) when is_reference(id), do: inspect(id)
  defp format_id(_id), do: "unknown"

  defp format_integer(value) when is_integer(value), do: value
  defp format_integer(_value), do: "unknown"
end
