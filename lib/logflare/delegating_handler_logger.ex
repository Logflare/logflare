defmodule Logflare.DelegatingHandlerLogger do
  @moduledoc """
  Logs abnormal Bandit DelegatingHandler connection terminations as structured errors.
  """

  require Logger

  @doc """
  Attaches the telemetry handler that logs Bandit DelegatingHandler connection failures.
  """
  @spec attach() :: :ok | {:error, :already_exists}
  def attach do
    # Thousand Island 1.4 reports handler exceptions through `:stop`.
    # https://github.com/mtrudel/thousand_island/pull/222 adds `:exception` events for
    # exceptions from `handle_connection/2` and `handle_data/3`, so subscribe to both.
    events = [
      [:thousand_island, :connection, :stop],
      [:thousand_island, :connection, :exception]
    ]

    :telemetry.attach_many(
      __MODULE__,
      events,
      &__MODULE__.handle_event/4,
      _no_config = []
    )
  end

  @doc """
  Detaches the telemetry handler.
  """
  @spec detach() :: :ok | {:error, :not_found}
  def detach do
    :telemetry.detach(__MODULE__)
  end

  @doc false
  @spec handle_event(
          :telemetry.event_name(),
          :telemetry.event_measurements(),
          :telemetry.event_metadata(),
          []
        ) :: :ok | :ignore
  def handle_event(event, measurements, metadata, _no_config) do
    case {event, metadata} do
      # Normal closes and idle timeouts are expected connection lifecycle events.
      {[:thousand_island, :connection, :stop],
       %{handler: Bandit.DelegatingHandler, error: error} = metadata}
      when error not in [nil, :normal, :timeout] ->
        Logger.error("Bandit DelegatingHandler connection failure",
          bandit_connection: connection_metadata(measurements, metadata, "stop", error)
        )

      {[:thousand_island, :connection, :exception],
       %{handler: Bandit.DelegatingHandler, reason: reason} = metadata} ->
        Logger.error("Bandit DelegatingHandler connection failure",
          bandit_connection:
            measurements
            |> connection_metadata(metadata, "exception", reason)
            |> put_if_present(:kind, format_atom(metadata[:kind]))
        )

      _event ->
        :ignore
    end
  end

  defp connection_metadata(measurements, metadata, event, error) do
    %{
      duration_ms: format_duration(measurements),
      error: format_error(error),
      event: event,
      peer_ip: format_ip(metadata[:remote_address]),
      peer_port: format_integer(metadata[:remote_port]),
      received_bytes: format_integer(measurements[:recv_oct]),
      sent_bytes: format_integer(measurements[:send_oct])
    }
    |> Map.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp format_error(error) when is_tuple(error) and tuple_size(error) > 0,
    do: error |> elem(0) |> format_error()

  defp format_error(%Bandit.TransportError{error: error})
       when is_atom(error) and not is_nil(error),
       do: Atom.to_string(error)

  defp format_error(%{__struct__: module}) when is_atom(module), do: inspect(module)
  defp format_error(nil), do: "unknown_error"
  defp format_error(error) when is_atom(error), do: Atom.to_string(error)
  defp format_error(_error), do: "unknown_error"

  defp format_ip(address) when is_tuple(address) do
    case :inet.ntoa(address) do
      formatted when is_list(formatted) -> to_string(formatted)
      _error -> nil
    end
  end

  defp format_ip(_address), do: nil

  defp format_duration(%{duration: duration}) when is_integer(duration) and duration >= 0 do
    System.convert_time_unit(duration, :native, :millisecond)
  end

  defp format_duration(_measurements), do: nil

  defp format_integer(value) when is_integer(value), do: value
  defp format_integer(_value), do: nil

  defp format_atom(nil), do: nil
  defp format_atom(value) when is_atom(value), do: Atom.to_string(value)
  defp format_atom(_value), do: nil

  defp put_if_present(map, _key, nil), do: map
  defp put_if_present(map, key, value), do: Map.put(map, key, value)
end
