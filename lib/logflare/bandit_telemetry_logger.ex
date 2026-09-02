defmodule Logflare.BanditTelemetryLogger do
  @moduledoc """
  Logs failed Bandit requests and abnormal Thousand Island connection terminations with extra
  metadata.
  """

  require Logger

  alias Plug.Conn.Status

  @doc """
  Attaches the telemetry handler that logs Bandit request and connection failures.
  """
  def attach do
    events = [
      [:bandit, :request, :stop],
      [:bandit, :request, :exception],
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
  def detach do
    :telemetry.detach(__MODULE__)
  end

  @doc false
  def handle_event(event, measurements, metadata, _no_config) do
    case {event, metadata, exception_status(metadata)} do
      {[:bandit, :request, :stop], %{error: error} = metadata, _status}
      when error not in [nil, "Unrecoverable error: closed", "Client reset stream normally"] ->
        Logger.warning(fn ->
          "Bandit request #{format_request(metadata)} stopped: #{format_request_error(error)}"
        end)

      {[:bandit, :request, :exception], %{exception: exception} = metadata, status}
      when status in 500..599 ->
        Logger.error(fn ->
          "Bandit request #{format_request(metadata)} crashed: " <>
            "kind=#{format_atom(metadata[:kind])} type=#{format_error(exception)} " <>
            "http_status=#{status}"
        end)

      {[:thousand_island, :connection, :stop],
       %{handler: Bandit.DelegatingHandler, error: error} = metadata, _status}
      when error not in [nil, :normal, :timeout] ->
        Logger.warning(fn ->
          "Bandit connection #{format_connection(measurements, metadata)} stopped: " <>
            format_error(error)
        end)

      {[:thousand_island, :connection, :exception],
       %{handler: Bandit.DelegatingHandler, reason: reason} = metadata, _status} ->
        Logger.error(fn ->
          "Bandit connection #{format_connection(measurements, metadata)} crashed: " <>
            "kind=#{format_atom(metadata[:kind])} reason=#{format_error(reason)}"
        end)

      _event ->
        :ignore
    end
  end

  defp format_request(metadata) do
    conn = metadata[:conn]

    "method=#{format_method(conn)} route=#{format_route(conn)} " <>
      "connection_id=#{format_id(metadata[:connection_telemetry_span_context])} " <>
      "peer_ip=#{format_peer_ip(conn)}"
  end

  defp format_connection(measurements, metadata) do
    "connection_id=#{format_id(metadata[:telemetry_span_context])} " <>
      "peer_ip=#{format_ip(metadata[:remote_address])} " <>
      "peer_port=#{format_integer(metadata[:remote_port])} " <>
      "duration_ms=#{format_duration(measurements)} " <>
      "received_bytes=#{format_integer(measurements[:recv_oct])} " <>
      "sent_bytes=#{format_integer(measurements[:send_oct])}"
  end

  defp format_method(%Plug.Conn{method: method})
       when method in ~w(CONNECT DELETE GET HEAD OPTIONS PATCH POST PUT TRACE),
       do: method

  defp format_method(%Plug.Conn{}), do: "OTHER"
  defp format_method(_conn), do: "unknown"

  defp format_route(%Plug.Conn{} = conn) do
    case Phoenix.Router.route_info(LogflareWeb.Router, conn.method, conn.request_path, conn.host) do
      %{route: route} -> route
      :error -> "unknown"
    end
  end

  defp format_route(_conn), do: "unknown"

  defp format_request_error("Unrecoverable error: " <> reason)
       when reason in ~w(econnaborted econnreset ehostunreach einval enetdown enetunreach enotconn epipe etimedout timeout),
       do: reason

  defp format_request_error(error) when is_atom(error), do: Atom.to_string(error)
  defp format_request_error(_error), do: "request_error"

  defp format_error(error) when is_tuple(error) and tuple_size(error) > 0,
    do: error |> elem(0) |> format_error()

  defp format_error(%{__struct__: module}) when is_atom(module), do: inspect(module)
  defp format_error(error) when is_atom(error), do: Atom.to_string(error)
  defp format_error(_error), do: "unknown_error"

  defp exception_status(%{exception: exception}) when is_exception(exception) do
    exception
    |> Plug.Exception.status()
    |> Status.code()
  end

  defp exception_status(%{exception: _exception}), do: 500
  defp exception_status(_metadata), do: nil

  defp format_peer_ip(%Plug.Conn{remote_ip: address}), do: format_ip(address)
  defp format_peer_ip(_conn), do: "unknown"

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

  defp format_atom(value) when is_atom(value), do: Atom.to_string(value)
  defp format_atom(_value), do: "unknown"
end
