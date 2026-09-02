defmodule Logflare.BanditTelemetryLogger do
  @moduledoc false

  require Logger

  @events [
    [:bandit, :request, :stop],
    [:bandit, :request, :exception],
    [:thousand_island, :connection, :stop],
    [:thousand_island, :connection, :exception]
  ]
  @http_methods ~w(CONNECT DELETE GET HEAD OPTIONS PATCH POST PUT TRACE)
  @ignored_connection_errors [nil, :normal, :timeout]
  @ignored_request_errors ["Unrecoverable error: closed", "Client reset stream normally"]
  @transport_error_reasons ~w(econnaborted econnreset ehostunreach einval enetdown enetunreach enotconn epipe etimedout timeout)

  @spec attach() :: :ok
  def attach do
    case :telemetry.attach_many(__MODULE__, @events, &__MODULE__.handle_event/4, nil) do
      :ok -> :ok
      {:error, :already_exists} -> :ok
    end
  end

  @spec handle_event(
          :telemetry.event_name(),
          :telemetry.event_measurements(),
          :telemetry.event_metadata(),
          term()
        ) :: :ok
  def handle_event(event, measurements, metadata, config) do
    do_handle_event(event, measurements, metadata, config)
  catch
    _kind, _reason -> :ok
  end

  defp do_handle_event(
         [:bandit, :request, :stop],
         _measurements,
         %{error: error} = metadata,
         _config
       )
       when error not in @ignored_request_errors and not is_nil(error) do
    Logger.warning("Bandit request stopped with error",
      telemetry_event: "bandit.request.stop",
      connection_id: span_id(metadata[:connection_telemetry_span_context]),
      request: request_metadata(metadata[:conn]),
      peer_ip: request_peer_ip(metadata[:conn]),
      error_reason: request_error_reason(error)
    )
  end

  defp do_handle_event(
         [:bandit, :request, :exception],
         _measurements,
         %{exception: exception} = metadata,
         _config
       ) do
    status = exception_status(exception)

    if is_nil(status) or status in 500..599 do
      Logger.error("Bandit request crashed",
        telemetry_event: "bandit.request.exception",
        connection_id: span_id(metadata[:connection_telemetry_span_context]),
        request: request_metadata(metadata[:conn]),
        peer_ip: request_peer_ip(metadata[:conn]),
        exception_kind: atom_string(metadata[:kind]),
        exception_type: error_type(exception),
        http_status: status
      )
    end

    :ok
  end

  defp do_handle_event(
         [:thousand_island, :connection, :stop],
         measurements,
         %{handler: Bandit.DelegatingHandler, error: error} = metadata,
         _config
       )
       when error not in @ignored_connection_errors do
    log_connection(
      :warning,
      "Bandit connection stopped with error",
      "stop",
      measurements,
      metadata,
      error_reason: error_reason(error),
      exception_type: error_type(error)
    )
  end

  defp do_handle_event(
         [:thousand_island, :connection, :exception],
         measurements,
         %{handler: Bandit.DelegatingHandler, reason: reason} = metadata,
         _config
       ) do
    log_connection(:error, "Bandit connection crashed", "exception", measurements, metadata,
      error_reason: error_reason(reason),
      exception_kind: atom_string(metadata[:kind]),
      exception_type: error_type(reason)
    )
  end

  defp do_handle_event(_event, _measurements, _metadata, _config), do: :ok

  defp log_connection(level, message, event, measurements, metadata, error_metadata) do
    Logger.log(
      level,
      message,
      [
        telemetry_event: "thousand_island.connection.#{event}",
        connection_id: span_id(metadata[:telemetry_span_context]),
        peer_ip: format_ip(metadata[:remote_address]),
        peer_port: metadata[:remote_port],
        duration_ms: duration_ms(measurements),
        sent_bytes: measurements[:send_oct],
        received_bytes: measurements[:recv_oct]
      ] ++ error_metadata
    )
  end

  defp request_metadata(%Plug.Conn{} = conn) do
    %{
      method: http_method(conn.method),
      route: route(conn)
    }
  end

  defp request_metadata(_conn), do: %{}

  defp route(conn) do
    case Phoenix.Router.route_info(LogflareWeb.Router, conn.method, conn.request_path, conn.host) do
      %{route: route} -> route
      :error -> nil
    end
  end

  defp request_error_reason("Unrecoverable error: " <> reason)
       when reason in @transport_error_reasons,
       do: reason

  defp request_error_reason(error) when is_binary(error), do: "request_error"

  defp request_error_reason(error), do: error_reason(error)

  defp error_reason(error) when is_tuple(error) and tuple_size(error) > 0,
    do: error |> elem(0) |> error_reason()

  defp error_reason(error) when is_atom(error), do: Atom.to_string(error)
  defp error_reason(error) when is_binary(error), do: "unknown_error"
  defp error_reason(_error), do: nil

  defp error_type(%{__struct__: module}), do: inspect(module)

  defp error_type(error) when is_tuple(error) and tuple_size(error) > 0,
    do: error |> elem(0) |> error_type()

  defp error_type(_error), do: nil

  defp exception_status(%{__exception__: true} = exception),
    do: Plug.Exception.status(exception)

  defp exception_status(_exception), do: nil

  defp request_peer_ip(%Plug.Conn{remote_ip: address}), do: format_ip(address)
  defp request_peer_ip(_conn), do: nil

  defp duration_ms(%{duration: duration}) when is_integer(duration) and duration >= 0,
    do: System.convert_time_unit(duration, :native, :millisecond)

  defp duration_ms(_measurements), do: nil

  defp span_id(span_context) when is_reference(span_context), do: inspect(span_context)
  defp span_id(_span_context), do: nil

  defp atom_string(value) when is_atom(value), do: Atom.to_string(value)
  defp atom_string(_value), do: nil

  defp http_method(method) when method in @http_methods, do: method
  defp http_method(_method), do: "OTHER"

  defp format_ip(address) do
    address
    |> :inet.ntoa()
    |> to_string()
  end
end
