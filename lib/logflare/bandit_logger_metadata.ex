defmodule Logflare.BanditLoggerMetadata do
  @moduledoc false

  @events [
    [:bandit, :request, :start],
    [:thousand_island, :connection, :stop]
  ]

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
  def handle_event(
        [:bandit, :request, :start],
        _measurements,
        %{conn: %Plug.Conn{} = conn},
        _config
      ) do
    Logger.metadata(request: request_metadata(conn))
  end

  def handle_event(
        [:thousand_island, :connection, :stop],
        _measurements,
        %{
          handler: Bandit.DelegatingHandler,
          remote_address: remote_address,
          remote_port: remote_port
        } = metadata,
        _config
      )
      when is_integer(remote_port) do
    connection =
      %{remote_port: remote_port}
      |> maybe_put(:remote_ip, format_ip(remote_address))
      |> maybe_put(:error, format_error(Map.get(metadata, :error)))

    Logger.metadata(connection: connection)
  end

  def handle_event(_event, _measurements, _metadata, _config), do: :ok

  defp request_metadata(conn) do
    %{}
    |> maybe_put(:method, bounded_binary(conn.method, 16))
    |> maybe_put(:path, route_path(conn))
  end

  defp route_path(conn) do
    case Phoenix.Router.route_info(LogflareWeb.Router, conn.method, conn.request_path, conn.host) do
      %{route: route} -> route
      :error -> nil
    end
  rescue
    _error -> nil
  end

  defp format_error(error) when is_atom(error), do: Atom.to_string(error)
  defp format_error(error) when is_binary(error), do: bounded_binary(error, 128)
  defp format_error({error, _details}) when is_atom(error), do: Atom.to_string(error)
  defp format_error(_error), do: nil

  defp bounded_binary(nil, _length), do: nil

  defp bounded_binary(value, length) when is_binary(value) do
    if String.valid?(value), do: String.byte_slice(value, 0, length)
  end

  defp bounded_binary(_value, _length), do: nil

  defp format_ip(address) do
    case :inet.ntoa(address) do
      formatted when is_list(formatted) -> to_string(formatted)
      _error -> nil
    end
  rescue
    _error -> nil
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
