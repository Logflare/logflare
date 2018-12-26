defmodule LogflareWeb.UserSocket do
  use Phoenix.Socket

  ## Channels
  channel "source:*", LogflareWeb.SourceChannel

  ## Transports - Deprecated in Phoenix 1.4
  # transport :websocket, Phoenix.Transports.WebSocket

  def connect(_params, socket) do
    {:ok, socket}
  end
  def id(_socket), do: nil
end
