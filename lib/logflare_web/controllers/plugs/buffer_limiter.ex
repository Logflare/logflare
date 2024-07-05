defmodule LogflareWeb.Plugs.BufferLimiter do
  @moduledoc """
  A plug that allows or denies API action based on the API request rate rules for user/source
  """
  import Plug.Conn
  alias Logflare.Backends

  def init(_opts), do: nil

  def call(%{assigns: %{source: source}} = conn, _opts \\ []) do
    if Backends.local_pending_buffer_full?(source) do
      conn
      |> send_resp(429, "Buffer full: Too many requests")
      |> halt()
    else
      conn
    end
  end
end
