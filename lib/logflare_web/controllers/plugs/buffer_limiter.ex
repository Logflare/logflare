defmodule LogflareWeb.Plugs.BufferLimiter do
  @moduledoc """
  A plug that rate limits API requests based on buffer capacity.

  Returns 429 when the source's ingestion buffer is full, preventing
  overload conditions. Supports default ingest backend filtering when
  enabled on the source.
  """

  import Plug.Conn

  alias Logflare.Backends
  alias Logflare.Source

  @type opts :: any()

  @doc false
  @spec init(opts()) :: opts()
  def init(opts), do: opts

  @doc """
  Checks buffer capacity and applies rate limiting based on source configuration.
  """
  @spec call(Plug.Conn.t(), opts()) :: Plug.Conn.t()
  def call(%{assigns: %{source: %Source{} = source}} = conn, _opts) do
    if Backends.cached_local_pending_buffer_full?(source) do
      conn
      |> send_resp(429, "Buffer full: Too many requests")
      |> halt()
    else
      conn
    end
  end
end
