defmodule LogflareWeb.Plugs.BufferLimiter do
  @moduledoc """
  A plug that rate limits API requests based on buffer capacity.

  Returns 429 when the source's ingestion buffer is full, preventing
  overload conditions. Supports default ingest backend filtering when
  enabled on the source.
  """
  alias Logflare.Backends
  alias Logflare.Sources.Source
  alias Logflare.SystemCache
  alias LogflareWeb.Api.FallbackController

  @memory_limit 0.85

  @type opts :: any()

  @doc false
  @spec init(opts()) :: opts()
  def init(opts), do: opts

  @doc """
  Checks buffer capacity and applies rate limiting based on source configuration.
  """
  @spec call(Plug.Conn.t(), opts()) :: Plug.Conn.t()
  def call(%{assigns: %{source: %Source{} = source}} = conn, _opts) do
    if SystemCache.memory_utilization() >= @memory_limit or
         Backends.cached_local_pending_buffer_full?(source) do
      :telemetry.execute(
        [:logflare, :ingest, :requests, :buffer_full],
        %{count: 1},
        %{source_id: source.id, source_token: source.token}
      )

      FallbackController.call(conn, {:error, :buffer_full})
    else
      conn
    end
  end
end
