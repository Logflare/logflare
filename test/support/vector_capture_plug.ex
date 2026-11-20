defmodule Logflare.Test.VectorCapturePlug do
  @moduledoc """
  Minimal Plug that captures the raw request body sent by Vector's `http` sink
  and forwards it to a target process. Used by the Vector integration test to
  replay Vector's real HTTP/JSON serialization through Logflare's ingest path.
  """

  @behaviour Plug

  import Plug.Conn

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(conn, opts) do
    {:ok, body, conn} = read_body(conn, length: 50_000_000)
    send(Keyword.fetch!(opts, :target), {:vector_http_body, body})
    send_resp(conn, 200, "")
  end
end
