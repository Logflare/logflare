defmodule Plug.Parsers.BERT do
  @moduledoc """
  Parses BERT (http://bert-rpc.org) request body
  """

  @behaviour Plug.Parsers
  import Plug.Conn
  @gzip_header {"content-encoding", "gzip"}

  def init(_params) do
  end

  def parse(conn, "application", "bert", _headers, _opts) do
    conn
    |> read_body()
    |> decode()
  end

  @doc false
  def parse(conn, _type, _subtype, _headers, _opts) do
    {:next, conn}
  end

  def decode({:ok, <<>>, conn}) do
    {:ok, %{}, conn}
  end

  def decode({:ok, body, conn}) do
    body =
      if @gzip_header in conn.req_headers do
        body |> :zlib.gunzip() |> Bertex.safe_decode()
      else
        body |> Bertex.safe_decode()
      end

    {:ok, body, conn}
  rescue
    e ->
      reraise Plug.Parsers.ParseError, [exception: e], __STACKTRACE__
  end

  def decode({:more, _, conn}) do
    {:error, :too_large, conn}
  end

  def decode({:error, :timeout}) do
    raise Plug.TimeoutError
  end

  def decode({:error, _}) do
    raise Plug.BadRequestError
  end

  def atoms() do
    # fixes a bug in Bertex where Bertex.safe_decode errors because
    # :bert and :dict atoms returned by :binary_to_term do not exist and are treated
    # as coming from the binary
    [:bert, :dict]
  end
end
